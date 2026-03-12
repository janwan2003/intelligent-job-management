"""Job CRUD, stop, resume, clear, and log endpoints."""

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

from src.constants import (
    NATS_SUBJECT_STOP_REQUESTED,
    NATS_SUBJECT_SUBMITTED,
    OUTPUT_LOG_FILENAME,
    RESUMABLE_STATUSES,
    RUNS_DIR,
    STATUS_PREEMPTED,
    STATUS_QUEUED,
    STOPPABLE_STATUSES,
)
from src.models import _JOB_COLUMNS, Job, JobCreate, _row_to_job
from src.profiling import scheduler
from src.state import require_db, require_js

logger = logging.getLogger(__name__)

router = APIRouter()

# Directory where job output logs are stored (mounted from host)
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))


@router.post("/jobs", response_model=Job, status_code=201)
async def create_job(job_request: JobCreate) -> Job:
    """Create a new job."""
    conn = require_db()
    jetstream = require_js()

    # Resolve image: prefer ANDREAS field, fall back to legacy
    effective_image = job_request.docker_image or job_request.image
    if not effective_image:
        raise HTTPException(status_code=422, detail="Either 'image' or 'dockerImage' must be provided")

    # Resolve command
    if not job_request.command:
        raise HTTPException(status_code=422, detail="'command' must be provided")
    effective_command = job_request.command

    # Validate deadline is in the future (if provided)
    if job_request.deadline and job_request.deadline.replace(tzinfo=UTC) < datetime.now(UTC):
        raise HTTPException(status_code=422, detail="Deadline must be in the future")

    job_id = str(uuid4())
    now = datetime.now(UTC)

    # Insert job into database
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO jobs (id, image, command, status, created_at, updated_at,
                              priority, deadline, batch_size, epochs_total,
                              profiling_epochs_no, required_memory_gb)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job_id,
                effective_image,
                json.dumps(effective_command),
                STATUS_QUEUED,
                now,
                now,
                job_request.priority,
                job_request.deadline,
                job_request.batch_size,
                job_request.epochs_total,
                job_request.profiling_epochs_no,
                job_request.required_memory_gb,
            ),
        )

    # Schedule: assign profiling config (or best config if already profiled)
    schedule_result = await scheduler.schedule_job(conn, job_id)

    # Publish job submission event to NATS
    await jetstream.publish(
        NATS_SUBJECT_SUBMITTED,
        json.dumps({"job_id": job_id}).encode(),
    )

    return Job(
        id=job_id,
        image=effective_image,
        command=effective_command,
        status=STATUS_QUEUED,
        created_at=now,
        updated_at=now,
        priority=job_request.priority,
        deadline=job_request.deadline,
        batch_size=job_request.batch_size,
        epochs_total=job_request.epochs_total,
        profiling_epochs_no=job_request.profiling_epochs_no,
        required_memory_gb=job_request.required_memory_gb,
        assigned_node=schedule_result.node_id,
        assigned_gpu_config=schedule_result.gpu_config,
        estimated_duration=schedule_result.estimated_duration,
        is_profiling_run=schedule_result.is_profiling_run,
    )


@router.get("/jobs", response_model=list[Job])
async def list_jobs() -> list[Job]:
    """List all jobs ordered by creation time (newest first)."""
    conn = require_db()

    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            ORDER BY created_at DESC
            """
        )
        rows = await cur.fetchall()

    return [_row_to_job(row) for row in rows]


@router.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str) -> Job:
    """Get a specific job by ID."""
    conn = require_db()

    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        )
        row = await cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    return _row_to_job(row)


@router.post("/jobs/{job_id}/stop", status_code=202)
async def stop_job(job_id: str) -> dict[str, str]:
    """Request job to be stopped."""
    conn = require_db()
    jetstream = require_js()

    logger.info("Received stop request for job: %s", job_id)

    # Fetch current status
    async with conn.cursor() as cur:
        await cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        current_status = row[0]

    # Validate: can only stop QUEUED or RUNNING jobs
    if current_status not in STOPPABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot stop job with status {current_status}",
        )

    if current_status == STATUS_QUEUED:
        # QUEUED jobs have no container — mark PREEMPTED directly
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                (STATUS_PREEMPTED, datetime.now(UTC), job_id),
            )
        logger.info("Stopped QUEUED job %s directly (no container)", job_id[:8])
        return {"status": "stopped", "job_id": job_id}

    # RUNNING jobs: publish stop request to worker via NATS
    ack = await jetstream.publish(
        NATS_SUBJECT_STOP_REQUESTED,
        json.dumps({"job_id": job_id}).encode(),
    )
    logger.debug("NATS ack - seq: %s, duplicate: %s", ack.seq, ack.duplicate)

    return {"status": "stop_requested", "job_id": job_id}


@router.post("/jobs/{job_id}/resume", status_code=202)
async def resume_job(job_id: str) -> dict[str, str]:
    """Request job to be resumed."""
    conn = require_db()
    jetstream = require_js()

    # Fetch current status
    async with conn.cursor() as cur:
        await cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        current_status = row[0]

    # Validate: can only resume PREEMPTED or FAILED jobs
    if current_status not in RESUMABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot resume job with status {current_status}",
        )

    # Set status to QUEUED (profiling results are preserved across resumes
    # so each resume profiles one additional configuration)
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
            (STATUS_QUEUED, datetime.now(UTC), job_id),
        )

    # Re-schedule: profiles one new config, or runs on best if all profiled
    await scheduler.schedule_job(conn, job_id)

    # Publish resume request to NATS
    await jetstream.publish(
        NATS_SUBJECT_SUBMITTED,
        json.dumps({"job_id": job_id}).encode(),
    )
    logger.info("Resumed job %s (set to QUEUED, will profile next config)", job_id[:8])

    return {"status": "resume_requested", "job_id": job_id}


@router.delete("/jobs/{job_id}", status_code=204)
async def delete_job(job_id: str) -> None:
    """Delete a job."""
    conn = require_db()

    async with conn.cursor() as cur:
        # Check if job exists
        await cur.execute("SELECT id FROM jobs WHERE id = %s", (job_id,))
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="Job not found")

    # Delete profiling results first, then the job (atomically)
    async with conn.transaction(), conn.cursor() as cur:
        await cur.execute("DELETE FROM profiling_results WHERE job_id = %s", (job_id,))
        await cur.execute("DELETE FROM jobs WHERE id = %s", (job_id,))

    return None


@router.delete("/jobs", status_code=204)
async def clear_all_jobs() -> None:
    """Delete all jobs and their profiling results."""
    conn = require_db()

    async with conn.transaction(), conn.cursor() as cur:
        await cur.execute("DELETE FROM profiling_results")
        await cur.execute("DELETE FROM jobs")

    return None


@router.get("/jobs/{job_id}/logs")
async def get_job_logs(job_id: str) -> PlainTextResponse:
    """Return the output log for a job."""
    conn = require_db()

    # Verify job exists
    async with conn.cursor() as cur:
        await cur.execute("SELECT id FROM jobs WHERE id = %s", (job_id,))
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="Job not found")

    log_path = DATA_DIR / RUNS_DIR / job_id / OUTPUT_LOG_FILENAME
    if not log_path.is_file():
        return PlainTextResponse("No logs available yet.\n", status_code=200)

    return PlainTextResponse(log_path.read_text())

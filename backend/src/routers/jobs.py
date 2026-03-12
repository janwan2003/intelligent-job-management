"""Job CRUD, stop, resume, clear, and log endpoints."""

import asyncio
import json
import logging
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse
from psycopg.rows import dict_row  # type: ignore[import-not-found]

import src.state as state
from src.constants import (
    NATS_SUBJECT_STOP_REQUESTED,
    NATS_SUBJECT_SUBMITTED,
    OUTPUT_LOG_FILENAME,
    RESUMABLE_STATUSES,
    RUNS_DIR,
    STOPPABLE_STATUSES,
    JobStatus,
    nats_job_payload,
)
from src.models import Job, JobCreate
from src.profiling import scheduler
from src.state import require_js

logger = logging.getLogger(__name__)

router = APIRouter()

# Directory where job output logs are stored (mounted from host)
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))

# Docker image name validation
_IMAGE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._/:@-]*$")

# Max log file size to serve (5 MB)
_MAX_LOG_SIZE = 5 * 1024 * 1024

# UUID v4 pattern for path-safety validation
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)


@router.post("/jobs", response_model=Job, status_code=201)
async def create_job(job_request: JobCreate) -> Job:
    """Create a new job."""
    jetstream = require_js()

    # Validate image name
    if not _IMAGE_RE.match(job_request.image):
        raise HTTPException(status_code=422, detail="Invalid Docker image name")

    # Validate deadline is in the future (if provided)
    if job_request.deadline and job_request.deadline.replace(tzinfo=UTC) < datetime.now(UTC):
        raise HTTPException(status_code=422, detail="Deadline must be in the future")

    job_id = str(uuid4())
    now = datetime.now(UTC)

    async with state.schedule_lock, state.get_conn() as conn:
        # Insert job into database
        await conn.execute(
            """
            INSERT INTO jobs (id, image, command, status, created_at, updated_at,
                              priority, deadline, batch_size, epochs_total,
                              profiling_epochs_no, required_memory_gb, log_interval)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job_id,
                job_request.image,
                json.dumps(job_request.command),
                JobStatus.QUEUED,
                now,
                now,
                job_request.priority,
                job_request.deadline,
                job_request.batch_size,
                job_request.epochs_total,
                job_request.profiling_epochs_no,
                job_request.required_memory_gb,
                job_request.log_interval,
            ),
        )

        # Schedule: assign profiling config (or best config if already profiled)
        schedule_result = await scheduler.schedule_job(conn, job_id)

    if schedule_result.node_id is not None:
        # Notify the worker immediately — a node is ready
        await jetstream.publish(
            NATS_SUBJECT_SUBMITTED,
            nats_job_payload(job_id),
        )
    else:
        # No node available right now; queue watcher will start it when one frees up
        logger.info("No node available for new job %s — leaving QUEUED, watcher will retry", job_id[:8])

    return Job(
        id=job_id,
        image=job_request.image,
        command=job_request.command,
        status=JobStatus.QUEUED,
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
        is_profiling_run=schedule_result.is_profiling_run,
        log_interval=job_request.log_interval,
    )


@router.get("/jobs", response_model=list[Job])
async def list_jobs(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[Job]:
    """List all jobs ordered by creation time (newest first)."""
    async with state.get_conn() as conn:
        cur = conn.cursor(row_factory=dict_row)
        await cur.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (limit, offset),
        )
        rows = await cur.fetchall()

    return [Job.model_validate(row) for row in rows]


@router.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str) -> Job:
    """Get a specific job by ID."""
    async with state.get_conn() as conn:
        cur = conn.cursor(row_factory=dict_row)
        await cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    return Job.model_validate(row)


@router.post("/jobs/{job_id}/stop", status_code=202)
async def stop_job(job_id: str) -> dict[str, str]:
    """Request job to be stopped."""
    jetstream = require_js()

    logger.info("Received stop request for job: %s", job_id)

    async with state.get_conn() as conn:
        # Atomic: try to mark QUEUED → PREEMPTED
        cur = await conn.execute(
            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s AND status = %s RETURNING id",
            (JobStatus.PREEMPTED, datetime.now(UTC), job_id, JobStatus.QUEUED),
        )
        if await cur.fetchone():
            logger.info("Stopped QUEUED job %s directly (no container)", job_id[:8])
            return {"status": "stopped", "job_id": job_id}

        # Check if job exists and is in a stoppable status (RUNNING/PROFILING)
        cur = await conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")

        current_status = row[0]
        if current_status not in STOPPABLE_STATUSES:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot stop job with status {current_status}",
            )

    # RUNNING/PROFILING jobs: publish stop request to worker via NATS
    ack = await jetstream.publish(
        NATS_SUBJECT_STOP_REQUESTED,
        nats_job_payload(job_id),
    )
    logger.debug("NATS ack - seq: %s, duplicate: %s", ack.seq, ack.duplicate)

    return {"status": "stop_requested", "job_id": job_id}


@router.post("/jobs/{job_id}/resume", status_code=202)
async def resume_job(job_id: str) -> dict[str, str]:
    """Request job to be resumed."""
    jetstream = require_js()

    async with state.schedule_lock, state.get_conn() as conn:
        # Atomic: try to mark PREEMPTED/FAILED → QUEUED
        now = datetime.now(UTC)
        cur = await conn.execute(
            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s AND status = ANY(%s) RETURNING id",
            (JobStatus.QUEUED, now, job_id, list(RESUMABLE_STATUSES)),
        )
        if not await cur.fetchone():
            # Check if job exists
            cur = await conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Job not found")
            raise HTTPException(
                status_code=409,
                detail=f"Cannot resume job with status {row[0]}",
            )

        # Re-schedule: profiles one new config, or runs on best if all profiled
        schedule_result = await scheduler.schedule_job(conn, job_id)

    if schedule_result.node_id is not None:
        await jetstream.publish(
            NATS_SUBJECT_SUBMITTED,
            nats_job_payload(job_id),
        )
        logger.info("Resumed job %s (node=%s)", job_id[:8], schedule_result.node_id)
    else:
        logger.info("Resumed job %s — no node available, watcher will retry", job_id[:8])

    return {"status": "resume_requested", "job_id": job_id}


@router.delete("/jobs/{job_id}", status_code=204)
async def delete_job(job_id: str) -> None:
    """Delete a job."""
    async with state.get_conn() as conn:
        # Check if job exists and get status
        cur = await conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")

        # If job is running/profiling, request stop first
        if row[0] in STOPPABLE_STATUSES and row[0] != JobStatus.QUEUED:
            try:
                js = require_js()
                await js.publish(
                    NATS_SUBJECT_STOP_REQUESTED,
                    nats_job_payload(job_id),
                )
            except Exception:
                logger.warning("Failed to publish stop event for job %s before delete", job_id[:8])

        # Delete profiling results first, then the job (atomically)
        async with conn.transaction():
            await conn.execute("DELETE FROM profiling_results WHERE job_id = %s", (job_id,))
            await conn.execute("DELETE FROM jobs WHERE id = %s", (job_id,))

    return None


@router.delete("/jobs", status_code=204)
async def clear_all_jobs() -> None:
    """Delete all jobs and their profiling results."""
    async with state.get_conn() as conn, conn.transaction():
        await conn.execute("DELETE FROM profiling_results")
        await conn.execute("DELETE FROM jobs")

    return None


@router.get("/jobs/{job_id}/logs")
async def get_job_logs(job_id: str) -> PlainTextResponse:
    """Return the output log for a job."""
    # Validate job_id format (path traversal prevention)
    if not _UUID_RE.match(job_id):
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    async with state.get_conn() as conn:
        # Verify job exists
        cur = await conn.execute("SELECT id FROM jobs WHERE id = %s", (job_id,))
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="Job not found")

    log_path = DATA_DIR / RUNS_DIR / job_id / OUTPUT_LOG_FILENAME

    # Path containment check
    try:
        log_path.resolve().relative_to(DATA_DIR.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID") from None

    if not log_path.is_file():
        return PlainTextResponse("No logs available yet.\n", status_code=200)

    if log_path.stat().st_size > _MAX_LOG_SIZE:
        raise HTTPException(status_code=413, detail="Log file too large")

    content = await asyncio.to_thread(log_path.read_text)
    return PlainTextResponse(content)

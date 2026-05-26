"""Job CRUD, stop, resume, clear, and log endpoints."""

import asyncio
import json
import logging
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse
from psycopg.rows import dict_row
from shared.constants import OUTPUT_LOG_FILENAME, PG_NOTIFY_SCHEDULE, RUNS_DIR, JobStatus
from shared.profiling_sql import delete_unmeasured_claims

import src.state as state
from src.constants import RESUMABLE_STATUSES, STOPPABLE_STATUSES
from src.models import Job, JobCreate
from src.profiling import scheduler
from src.state import require_runner

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
    runner = require_runner()

    # Validate image name
    if not _IMAGE_RE.match(job_request.image):
        raise HTTPException(status_code=422, detail="Invalid Docker image name")

    # Past deadlines are allowed — the optimizer treats them as urgent
    # ("expected_tardiness > 0") rather than invalid.

    instance_id = str(uuid4())
    now = datetime.now(UTC)

    # Build effective command: scriptPath overrides command if provided
    effective_command = job_request.command
    if job_request.script_path:
        effective_command = ["python3", job_request.script_path]

    async with state.schedule_lock, state.get_conn() as conn:
        # Insert job into database
        await conn.execute(
            """
            INSERT INTO jobs (id, job_id, image, command, script_path, directory_to_mount,
                              status, created_at, updated_at,
                              priority, deadline, batch_size, epochs_total,
                              profiling_epochs_no)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                instance_id,
                job_request.job_id,
                job_request.image,
                json.dumps(effective_command),
                job_request.script_path,
                job_request.directory_to_mount,
                JobStatus.QUEUED,
                now,
                now,
                job_request.priority,
                job_request.deadline,
                job_request.batch_size,
                job_request.epochs_total,
                job_request.profiling_epochs_no,
            ),
        )

        # Schedule: assign profiling config (or best config if already profiled)
        # Use job_id (type) for profiling correlation, instance_id for DB row
        schedule_result = await scheduler.schedule_job(conn, instance_id, job_type_id=job_request.job_id)

        # Wake the optimizer immediately.  Greedy placement above only fills
        # free slots; it cannot decide to preempt a lower-priority running
        # job to make room for an urgent (e.g. past-deadline) submission.
        # The 60s safety-net watcher would eventually trigger that decision,
        # but is too slow for the common case.  The notify is debounced on
        # the consumer side so a redundant pass when greedy already placed
        # the job is essentially free.
        await conn.execute(f"NOTIFY {PG_NOTIFY_SCHEDULE}")

    if schedule_result.node_id is not None and schedule_result.gpu_config is not None:
        # Go through dispatch_with_slot so the per-node semaphore acquires a
        # permit.  Calling enqueue directly here was a long-standing bug:
        # direct submissions and resumes bypassed the slot invariant and let
        # more containers run on a node than it had GPUs for.
        # Failures here (worker 409 because the chosen slot just got taken by
        # a concurrent dispatch, network blip, etc.) must NOT bubble up as a
        # 500 — the row is already inserted as QUEUED with an assigned_node,
        # and the scheduler watcher will retry dispatch on the next round.
        try:
            await runner.dispatch_with_slot(instance_id, schedule_result.node_id, schedule_result.gpu_config)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Initial dispatch for new job %s on %s failed (%s); leaving QUEUED for scheduler retry",
                instance_id[:8],
                schedule_result.node_id,
                e,
            )
    else:
        logger.info("No node available for new job %s — notified scheduler for optimizer pass", instance_id[:8])

    return Job(
        id=instance_id,
        job_id=job_request.job_id,
        image=job_request.image,
        command=effective_command,
        script_path=job_request.script_path,
        directory_to_mount=job_request.directory_to_mount,
        status=JobStatus.QUEUED,
        created_at=now,
        updated_at=now,
        priority=job_request.priority,
        deadline=job_request.deadline,
        batch_size=job_request.batch_size,
        epochs_total=job_request.epochs_total,
        profiling_epochs_no=job_request.profiling_epochs_no,
        assigned_node=schedule_result.node_id,
        assigned_gpu_config=schedule_result.gpu_config,
        is_profiling_run=schedule_result.is_profiling_run,
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
    """Request job to be stopped (user-driven; sticky PREEMPTED state).

    Atomicity contract: the DB flip to ``PREEMPTED`` happens *before* the
    async container kill is dispatched.  Otherwise the optimizer's preempt
    cycle, which selects victims by ``status IN ('RUNNING', 'PROFILING')``,
    can race this endpoint and re-dispatch the row to a different node
    before the worker's kill+persist lands — overwriting the user's
    PREEMPTED intent with QUEUED.  Flipping status first removes the row
    from the optimizer's preempt-target set immediately.
    """
    runner = require_runner()

    logger.info("Received stop request for job: %s", job_id)

    async with state.get_conn() as conn:
        # SELECT FOR UPDATE + conditional UPDATE in one transaction —
        # serializable against the optimizer's preempt-list build (which
        # also reads + writes the row).  The row lock blocks the optimizer
        # from racing in between our read and write.
        cur = await conn.execute("SELECT status FROM jobs WHERE id = %s FOR UPDATE", (job_id,))
        existing = await cur.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="Job not found")
        prev_status = existing[0]
        if prev_status not in STOPPABLE_STATUSES:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot stop job with status {prev_status}",
            )
        await conn.execute(
            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
            (JobStatus.PREEMPTED, datetime.now(UTC), job_id),
        )
        await conn.commit()

    if prev_status == JobStatus.QUEUED:
        logger.info("Stopped QUEUED job %s directly (no container)", job_id[:8])
        return {"status": "stopped", "job_id": job_id}

    # RUNNING / PROFILING: status already flipped; now kill the container.
    # Worker's /stop handler will see PREEMPTED and skip its own status
    # update, but still kill + free the slot (see worker/app.py).
    await runner.stop(job_id)
    return {"status": "stop_requested", "job_id": job_id}


@router.post("/jobs/{job_id}/resume", status_code=202)
async def resume_job(job_id: str) -> dict[str, str]:
    """Request job to be resumed."""
    runner = require_runner()

    async with state.schedule_lock, state.get_conn() as conn:
        # Atomic: try to mark PREEMPTED/FAILED → QUEUED
        now = datetime.now(UTC)
        cur = await conn.execute(
            """UPDATE jobs SET status = %s, assigned_node = NULL, assigned_gpu_config = NULL,
               updated_at = %s WHERE id = %s AND status = ANY(%s) RETURNING id, job_id""",
            (JobStatus.QUEUED, now, job_id, list(RESUMABLE_STATUSES)),
        )
        row = await cur.fetchone()
        if not row:
            cur = await conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
            status_row = await cur.fetchone()
            if not status_row:
                raise HTTPException(status_code=404, detail="Job not found")
            raise HTTPException(
                status_code=409,
                detail=f"Cannot resume job with status {status_row[0]}",
            )

        # Re-schedule with correct type_id for profiling lookup
        job_type_id = row[1]
        schedule_result = await scheduler.schedule_job(conn, job_id, job_type_id=job_type_id)

        # Same rationale as create_job: wake the optimizer so a resumed job
        # with high priority / past deadline can preempt running work
        # without waiting for the 60s safety-net watcher.
        await conn.execute(f"NOTIFY {PG_NOTIFY_SCHEDULE}")

    if schedule_result.node_id is not None and schedule_result.gpu_config is not None:
        # Same fix as create_job — must acquire a slot permit, not bypass it.
        await runner.dispatch_with_slot(job_id, schedule_result.node_id, schedule_result.gpu_config)
        logger.info("Resumed job %s (node=%s)", job_id[:8], schedule_result.node_id)
    else:
        logger.info("Resumed job %s — no node available, notified scheduler for optimizer pass", job_id[:8])

    return {"status": "resume_requested", "job_id": job_id}


@router.delete("/jobs/{job_id}", status_code=204)
async def delete_job(job_id: str) -> None:
    """Delete a job, killing any running container first."""
    runner = require_runner()

    async with state.get_conn() as conn:
        cur = await conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")

        # If job is running/profiling, kill it first
        if row[0] in STOPPABLE_STATUSES and row[0] != JobStatus.QUEUED:
            try:
                await runner.stop(job_id)
            except Exception:
                logger.warning("Failed to stop job %s before delete", job_id[:8])

        # Atomically clear in-flight claims owned by this instance, then the
        # job row itself.  Important: we filter by ``instance_id``, not by
        # ``profiling_results.job_id`` — the latter is the *type* ID and is
        # shared across instances, so deleting on it would wipe cached
        # measurements that other instances of the same type still rely on.
        # Completed rows (duration_seconds NOT NULL) are left alone so future
        # instances of this type continue to benefit from the cached profile.
        async with conn.transaction(), conn.cursor() as cur:
            await delete_unmeasured_claims(cur, job_id)
            await cur.execute("DELETE FROM jobs WHERE id = %s", (job_id,))

    return None


@router.delete("/jobs", status_code=204)
async def clear_all_jobs() -> None:
    """Delete all jobs and their profiling results.

    Stops running containers first — orphaned containers would emit
    ``ijm_slot_freed`` for deleted rows and over-release the per-node semaphore.
    """
    runner = require_runner()

    async with state.get_conn() as conn:
        cur = await conn.execute(
            "SELECT id FROM jobs WHERE status = ANY(%s)",
            (list(STOPPABLE_STATUSES),),
        )
        active_ids = [r[0] for r in await cur.fetchall()]

    for jid in active_ids:
        try:
            await runner.stop(jid, reason="user")
        except Exception:
            logger.warning("Failed to stop job %s during clear_all_jobs", jid[:8])

    async with state.get_conn() as conn, conn.transaction():
        await conn.execute("DELETE FROM profiling_results")
        await conn.execute("DELETE FROM jobs")

    # Force drift-recover so phantom permits from dispatch tasks blocked on
    # acquire don't linger until the periodic watcher fires.
    if state.node_slots is not None:
        await state.node_slots.recover_from_drift(state.get_conn)

    return None


@router.get("/jobs/{job_id}/logs")
async def get_job_logs(job_id: str) -> PlainTextResponse:
    """Return the output log for a job by proxying to the worker that owns it.

    When the row has no assigned_node we fan out to every worker and pick the
    response with the *newest mtime* — picking the longest mis-selects the
    frozen file on the original node after a migration.
    """
    if not _UUID_RE.match(job_id):
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    async with state.get_conn() as conn:
        cur = await conn.execute("SELECT id, assigned_node FROM jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        assigned_node = row[1]

    runner = state.job_runner
    cluster = getattr(runner, "_cluster", None)

    async def _fetch_from(worker_url: str) -> str | None:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(f"{worker_url}/jobs/{job_id}/logs")
                resp.raise_for_status()
                return resp.text
        except Exception as exc:
            logger.warning("Failed to fetch logs from worker %s: %s", worker_url, exc)
            return None

    # 1. Direct proxy to the assigned node's worker.
    if assigned_node and hasattr(runner, "get_worker_url"):
        worker_url = runner.get_worker_url(assigned_node)
        if worker_url:
            text = await _fetch_from(worker_url)
            if text is not None and text.strip() and text.strip() != "No logs available yet.":
                return PlainTextResponse(text)

    # 2. Fan out to every worker that has a logs endpoint.  Pick by mtime
    #    (the worker reports it via X-Log-Mtime if present) — newest wins,
    #    which is the live writer.  When no header is available, fall back
    #    to "non-empty, longest" but only among non-default responses.
    if cluster is not None:
        urls = [n["workerUrl"] for n in cluster.nodes if n.get("workerUrl")]
        if urls:

            async def _fetch_with_mtime(url: str) -> tuple[str, float] | None:
                try:
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        resp = await client.get(f"{url}/jobs/{job_id}/logs")
                        resp.raise_for_status()
                        mtime_hdr = resp.headers.get("X-Log-Mtime")
                        mtime = float(mtime_hdr) if mtime_hdr else 0.0
                        return resp.text, mtime
                except Exception:
                    return None

            results = await asyncio.gather(*(_fetch_with_mtime(u) for u in urls))
            valid = [
                (text, mtime)
                for r in results
                if r is not None
                for text, mtime in [r]
                if text.strip() and text.strip() != "No logs available yet."
            ]
            if valid:
                # Prefer newest mtime; ties broken by length.
                text, _ = max(valid, key=lambda tm: (tm[1], len(tm[0])))
                return PlainTextResponse(text)

    # 3. Local FS fallback (local dev).
    log_path = DATA_DIR / RUNS_DIR / job_id / OUTPUT_LOG_FILENAME
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

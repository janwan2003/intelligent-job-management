"""Profiling helpers for the IJM worker."""

import logging
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.types.json import Json
from shared.constants import PG_NOTIFY_SCHEDULE, JobStatus

from constants import JOB_ID_DISPLAY_LENGTH
from db import fetch_job

logger = logging.getLogger(__name__)


def compute_duration(
    epoch_timestamps: list[tuple[int, float]] | None,
    run_start_time: datetime,
) -> float:
    """Compute profiling duration, excluding first epoch as warmup."""
    if epoch_timestamps and len(epoch_timestamps) >= 3:
        intervals = [epoch_timestamps[i + 1][1] - epoch_timestamps[i][1] for i in range(len(epoch_timestamps) - 1)]
        steady = intervals[1:]
        mean_epoch_time = sum(steady) / len(steady)
        total_epochs = epoch_timestamps[-1][0]
        logger.info("Profiling: warmup=%.4fs, steady mean=%.4fs/epoch", intervals[0], mean_epoch_time)
        return mean_epoch_time * total_epochs
    return (datetime.now(UTC) - run_start_time).total_seconds()


async def handle_complete(
    conn: psycopg.AsyncConnection[Any],
    job_id: str,
    run_start_time: datetime,
    epoch_timestamps: list[tuple[int, float]],
) -> bool:
    """Write profiling result to DB and reset job to QUEUED. Returns True if this was a profiling run."""
    job = await fetch_job(conn, job_id, "is_profiling_run", "assigned_gpu_config", "assigned_node", "job_id")
    if not job or not job["is_profiling_run"]:
        return False

    duration = compute_duration(epoch_timestamps, run_start_time)
    now = datetime.now(UTC)
    async with conn.cursor() as cur:
        await cur.execute(
            """UPDATE profiling_results
               SET duration_seconds = %s, node_id = %s
               WHERE job_id = %s AND gpu_config = %s::jsonb AND duration_seconds IS NULL""",
            (duration, job["assigned_node"], job["job_id"], Json(job["assigned_gpu_config"])),
        )
        await cur.execute(
            """UPDATE jobs SET status = %s, assigned_node = NULL, assigned_gpu_config = NULL,
               is_profiling_run = FALSE, updated_at = %s WHERE id = %s""",
            (JobStatus.QUEUED, now, job_id),
        )
    await conn.commit()
    await conn.execute(f"NOTIFY {PG_NOTIFY_SCHEDULE}")
    await conn.commit()

    logger.info(
        "Profiling complete for job %s (type=%s): %s = %.1fs — reset to QUEUED",
        job_id[:JOB_ID_DISPLAY_LENGTH],
        job["job_id"],
        job["assigned_gpu_config"],
        duration,
    )
    return True

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


# Need ≥ 2 timestamps: one ending the warmup epoch and one ending a
# steady-state epoch, giving 1 post-warmup interval to average.
MIN_EPOCH_TIMESTAMPS_FOR_STEADY = 2


def compute_duration(
    epoch_timestamps: list[tuple[int, float]] | None,
    run_start_time: datetime,
) -> float:
    """Compute mean per-epoch duration, excluding the first (warmup) epoch.

    The runtime emits a timestamp after each epoch finishes, so
    ``epoch_timestamps[0]`` marks the end of epoch 1 (warmup) — its duration
    is unknown because the run-start clock is wall-time, not the container's
    monotonic clock. Every interval between timestamps is therefore already
    a post-warmup sample.
    """
    if epoch_timestamps and len(epoch_timestamps) >= MIN_EPOCH_TIMESTAMPS_FOR_STEADY:
        intervals = [epoch_timestamps[i + 1][1] - epoch_timestamps[i][1] for i in range(len(epoch_timestamps) - 1)]
        mean_epoch_time = sum(intervals) / len(intervals)
        logger.info(
            "Profiling: warmup epoch excluded, mean=%.4fs/epoch over %d sample(s)",
            mean_epoch_time,
            len(intervals),
        )
        return mean_epoch_time
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
        # Filter by instance_id so a stray finish from a different instance
        # cannot overwrite this row, and warn loudly if no claim row exists
        # (the scheduler should never let two instances claim the same config,
        # but if it does we want to know rather than lose a result silently).
        await cur.execute(
            """UPDATE profiling_results
               SET duration_seconds = %s, node_id = %s
               WHERE job_id = %s AND gpu_config = %s::jsonb
                 AND instance_id = %s AND duration_seconds IS NULL""",
            (duration, job["assigned_node"], job["job_id"], Json(job["assigned_gpu_config"]), job_id),
        )
        if cur.rowcount == 0:
            logger.warning(
                "No in-flight profiling claim for job %s (type=%s, %s) — result not recorded",
                job_id[:JOB_ID_DISPLAY_LENGTH],
                job["job_id"],
                job["assigned_gpu_config"],
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

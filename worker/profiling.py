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


# Need >= 2 per-epoch samples: epoch 1 is warmup (cold CUDA / cuDNN autotune,
# first dataset pass) and is dropped, leaving >= 1 steady-state sample.
MIN_EPOCH_SAMPLES_FOR_STEADY = 2


def compute_duration(
    epoch_durations: list[tuple[int, float]] | None,
    run_start_time: datetime,
) -> float:
    """Mean steady-state per-epoch compute time, warmup (epoch 1) excluded.

    Each entry is ``(epoch_num, compute_seconds)`` — the per-epoch time the
    *runtime itself* measured and logged (the trailing ``- 5.21s``), captured
    worker-side by ``EPOCH_DONE_RE``.  We use the trainer's own number rather
    than timing when its log lines arrive at the worker: arrival intervals
    absorb checkpoint-save I/O and log/network jitter, which run much heavier
    during the concurrent profile sweep than during the steady standard run
    and would bias the estimate high (observed 2026-06-25: A40x1 = 8.0s by
    arrival interval vs ~5.2s real compute).

    Epoch 1 carries cold-start overhead that does not recur, so it is dropped
    and the remaining samples are averaged.
    """
    if epoch_durations and len(epoch_durations) >= MIN_EPOCH_SAMPLES_FOR_STEADY:
        steady = [d for _, d in epoch_durations[1:]]  # drop epoch-1 warmup
        mean_epoch_time = sum(steady) / len(steady)
        logger.info(
            "Profiling: warmup epoch excluded, mean=%.4fs/epoch over %d sample(s)",
            mean_epoch_time,
            len(steady),
        )
        return mean_epoch_time
    return (datetime.now(UTC) - run_start_time).total_seconds()


async def handle_complete(
    conn: psycopg.AsyncConnection[Any],
    job_id: str,
    run_start_time: datetime,
    epoch_durations: list[tuple[int, float]],
) -> bool:
    """Write profiling result to DB and reset job to QUEUED. Returns True if this was a profiling run."""
    job = await fetch_job(conn, job_id, "is_profiling_run", "assigned_gpu_config", "assigned_node", "job_id")
    if not job or not job["is_profiling_run"]:
        return False

    duration = compute_duration(epoch_durations, run_start_time)
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
        # Keep is_profiling_run=TRUE if this instance still has unmeasured
        # claims — otherwise the long-lease GC could collect them on its
        # 60-min tick and the remaining profile cells would never dispatch.
        await cur.execute(
            "SELECT 1 FROM profiling_results WHERE instance_id = %s AND duration_seconds IS NULL LIMIT 1",
            (job_id,),
        )
        still_owes = (await cur.fetchone()) is not None
        logger.info(
            "Profile-complete %s: still_owes=%s → is_profiling_run will be %s",
            job_id[:JOB_ID_DISPLAY_LENGTH],
            still_owes,
            still_owes,
        )
        await cur.execute(
            """UPDATE jobs SET status = %s, assigned_node = NULL, assigned_gpu_config = NULL,
               is_profiling_run = %s, updated_at = %s WHERE id = %s""",
            (JobStatus.QUEUED, still_owes, now, job_id),
        )
        # NOTIFY in the same transaction so the scheduler wake-up can never
        # outlive an unwritten status flip (or vice versa).
        await cur.execute(f"NOTIFY {PG_NOTIFY_SCHEDULE}")
    await conn.commit()

    logger.info(
        "Profiling complete for job %s (type=%s): %s = %.1fs — reset to QUEUED",
        job_id[:JOB_ID_DISPLAY_LENGTH],
        job["job_id"],
        job["assigned_gpu_config"],
        duration,
    )
    return True

"""Startup reconciliation for the IJM worker."""

import logging

from shared.constants import JobStatus

from constants import JOB_ID_DISPLAY_LENGTH, NODE_ID, container_name_for
from db import conn, update_job
from docker import list_containers

logger = logging.getLogger(__name__)


async def reconcile_job_states() -> None:
    """Mark RUNNING/PROFILING jobs as FAILED if their container is gone."""
    logger.info("Reconciling job states for node %s", NODE_ID)
    try:
        running_containers = await list_containers()
        async with conn() as c, c.cursor() as cur:
            await cur.execute(
                "SELECT id, container_name, status FROM jobs WHERE status IN (%s, %s) AND assigned_node = %s",
                (JobStatus.RUNNING, JobStatus.PROFILING, NODE_ID),
            )
            for job in await cur.fetchall():
                expected = job["container_name"] or container_name_for(job["id"])
                if expected not in running_containers:
                    logger.warning(
                        "Job %s container %s gone — marking FAILED",
                        job["id"][:JOB_ID_DISPLAY_LENGTH],
                        expected,
                    )
                    await update_job(c, job["id"], status=JobStatus.FAILED)
    except Exception:
        logger.exception("Failed to reconcile job states")


async def pickup_queued_jobs() -> None:
    """Enqueue QUEUED jobs assigned to this node that were missed while the worker was down."""
    from execution import dispatch_job  # deferred to avoid circular import

    try:
        async with conn() as c, c.cursor() as cur:
            await cur.execute(
                "SELECT id FROM jobs WHERE status = %s AND assigned_node = %s ORDER BY created_at ASC",
                (JobStatus.QUEUED, NODE_ID),
            )
            rows = await cur.fetchall()
        if rows:
            logger.info("Found %d missed QUEUED job(s) — enqueuing", len(rows))
            for row in rows:
                dispatch_job(row["id"])
    except Exception:
        logger.exception("Failed to pick up queued jobs")

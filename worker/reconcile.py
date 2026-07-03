"""Startup reconciliation for the IJM worker."""

import logging

from shared.constants import PG_NOTIFY_SCHEDULE, PG_NOTIFY_SLOT_FREED, JobStatus, SlotFreedReason
from shared.profiling_sql import delete_unmeasured_claims

from constants import JOB_ID_DISPLAY_LENGTH, NODE_ID, container_name_for
from db import conn, update_job
from docker import list_containers

logger = logging.getLogger(__name__)


async def reconcile_job_states() -> None:
    """Mark RUNNING/PROFILING jobs as FAILED if their container is gone.

    For each row we move to FAILED we also emit ``ijm_slot_freed`` so the
    API's per-node semaphore releases the permit that was acquired when the
    job was originally dispatched.  Without this, a worker restart while
    holding running jobs would leave the API permanently believing those
    slots are taken — dispatches block, the optimizer marks the slot
    occupied, etc.  The kill side of the lifecycle (Phase 4) emits the
    same NOTIFY in the live-container path, so both halves produce a single
    release per acquire.
    """
    logger.info("Reconciling job states for node %s", NODE_ID)
    try:
        running_containers = await list_containers()
        async with conn() as c, c.cursor() as cur:
            await cur.execute(
                "SELECT id, container_name, status, assigned_node, assigned_gpu_config "
                "FROM jobs WHERE status IN (%s, %s) AND assigned_node = %s",
                (JobStatus.RUNNING, JobStatus.PROFILING, NODE_ID),
            )
            for job in await cur.fetchall():
                expected = job["container_name"] or container_name_for(job["id"])
                if expected not in running_containers:
                    cfg = job.get("assigned_gpu_config") or {}
                    n_gpus = sum(int(v) for v in cfg.values())
                    slot_node = job.get("assigned_node")
                    logger.warning(
                        "Job %s container %s gone — marking FAILED",
                        job["id"][:JOB_ID_DISPLAY_LENGTH],
                        expected,
                    )
                    await update_job(c, job["id"], status=JobStatus.FAILED)
                    # Drop unmeasured profile claims owned by this dead
                    # instance — no API-side sweep runs any more, so leaving
                    # the claim would block the cell for peers until the
                    # long-lease GC fires from the 60-min queue watcher.
                    await delete_unmeasured_claims(cur, job["id"])
                    if slot_node and n_gpus > 0:
                        await cur.execute(
                            "SELECT pg_notify(%s, %s)",
                            (PG_NOTIFY_SLOT_FREED, f"{slot_node}:{n_gpus}:{SlotFreedReason.ORPHAN_DRAIN}"),
                        )
            await c.commit()
    except Exception:
        logger.exception("Failed to reconcile job states")


async def pickup_queued_jobs() -> None:
    """Reset orphaned QUEUED rows so the API's central scheduler re-places them.

    Previously this directly dispatched the rows on the worker, but that
    bypassed the API's per-node slot semaphore: when the resulting container
    eventually completed, the worker emitted ``ijm_slot_freed`` and the API
    released a permit that had never been acquired.  Each worker-restart
    cycle leaked one phantom permit per picked-up job, which is exactly how
    matemagician ended up running 3 containers on 2 GPUs.

    The fix: clear ``assigned_node`` so the row goes back to "fresh QUEUED",
    then NOTIFY the API's scheduler so the next pass re-places the row
    through ``dispatch_with_slot`` (which DOES acquire a permit).  Brief
    extra latency on worker restart (sub-second once the API picks up the
    NOTIFY); slot accounting stays consistent.
    """
    try:
        async with conn() as c, c.cursor() as cur:
            await cur.execute(
                "SELECT id FROM jobs WHERE status = %s AND assigned_node = %s ORDER BY created_at ASC",
                (JobStatus.QUEUED, NODE_ID),
            )
            rows = await cur.fetchall()
            if rows:
                ids = [row["id"] for row in rows]
                logger.info(
                    "Found %d missed QUEUED job(s) on %s — clearing assignment for re-placement",
                    len(ids),
                    NODE_ID,
                )
                await cur.execute(
                    "UPDATE jobs SET assigned_node = NULL, assigned_gpu_config = NULL, "
                    "updated_at = NOW() "
                    "WHERE id = ANY(%s) AND status = %s AND assigned_node = %s",
                    (ids, JobStatus.QUEUED, NODE_ID),
                )
                await cur.execute(f"NOTIFY {PG_NOTIFY_SCHEDULE}")
                await c.commit()
    except Exception:
        logger.exception("Failed to reset orphaned QUEUED jobs")

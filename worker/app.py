#!/usr/bin/env python3
"""Worker HTTP server — executes training jobs in Docker containers.

Receives dispatch requests from the central IJM API and runs containers
locally. Updates job state directly in PostgreSQL. Designed to run on
each GPU node; the central API routes to the correct node by URL.
"""

import asyncio
import logging
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from shared.constants import OUTPUT_LOG_FILENAME, PG_NOTIFY_SLOT_FREED, RUNS_DIR, JobStatus

from constants import JOB_ID_DISPLAY_LENGTH, NODE_ID, WORKER_PORT, container_name_for
from db import conn, fetch_job, update_job
from docker import kill_container
from execution import HOST_ROOT, _job_tasks, dispatch_job, dispatch_ready, running_jobs, running_tasks, stopping
from reconcile import pickup_queued_jobs, reconcile_job_states

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    logger.info("Worker starting up (node_id=%s)", NODE_ID)
    await reconcile_job_states()
    await pickup_queued_jobs()
    yield
    logger.info("Worker shutting down — waiting for %d running job(s)", len(_job_tasks))
    if _job_tasks:
        await asyncio.gather(*_job_tasks, return_exceptions=True)


app = FastAPI(title="IJM Worker", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "node_id": NODE_ID, "running": list(running_jobs.keys())}


async def _zombie_release(job_id: str) -> None:
    """Mark the row FAILED and emit ijm_slot_freed for its assigned slot.

    Last-resort path when ``kill_container`` raises after retries: we'd
    rather log a manual-cleanup zombie container and keep the API's
    permit accounting consistent than block the whole node forever.
    """
    try:
        async with conn() as c:
            cur = await c.execute(
                "SELECT assigned_node, assigned_gpu_config FROM jobs WHERE id = %s",
                (job_id,),
            )
            row = await cur.fetchone()
            slot_node = row.get("assigned_node") if row else None
            slot_cfg = row.get("assigned_gpu_config") if row else None
            slot_n = sum(int(v) for v in (slot_cfg or {}).values())
            await update_job(c, job_id, status=JobStatus.FAILED)
            if slot_node and slot_n > 0:
                await c.execute(
                    "SELECT pg_notify(%s, %s)",
                    (PG_NOTIFY_SLOT_FREED, f"{slot_node}:{slot_n}"),
                )
            await c.commit()
    except Exception:
        logger.exception("_zombie_release failed for %s — slot may now be permanently leaked", job_id[:8])


@app.post("/jobs/{job_id}/run", status_code=202)
async def run_job(job_id: str) -> dict[str, str]:
    # Reject if the previous task is still draining (finally hasn't popped),
    # else a retry /run in that gap would spawn a duplicate _run_job.
    rt = running_tasks.get(job_id)
    if job_id in running_jobs or (rt is not None and not rt.done()):
        raise HTTPException(status_code=409, detail="Job already running or draining")
    dispatch_job(job_id)
    return {"status": "accepted"}


@app.post("/jobs/{job_id}/stop", status_code=202)
async def stop_job(job_id: str, reason: str = "user") -> dict[str, str]:
    """Stop a job's container.

    reason=user: status → PREEMPTED (manual resume).
    reason=auto: status → QUEUED with assignment cleared (next scheduler pass replaces it).
    """
    # Flag so the run task interprets imminent exit_code=137 as ours and
    # doesn't write status=FAILED before _persist_stop flips it.
    stopping[job_id] = reason
    try:
        return await _stop_job_impl(job_id, reason)
    finally:
        stopping.pop(job_id, None)


async def _stop_job_impl(job_id: str, reason: str) -> dict[str, str]:
    if reason == "auto":
        new_status = JobStatus.QUEUED
        clear_assignment = True
    else:
        new_status = JobStatus.PREEMPTED
        clear_assignment = False

    # Stale-stop guard: if the row points at a different node now, don't touch
    # the DB (would clobber the fresh assignment).  Still kill any local stray.
    async with conn() as c:
        row = await fetch_job(c, job_id, "assigned_node")
    if row is not None and row.get("assigned_node") not in (NODE_ID, None):
        stale_node = row.get("assigned_node")
        logger.info(
            "Stale /stop for %s: row.assigned_node=%s, this node=%s — skipping DB write",
            job_id[:JOB_ID_DISPLAY_LENGTH],
            stale_node,
            NODE_ID,
        )
        try:
            await kill_container(container_name_for(job_id))
        except Exception:
            # Defensive cleanup; missing container is the expected case.
            logger.debug(
                "No stray container for stale /stop %s (expected)",
                job_id[:JOB_ID_DISPLAY_LENGTH],
            )
        return {"status": "stale", "reason": reason}

    async def _persist_stop(c: Any) -> None:
        # Read assignment+status to emit ijm_slot_freed with the right payload
        # and avoid clobbering an already-terminal row.
        cur = await c.execute(
            "SELECT assigned_node, assigned_gpu_config, status FROM jobs WHERE id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
        prev_node = row.get("assigned_node") if row else None
        prev_cfg = row.get("assigned_gpu_config") if row else None
        prev_status = row.get("status") if row else None
        n_gpus = sum(int(v) for v in (prev_cfg or {}).values())

        # SUCCEEDED is terminal — never touch it.  PREEMPTED is *almost*
        # terminal, but the user-/stop path pre-flips status in the API, so for
        # reason=user we still need to kill the container and free the slot.
        if prev_status == JobStatus.SUCCEEDED:
            logger.info(
                "/stop for %s ignored — row already SUCCEEDED",
                job_id[:JOB_ID_DISPLAY_LENGTH],
            )
            await c.commit()
            return
        pre_flipped_user_stop = prev_status == JobStatus.PREEMPTED and reason == "user"
        if prev_status == JobStatus.PREEMPTED and reason != "user":
            logger.info(
                "/stop for %s (reason=%s) ignored — row already PREEMPTED by user-stop",
                job_id[:JOB_ID_DISPLAY_LENGTH],
                reason,
            )
            await c.commit()
            return

        if pre_flipped_user_stop:
            # API already wrote status=PREEMPTED.  Skip our redundant update.
            pass
        elif clear_assignment:
            await update_job(
                c, job_id, status=new_status, assigned_node=None, assigned_gpu_config=None, container_name=None
            )
        else:
            await update_job(c, job_id, status=new_status)

        # NOTIFY ijm_slot_freed when this run actually claimed the slot
        # (includes the pre-flipped-by-API case).
        notify_owns_slot = prev_status in (JobStatus.RUNNING, JobStatus.PROFILING) or pre_flipped_user_stop
        if prev_node and n_gpus > 0 and notify_owns_slot:
            await c.execute("SELECT pg_notify(%s, %s)", (PG_NOTIFY_SLOT_FREED, f"{prev_node}:{n_gpus}"))
        # Atomic commit of status flip + slot-freed NOTIFY — a split would
        # leave the API waiting for a release that never fires.
        await c.commit()

    # If dispatch is mid-launch (running_jobs[job_id] is the None placeholder)
    # wait for the container to actually exist — killing now would no-op and
    # persist QUEUED while the real container then starts under the QUEUED row.
    ready_event = dispatch_ready.get(job_id)
    if ready_event is not None and not ready_event.is_set():
        try:
            await asyncio.wait_for(ready_event.wait(), timeout=10.0)
        except TimeoutError:
            logger.warning(
                "Timed out waiting for Phase 2 of job %s — proceeding with /stop",
                job_id[:JOB_ID_DISPLAY_LENGTH],
            )

    process = running_jobs.get(job_id)
    if process is not None:
        name = container_name_for(job_id)
        # Terminate the docker-run subprocess directly — handles the window
        # before the daemon has registered the container (`docker kill <name>`
        # would miss with rc=1).
        try:
            process.terminate()
        except Exception:
            logger.exception("Failed to terminate docker run subprocess for %s", job_id[:8])
        # Kill BEFORE persisting so a silent kill failure can't leave a still-
        # running container under a QUEUED row.
        try:
            await kill_container(name)
        except Exception as exc:
            # Docker can't kill the container — mark FAILED + emit slot-freed
            # ourselves (Phase 4 / _persist_stop won't run if we raise).
            logger.exception(
                "Failed to kill container %s for %s — marking row zombie + releasing slot",
                name,
                job_id[:8],
            )
            await _zombie_release(job_id)
            raise HTTPException(status_code=500, detail=f"kill failed: {exc}") from exc
        # Drain the run task so the next dispatch doesn't hit the
        # `job_id in running_jobs` 409.  60 s is a safety net; steady-state
        # completion is ~200 ms after process.terminate().
        run_task = running_tasks.get(job_id)
        if run_task is not None and not run_task.done():
            try:
                await asyncio.wait_for(asyncio.shield(run_task), timeout=60.0)
            except TimeoutError:
                logger.warning(
                    "Run task for %s did not drain within 60s — proceeding to persist",
                    job_id[:JOB_ID_DISPLAY_LENGTH],
                )
            except Exception:
                # The run task itself logs+marks FAILED on exceptions; we
                # only care that it has completed for /stop purposes.
                pass
        async with conn() as c:
            await _persist_stop(c)
        logger.info("Killed container %s (reason=%s)", name, reason)
        return {"status": "stopped", "reason": reason}

    # Either the placeholder cleared (dispatch bailed out) or there was no
    # in-flight dispatch at all — fall through to the slow path.
    async with conn() as c:
        job = await fetch_job(c, job_id, "container_name", "status")
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job["status"] == JobStatus.QUEUED:
            await _persist_stop(c)
            return {"status": "stopped", "reason": reason}

        # PREEMPTED is allowed for reason=user: API atomically pre-flipped
        # status to PREEMPTED, and we still need to kill the container +
        # free the slot.  Auto-preempts must not touch a PREEMPTED row.
        if job["status"] == JobStatus.PREEMPTED and reason != "user":
            return {"status": "no_action"}
        if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING, JobStatus.PREEMPTED):
            return {"status": "no_action"}

        name = job["container_name"] or container_name_for(job_id)

    try:
        await kill_container(name)
    except Exception as exc:
        logger.exception(
            "Failed to kill container %s for %s (slow path) — marking row zombie + releasing slot",
            name,
            job_id[:8],
        )
        await _zombie_release(job_id)
        raise HTTPException(status_code=500, detail=f"kill failed: {exc}") from exc
    async with conn() as c:
        await _persist_stop(c)
    logger.info("Killed container %s (reason=%s)", name, reason)
    return {"status": "stopped", "reason": reason}


@app.get("/jobs/{job_id}/logs")
async def get_job_logs(job_id: str) -> PlainTextResponse:
    if not _UUID_RE.match(job_id):
        raise HTTPException(status_code=400, detail="Invalid job ID format")
    log_path = Path(HOST_ROOT) / "data" / RUNS_DIR / job_id / OUTPUT_LOG_FILENAME
    if not log_path.is_file():
        return PlainTextResponse("No logs available yet.\n", status_code=200)
    # ``X-Log-Mtime`` lets the API's fan-out pick the freshest copy when a
    # job has migrated between nodes (the old node's frozen file is longer
    # but staler than the new node's live one).
    stat = log_path.stat()
    content = await asyncio.to_thread(log_path.read_text)
    return PlainTextResponse(content, headers={"X-Log-Mtime": str(stat.st_mtime)})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=WORKER_PORT, log_level="info")

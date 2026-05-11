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
    # Reject duplicates not just when ``running_jobs`` has the id (Phase 1
    # has claimed) but also when the previous task is still draining (its
    # ``finally`` hasn't popped yet).  Without the second check, a retry
    # /run that lands in the millisecond gap between Phase 4 and the
    # finally would spawn a SECOND _run_job for the same id — two acquires
    # in API memory, two run tasks, eventually two attempts to start the
    # same-named container and confused state.
    rt = running_tasks.get(job_id)
    if job_id in running_jobs or (rt is not None and not rt.done()):
        raise HTTPException(status_code=409, detail="Job already running or draining")
    dispatch_job(job_id)
    return {"status": "accepted"}


@app.post("/jobs/{job_id}/stop", status_code=202)
async def stop_job(job_id: str, reason: str = "user") -> dict[str, str]:
    """Stop a job's container.

    ``reason=user`` (default): user-initiated stop.  Status → PREEMPTED so
    the job sits explicitly stopped until the user manually resumes it.
    ``reason=auto``: scheduler-initiated preemption (e.g. optimizer making
    room for an urgent job).  Status → QUEUED with ``assigned_node`` cleared
    so the next scheduling pass can place it elsewhere without going through
    a separate requeue step.  This avoids the misleading PREEMPTED state for
    jobs that are really just waiting for resources.
    """
    # Tell Phase 4 that any imminent exit_code=137 is OUR doing — without
    # this flag the run task races and writes status=FAILED before
    # ``_persist_stop`` can flip it.  Cleared in ``finally`` so a /stop that
    # errors mid-flight doesn't leak the flag.
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

    # Stale-stop guard.  Two consecutive optimizer preempt passes can each
    # spawn a /stop for the same job; the first lands and the row is
    # re-assigned to another node before the second arrives here.  If the
    # row no longer points at us we MUST NOT touch the DB — otherwise we'd
    # nullify the fresh assignment.  Kill any stray local container with
    # the canonical name as defensive cleanup (e.g. left over from a prior
    # run on this node) and return 200.  The API treats /stop as best-effort
    # idempotent so 200 is the right shape.
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
        # Read pre-stop assignment + status so we can NOTIFY ijm_slot_freed
        # with the right (node, n_gpus) AND avoid clobbering a terminal row
        # that was finalized by Phase 4 (FAILED/SUCCEEDED) or by a prior /stop.
        cur = await c.execute(
            "SELECT assigned_node, assigned_gpu_config, status FROM jobs WHERE id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
        prev_node = row.get("assigned_node") if row else None
        prev_cfg = row.get("assigned_gpu_config") if row else None
        prev_status = row.get("status") if row else None
        n_gpus = sum(int(v) for v in (prev_cfg or {}).values())

        # SUCCEEDED and PREEMPTED are genuinely terminal — /stop is meaningless
        # for them and we must not override.  FAILED, however, is a transient
        # label Phase 4 wrote because exit_code != 0; when /stop is the one
        # that just killed the container, exit_code 137 is OUR doing and
        # we want to flip to ``new_status`` (QUEUED for auto-preempt → row
        # gets re-scheduled, PREEMPTED for user-stop → user resumes manually).
        # A genuinely-failed job (image error, OOM) won't see /stop running
        # because nothing else preempts a finished job, so this branch is
        # only reached when our /stop did the killing.
        permanently_terminal = {JobStatus.SUCCEEDED, JobStatus.PREEMPTED}
        if prev_status in permanently_terminal:
            logger.info(
                "/stop for %s ignored — row already %s",
                job_id[:JOB_ID_DISPLAY_LENGTH],
                prev_status,
            )
            await c.commit()
            return

        if clear_assignment:
            await update_job(
                c, job_id, status=new_status, assigned_node=None, assigned_gpu_config=None, container_name=None
            )
        else:
            await update_job(c, job_id, status=new_status)

        # Only NOTIFY if the slot was still claimed by this run (status was
        # RUNNING/PROFILING when we read it).  For QUEUED rows the dispatch
        # task — if any — will release its own permit when its enqueue()
        # detects the cleared assigned_node and raises RuntimeError.  Emitting
        # NOTIFY here for QUEUED would race the dispatch's own release.
        notify_owns_slot = prev_status in (JobStatus.RUNNING, JobStatus.PROFILING)
        if prev_node and n_gpus > 0 and notify_owns_slot:
            # Use pg_notify() for safe payload parameterisation — node_id
            # is config-controlled but treating it as untrusted is cheap.
            await c.execute("SELECT pg_notify(%s, %s)", (PG_NOTIFY_SLOT_FREED, f"{prev_node}:{n_gpus}"))
        # Single commit so the status flip and the slot-freed notification
        # land atomically.  A crash between the two would otherwise leave
        # the API waiting for a slot release that never fires.
        await c.commit()

    # Wait for the dispatch placeholder to resolve.  ``running_jobs[job_id]``
    # is set to ``None`` synchronously after Phase 1's atomic claim and before
    # any awaitable, so seeing it here means a dispatch task is mid-launch.
    # Killing now would target a container that doesn't exist yet (the kill
    # post-condition "not in docker ps" is satisfied trivially), and we'd
    # then persist QUEUED while Phase 2 starts the real container — the
    # exact "RUNNING container under a QUEUED row" bug.  Wait on the
    # dispatch_ready Event instead of polling — Phase 2 sets it once the
    # Popen lands, or the run task's finally clause sets it on early bail.
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
        # Signal the `docker run` CLI subprocess directly. This handles the
        # window between Popen returning and the daemon registering the
        # container (where `docker kill <name>` would return rc=1 and miss).
        # `docker run --rm` propagates SIGTERM to stop+remove the container.
        try:
            process.terminate()
        except Exception:
            logger.exception("Failed to terminate docker run subprocess for %s", job_id[:8])
        # Kill BEFORE persisting. If the kill silently fails (rootless docker
        # SIGTERM dropped, daemon hiccup), persisting QUEUED+NULL would leave
        # the row pointing at a still-running container — exactly the bug we
        # hit where `progress` kept ticking up under a `QUEUED` row.
        try:
            await kill_container(name)
        except Exception as exc:
            # Zombie-kill defence: if docker can't kill the container (daemon
            # hung, disk pressure, rootless quirks), we'd otherwise leave
            # the row in RUNNING and the API permit acquired forever — far
            # worse than acknowledging a leaked GPU and freeing the slot
            # so other work can proceed.  Mark FAILED + emit ijm_slot_freed
            # explicitly here, since neither Phase 4 nor _persist_stop will
            # run if we raise.  Container is logged for manual cleanup.
            logger.exception(
                "Failed to kill container %s for %s — marking row zombie + releasing slot",
                name,
                job_id[:8],
            )
            await _zombie_release(job_id)
            raise HTTPException(status_code=500, detail=f"kill failed: {exc}") from exc
        # Drain the run task so the next dispatch doesn't hit the
        # `job_id in running_jobs` 409.  The 60 s timeout is a *safety net*
        # against pathological worker bugs that would hang /stop forever —
        # in steady state the task completes within ~200 ms after
        # process.terminate() + stream drain.  The API-side semaphore release
        # waits for /stop's completion, so this duration directly bounds the
        # slot-free signal; short timeouts would lie about slot freeness.
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

        if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
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

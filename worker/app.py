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

from constants import NODE_ID, WORKER_PORT, container_name_for
from db import conn, fetch_job, update_job
from docker import kill_container
from execution import HOST_ROOT, _job_tasks, dispatch_job, running_jobs
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


@app.post("/jobs/{job_id}/run", status_code=202)
async def run_job(job_id: str) -> dict[str, str]:
    if job_id in running_jobs:
        raise HTTPException(status_code=409, detail="Job already running")
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
    if reason == "auto":
        new_status = JobStatus.QUEUED
        clear_assignment = True
    else:
        new_status = JobStatus.PREEMPTED
        clear_assignment = False

    async def _persist_stop(c: Any) -> None:
        # Read pre-stop assignment so we can NOTIFY ijm_slot_freed with the
        # right (node, n_gpus) — the API listener releases that many permits.
        # Both auto- and user-stop free the slot (PREEMPTED jobs don't hold
        # slots even though their assigned_node may be retained for resume).
        cur = await c.execute(
            "SELECT assigned_node, assigned_gpu_config FROM jobs WHERE id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
        prev_node = row.get("assigned_node") if row else None
        prev_cfg = row.get("assigned_gpu_config") if row else None
        n_gpus = sum(int(v) for v in (prev_cfg or {}).values())

        if clear_assignment:
            await update_job(
                c, job_id, status=new_status, assigned_node=None, assigned_gpu_config=None, container_name=None
            )
        else:
            await update_job(c, job_id, status=new_status)

        if prev_node and n_gpus > 0:
            # Use pg_notify() for safe payload parameterisation — node_id
            # is config-controlled but treating it as untrusted is cheap.
            await c.execute("SELECT pg_notify(%s, %s)", (PG_NOTIFY_SLOT_FREED, f"{prev_node}:{n_gpus}"))
            await c.commit()

    # Wait for the dispatch placeholder to resolve.  ``running_jobs[job_id]``
    # is set to ``None`` synchronously after Phase 1's atomic claim and before
    # any awaitable, so seeing it here means a dispatch task is mid-launch.
    # Killing now would target a container that doesn't exist yet (the kill
    # post-condition "not in docker ps" is satisfied trivially), and we'd
    # then persist QUEUED while Phase 2 starts the real container — the
    # exact "RUNNING container under a QUEUED row" bug.  Bounded wait;
    # Phase 2 only does mkdir/rm-if-exists/Popen so it's normally <1s.
    if job_id in running_jobs and running_jobs.get(job_id) is None:
        for _ in range(100):  # ~10s
            await asyncio.sleep(0.1)
            if job_id not in running_jobs or running_jobs.get(job_id) is not None:
                break

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
            logger.exception("Failed to kill container %s for %s", name, job_id[:8])
            raise HTTPException(status_code=500, detail=f"kill failed: {exc}") from exc
        # Drain the run task so the next dispatch doesn't hit the
        # `job_id in running_jobs` 409. The bound is a *safety net* against
        # pathological worker bugs that would hang /stop forever — in steady
        # state the loop breaks at ~200 ms (immediately after process.wait
        # + stream_task drain).  60 s is generous enough that we never time
        # out under realistic load (and the API-side semaphore release waits
        # for /stop's completion, so this duration directly bounds the slot
        # free signal — short timeouts would lie about slot freeness).
        for _ in range(600):  # ~60 s safety bound
            if job_id not in running_jobs:
                break
            await asyncio.sleep(0.1)
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
        logger.exception("Failed to kill container %s for %s", name, job_id[:8])
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

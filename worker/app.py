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
from shared.constants import OUTPUT_LOG_FILENAME, RUNS_DIR, JobStatus

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
async def stop_job(job_id: str) -> dict[str, str]:
    process = running_jobs.get(job_id)
    if process:
        async with conn() as c:
            await update_job(c, job_id, status=JobStatus.PREEMPTED)
        name = container_name_for(job_id)
        result = await kill_container(name)
        logger.info("Killed container %s (rc=%d)", name, result.returncode)
        return {"status": "stopped"}

    async with conn() as c:
        job = await fetch_job(c, job_id, "container_name", "status")
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job["status"] == JobStatus.QUEUED:
            await update_job(c, job_id, status=JobStatus.PREEMPTED)
            return {"status": "preempted"}

        if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
            return {"status": "no_action"}

        name = job["container_name"] or container_name_for(job_id)
        await update_job(c, job_id, status=JobStatus.PREEMPTED)

    result = await kill_container(name)
    logger.info("Killed container %s (rc=%d)", name, result.returncode)
    return {"status": "stopped"}


@app.get("/jobs/{job_id}/logs")
async def get_job_logs(job_id: str) -> PlainTextResponse:
    if not _UUID_RE.match(job_id):
        raise HTTPException(status_code=400, detail="Invalid job ID format")
    log_path = Path(HOST_ROOT) / "data" / RUNS_DIR / job_id / OUTPUT_LOG_FILENAME
    if not log_path.is_file():
        return PlainTextResponse("No logs available yet.\n", status_code=200)
    content = await asyncio.to_thread(log_path.read_text)
    return PlainTextResponse(content)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=WORKER_PORT, log_level="info")

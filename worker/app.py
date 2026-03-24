#!/usr/bin/env python3
"""Worker HTTP server — executes training jobs in Docker containers.

Receives dispatch requests from the central IJM API and runs containers
locally. Updates job state directly in PostgreSQL. Designed to run on
each GPU node; the central API routes to the correct node by URL.
"""

import asyncio
import logging
import os
import re
import subprocess
import time
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Json
from shared.constants import DEFAULT_PROFILING_EPOCHS, OUTPUT_LOG_FILENAME, PG_NOTIFY_SCHEDULE, RUNS_DIR, JobStatus

from constants import (
    CHECKPOINT_DIR,
    CHECKPOINT_MOUNT_PATH,
    CONTAINER_NAME_PREFIX,
    DOCKER_CMD_TIMEOUT_SECONDS,
    JOB_ID_DISPLAY_LENGTH,
    NODE_ID,
    RUNNABLE_STATUSES,
    RUNS_MOUNT_PATH,
    WORKER_PORT,
)

# Regex to parse progress from training output, e.g. "Epoch 50/10000"
PROGRESS_RE = re.compile(r"Epoch\s+(\d+)/(\d+)")

JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", str(24 * 3600)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# In-process tracking: job_id → Popen process
running_jobs: dict[str, subprocess.Popen[str]] = {}
_job_tasks: set[asyncio.Task[None]] = set()

DATABASE_URL: str | None = os.getenv("DATABASE_URL")
HOST_ROOT: str = os.getenv("HOST_ROOT", "/host")
HOST_PROJECT_ROOT: str = os.path.normpath(os.getenv("HOST_PROJECT_ROOT", HOST_ROOT))


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


async def _connect_db() -> psycopg.AsyncConnection[Any]:
    if DATABASE_URL is None:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    return await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row)


@asynccontextmanager
async def _db() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    conn = await _connect_db()
    try:
        yield conn
    finally:
        await conn.close()


async def _update_job(conn: psycopg.AsyncConnection[Any], job_id: str, **fields: Any) -> None:
    fields["updated_at"] = datetime.now(UTC)
    sets = ", ".join(f"{k} = %({k})s" for k in fields)
    fields["_id"] = job_id
    async with conn.cursor() as cur:
        await cur.execute(f"UPDATE jobs SET {sets} WHERE id = %(_id)s", fields)  # noqa: S608
    await conn.commit()


async def _fetch_job(conn: psycopg.AsyncConnection[Any], job_id: str, *columns: str) -> dict[str, Any] | None:
    cols = ", ".join(columns) if columns else "*"
    async with conn.cursor() as cur:
        await cur.execute(f"SELECT {cols} FROM jobs WHERE id = %(id)s", {"id": job_id})  # noqa: S608
        return await cur.fetchone()


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------


def _build_docker_cmd(
    container_name: str,
    ckpt_host_path: str,
    runs_host_path: str,
    image: str,
    command: list[str],
    env_vars: dict[str, str] | None = None,
) -> list[str]:
    cmd = [
        "docker",
        "run",
        "--rm",
        "--name",
        container_name,
        "-v",
        f"{ckpt_host_path}:{CHECKPOINT_MOUNT_PATH}",
        "-v",
        f"{runs_host_path}:{RUNS_MOUNT_PATH}",
    ]
    for key, val in (env_vars or {}).items():
        cmd += ["-e", f"{key}={val}"]
    cmd.append(image)
    return cmd + command


async def _docker_run(*args: str, timeout: int = DOCKER_CMD_TIMEOUT_SECONDS) -> subprocess.CompletedProcess[str]:
    return await asyncio.to_thread(
        subprocess.run,
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


async def _list_containers() -> set[str]:
    result = await _docker_run("ps", "-a", "--filter", f"name={CONTAINER_NAME_PREFIX}", "--format", "{{.Names}}")
    return set(result.stdout.strip().split("\n")) if result.stdout.strip() else set()


async def _kill_container(container_name: str) -> subprocess.CompletedProcess[str]:
    return await _docker_run("kill", container_name)


# ---------------------------------------------------------------------------
# Startup reconciliation
# ---------------------------------------------------------------------------


async def _reconcile_job_states() -> None:
    """Mark RUNNING/PROFILING jobs as FAILED if their container is gone."""
    logger.info("Reconciling job states for node %s", NODE_ID)
    try:
        running_containers = await _list_containers()
        async with _db() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT id, container_name, status FROM jobs WHERE status IN (%s, %s) AND assigned_node = %s",
                (JobStatus.RUNNING, JobStatus.PROFILING, NODE_ID),
            )
            jobs = await cur.fetchall()
            for job in jobs:
                expected = job["container_name"] or f"{CONTAINER_NAME_PREFIX}{job['id'][:JOB_ID_DISPLAY_LENGTH]}"
                if expected not in running_containers:
                    logger.warning(
                        "Job %s container %s gone — marking FAILED", job["id"][:JOB_ID_DISPLAY_LENGTH], expected
                    )
                    await _update_job(conn, job["id"], status=JobStatus.FAILED)
    except Exception:
        logger.exception("Failed to reconcile job states")


async def _pickup_queued_jobs() -> None:
    """Enqueue QUEUED jobs assigned to this node that were missed while the worker was down."""
    try:
        async with _db() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT id FROM jobs WHERE status = %s AND assigned_node = %s ORDER BY created_at ASC",
                (JobStatus.QUEUED, NODE_ID),
            )
            rows = await cur.fetchall()
        if rows:
            logger.info("Found %d missed QUEUED job(s) — enqueuing", len(rows))
            for row in rows:
                _dispatch_job(row["id"])
    except Exception:
        logger.exception("Failed to pick up queued jobs")


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------


def _resolve_paths(job_id: str) -> tuple[Path, Path, Path, Path]:
    ckpt_local = Path(HOST_ROOT) / "data" / CHECKPOINT_DIR / job_id
    runs_local = Path(HOST_ROOT) / "data" / RUNS_DIR / job_id
    ckpt_host = Path(HOST_PROJECT_ROOT) / "data" / CHECKPOINT_DIR / job_id
    runs_host = Path(HOST_PROJECT_ROOT) / "data" / RUNS_DIR / job_id
    return ckpt_local, runs_local, ckpt_host, runs_host


def _prepare_checkpoint_dir(ckpt_local: Path, *, is_profiling: bool, is_first_run: bool) -> Path:
    if is_profiling:
        mount = ckpt_local / ".profiling"
        mount.mkdir(parents=True, exist_ok=True)
        for f in mount.iterdir():
            if f.is_file():
                f.unlink(missing_ok=True)
        return mount
    ckpt_local.mkdir(parents=True, exist_ok=True)
    if is_first_run:
        for f in ckpt_local.iterdir():
            if f.is_file():
                f.unlink(missing_ok=True)
    return ckpt_local


def _build_env_vars(job: dict[str, Any]) -> dict[str, str]:
    env: dict[str, str] = {
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
    }
    if job["is_profiling_run"]:
        env["EPOCHS_TOTAL"] = str(job.get("profiling_epochs_no") or DEFAULT_PROFILING_EPOCHS)
    elif job.get("epochs_total"):
        env["EPOCHS_TOTAL"] = str(job["epochs_total"])
    if job.get("batch_size"):
        env["BATCH_SIZE"] = str(job["batch_size"])
    return env


async def _stream_output(
    process: subprocess.Popen[str],
    conn: psycopg.AsyncConnection[Any],
    job_id: str,
    log_path: Path,
    epoch_timestamps: list[tuple[int, float]],
    *,
    is_profiling: bool,
) -> None:
    loop = asyncio.get_running_loop()
    stdout = process.stdout
    if stdout is None:
        return
    try:
        with open(log_path, "a") as log_file:
            while True:
                line = await loop.run_in_executor(None, stdout.readline)
                if not line:
                    break
                stripped = line.rstrip()
                logger.info("[Job %s] %s", job_id[:JOB_ID_DISPLAY_LENGTH], stripped)
                log_file.write(line)
                log_file.flush()
                match = PROGRESS_RE.search(stripped)
                if match:
                    epoch_num = int(match.group(1))
                    progress = f"{epoch_num}/{match.group(2)}"
                    await _update_job(conn, job_id, progress=progress)
                    if is_profiling:
                        epoch_timestamps.append((epoch_num, time.monotonic()))
    except Exception as exc:
        logger.debug("Output streaming ended: %s", exc)


async def _wait_for_process(process: subprocess.Popen[str], job_id: str) -> int:
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, process.wait),
            timeout=JOB_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.error("Job %s timed out after %ds", job_id[:JOB_ID_DISPLAY_LENGTH], JOB_TIMEOUT_SECONDS)
        process.terminate()
        return await loop.run_in_executor(None, process.wait)


def _compute_profiling_duration(
    epoch_timestamps: list[tuple[int, float]] | None,
    run_start_time: datetime,
) -> float:
    if epoch_timestamps and len(epoch_timestamps) >= 3:
        intervals = [epoch_timestamps[i + 1][1] - epoch_timestamps[i][1] for i in range(len(epoch_timestamps) - 1)]
        steady = intervals[1:]
        mean_epoch_time = sum(steady) / len(steady)
        total_epochs = epoch_timestamps[-1][0]
        logger.info("Profiling: warmup=%.4fs, steady mean=%.4fs/epoch", intervals[0], mean_epoch_time)
        return mean_epoch_time * total_epochs
    return (datetime.now(UTC) - run_start_time).total_seconds()


async def _handle_profiling_complete(
    conn: psycopg.AsyncConnection[Any],
    job_id: str,
    run_start_time: datetime,
    epoch_timestamps: list[tuple[int, float]],
) -> bool:
    """Write profiling result to DB and reset job to QUEUED for re-scheduling.

    Returns True if this was a profiling run.
    """
    job = await _fetch_job(conn, job_id, "is_profiling_run", "assigned_gpu_config", "assigned_node", "job_id")
    if not job or not job["is_profiling_run"]:
        return False

    duration = _compute_profiling_duration(epoch_timestamps, run_start_time)
    gpu_config = job["assigned_gpu_config"]
    node_id = job["assigned_node"]
    type_id = job["job_id"]

    now = datetime.now(UTC)
    async with conn.cursor() as cur:
        await cur.execute(
            """UPDATE profiling_results
               SET duration_seconds = %s, node_id = %s
               WHERE job_id = %s AND gpu_config = %s::jsonb AND duration_seconds IS NULL""",
            (duration, node_id, type_id, Json(gpu_config)),
        )
        # Reset job to QUEUED so the central API's queue watcher re-schedules it
        await cur.execute(
            """UPDATE jobs SET status = %s, assigned_node = NULL, assigned_gpu_config = NULL,
               is_profiling_run = FALSE, updated_at = %s WHERE id = %s""",
            (JobStatus.QUEUED, now, job_id),
        )
    await conn.commit()
    await conn.execute(f"NOTIFY {PG_NOTIFY_SCHEDULE}")  # wake the API scheduler immediately
    await conn.commit()

    logger.info(
        "Profiling complete for job %s (type=%s): %s = %.1fs — reset to QUEUED",
        job_id[:JOB_ID_DISPLAY_LENGTH],
        type_id,
        gpu_config,
        duration,
    )
    return True


async def _run_job(job_id: str) -> None:
    """Execute a job in a Docker container (background task)."""
    try:
        async with _db() as conn:
            job = await _fetch_job(
                conn,
                job_id,
                "image",
                "command",
                "status",
                "is_profiling_run",
                "profiling_epochs_no",
                "exit_code",
                "epochs_total",
                "batch_size",
                "assigned_node",
            )
            if not job:
                logger.error("Job %s not found", job_id)
                return
            if job["status"] not in RUNNABLE_STATUSES:
                logger.info("Job %s status %s not runnable, skipping", job_id[:JOB_ID_DISPLAY_LENGTH], job["status"])
                return

            run_status = JobStatus.PROFILING if job["is_profiling_run"] else JobStatus.RUNNING
            run_start_time = datetime.now(UTC)
            await _update_job(conn, job_id, status=run_status, progress=None)

            ckpt_local, runs_local, ckpt_host, runs_host = _resolve_paths(job_id)
            ckpt_local.mkdir(parents=True, exist_ok=True)
            runs_local.mkdir(parents=True, exist_ok=True)

            is_first_run = job["exit_code"] is None
            _prepare_checkpoint_dir(ckpt_local, is_profiling=job["is_profiling_run"], is_first_run=is_first_run)
            ckpt_host_mount = (ckpt_host / ".profiling") if job["is_profiling_run"] else ckpt_host

            container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
            await _update_job(conn, job_id, container_name=container_name)

            env_vars = _build_env_vars(job)
            docker_cmd = _build_docker_cmd(
                container_name,
                str(ckpt_host_mount),
                str(runs_host),
                job["image"],
                job["command"],
                env_vars=env_vars,
            )
            logger.info("Starting job %s: %s", job_id[:JOB_ID_DISPLAY_LENGTH], " ".join(docker_cmd))

            process = subprocess.Popen(docker_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            running_jobs[job_id] = process

            epoch_timestamps: list[tuple[int, float]] = []
            log_path = runs_local / OUTPUT_LOG_FILENAME
            stream_task = asyncio.create_task(
                _stream_output(process, conn, job_id, log_path, epoch_timestamps, is_profiling=job["is_profiling_run"])
            )

            exit_code = await _wait_for_process(process, job_id)
            await stream_task
            running_jobs.pop(job_id, None)

            # Handle completion
            current = await _fetch_job(conn, job_id, "status")
            if current and current["status"] == JobStatus.PREEMPTED:
                logger.info("Job %s was preempted, keeping status", job_id[:JOB_ID_DISPLAY_LENGTH])
                await _update_job(conn, job_id, exit_code=exit_code)
                return

            if exit_code != 0:
                logger.error("Job %s failed (exit=%d)", job_id[:JOB_ID_DISPLAY_LENGTH], exit_code)
                await _update_job(conn, job_id, status=JobStatus.FAILED, exit_code=exit_code)
                return

            is_profiling = await _handle_profiling_complete(conn, job_id, run_start_time, epoch_timestamps)
            if not is_profiling:
                logger.info("Job %s completed successfully", job_id[:JOB_ID_DISPLAY_LENGTH])
                await _update_job(conn, job_id, status=JobStatus.SUCCEEDED, exit_code=exit_code)

    except Exception:
        logger.exception("Failed to run job %s", job_id)
        try:
            async with _db() as conn:
                await _update_job(conn, job_id, status=JobStatus.FAILED)
        except Exception:
            logger.exception("Failed to update job %s status to FAILED", job_id)
    finally:
        running_jobs.pop(job_id, None)


def _dispatch_job(job_id: str) -> asyncio.Task[None]:
    task: asyncio.Task[None] = asyncio.create_task(_run_job(job_id), name=f"job-{job_id[:JOB_ID_DISPLAY_LENGTH]}")
    _job_tasks.add(task)
    task.add_done_callback(_job_tasks.discard)
    return task


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    logger.info("Worker starting up (node_id=%s)", NODE_ID)
    await _reconcile_job_states()
    await _pickup_queued_jobs()
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
    _dispatch_job(job_id)
    return {"status": "accepted"}


@app.post("/jobs/{job_id}/stop", status_code=202)
async def stop_job(job_id: str) -> dict[str, str]:
    # Fast path: tracked in-process
    process = running_jobs.get(job_id)
    if process:
        async with _db() as conn:
            await _update_job(conn, job_id, status=JobStatus.PREEMPTED)
        container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
        result = await _kill_container(container_name)
        logger.info("Killed container %s (rc=%d)", container_name, result.returncode)
        return {"status": "stopped"}

    # Slow path: look up from DB
    async with _db() as conn:
        job = await _fetch_job(conn, job_id, "container_name", "status")
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job["status"] == JobStatus.QUEUED:
            await _update_job(conn, job_id, status=JobStatus.PREEMPTED)
            return {"status": "preempted"}

        if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
            return {"status": "no_action"}

        container_name = job["container_name"] or f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
        await _update_job(conn, job_id, status=JobStatus.PREEMPTED)

    result = await _kill_container(container_name)
    logger.info("Killed container %s (rc=%d)", container_name, result.returncode)
    return {"status": "stopped"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=WORKER_PORT, log_level="info")

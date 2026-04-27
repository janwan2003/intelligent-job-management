"""Job execution lifecycle for the IJM worker."""

import asyncio
import logging
import os
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from shared.constants import DEFAULT_PROFILING_EPOCHS, OUTPUT_LOG_FILENAME, PG_NOTIFY_SCHEDULE, RUNS_DIR, JobStatus

from constants import CHECKPOINT_DIR, JOB_ID_DISPLAY_LENGTH, RUNNABLE_STATUSES, container_name_for
from db import conn, fetch_job, update_job
from docker import build_run_cmd
from profiling import handle_complete

logger = logging.getLogger(__name__)

PROGRESS_RE = re.compile(r"Epoch\s+(\d+)/(\d+)")
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", str(24 * 3600)))

HOST_ROOT: str = os.getenv("HOST_ROOT", "/host")
HOST_PROJECT_ROOT: str = os.path.normpath(os.getenv("HOST_PROJECT_ROOT", HOST_ROOT))

# In-process tracking
running_jobs: dict[str, subprocess.Popen[str]] = {}
_job_tasks: set[asyncio.Task[None]] = set()


def _resolve_paths(job_id: str) -> tuple[Path, Path, Path, Path]:
    """Return (ckpt_local, runs_local, ckpt_host, runs_host)."""
    ckpt_local = Path(HOST_ROOT) / "data" / CHECKPOINT_DIR / job_id
    runs_local = Path(HOST_ROOT) / "data" / RUNS_DIR / job_id
    ckpt_host = Path(HOST_PROJECT_ROOT) / "data" / CHECKPOINT_DIR / job_id
    runs_host = Path(HOST_PROJECT_ROOT) / "data" / RUNS_DIR / job_id
    return ckpt_local, runs_local, ckpt_host, runs_host


def _prepare_checkpoint_dir(ckpt_local: Path, *, is_profiling: bool, is_first_run: bool) -> None:
    """Create and clean checkpoint directory. Profiling uses an isolated .profiling/ subdir."""
    if is_profiling:
        mount = ckpt_local / ".profiling"
        mount.mkdir(parents=True, exist_ok=True)
        for f in mount.iterdir():
            if f.is_file():
                f.unlink(missing_ok=True)
    else:
        ckpt_local.mkdir(parents=True, exist_ok=True)
        if is_first_run:
            for f in ckpt_local.iterdir():
                if f.is_file():
                    f.unlink(missing_ok=True)


def _build_env_vars(job: dict[str, Any]) -> dict[str, str]:
    """Build environment variables for the training container."""
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
    job_id: str,
    log_path: Path,
    epoch_timestamps: list[tuple[int, float]],
    *,
    is_profiling: bool,
) -> None:
    """Stream container stdout, write to log file, parse progress updates."""
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
                    async with conn() as c:
                        await update_job(c, job_id, progress=progress)
                    if is_profiling:
                        epoch_timestamps.append((epoch_num, time.monotonic()))
    except Exception as exc:
        logger.debug("Output streaming ended: %s", exc)


async def _run_job(job_id: str) -> None:
    """Execute a job in a Docker container."""
    try:
        # Phase 1: fetch job + set status (short-lived connection)
        async with conn() as c:
            job = await fetch_job(
                c,
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
            name = container_name_for(job_id)
            await update_job(c, job_id, status=run_status, progress=None, container_name=name)

        # Phase 2: prepare dirs, launch container (no DB connection held)
        ckpt_local, runs_local, ckpt_host, runs_host = _resolve_paths(job_id)
        ckpt_local.mkdir(parents=True, exist_ok=True)
        runs_local.mkdir(parents=True, exist_ok=True)

        _prepare_checkpoint_dir(ckpt_local, is_profiling=job["is_profiling_run"], is_first_run=job["exit_code"] is None)
        ckpt_host_mount = (ckpt_host / ".profiling") if job["is_profiling_run"] else ckpt_host

        # Shared dataset cache (e.g. MNIST) — mounted at /runs/data inside the
        # container so training scripts can find pre-downloaded data.
        shared_data_local = Path(HOST_ROOT) / "data" / "shared" / "data"
        shared_data_host = Path(HOST_PROJECT_ROOT) / "data" / "shared" / "data"
        extra_volumes: dict[str, str] = {}
        if shared_data_local.exists():
            extra_volumes[str(shared_data_host)] = "/runs/data"

        docker_cmd = build_run_cmd(
            name,
            str(ckpt_host_mount),
            str(runs_host),
            job["image"],
            job["command"],
            env_vars=_build_env_vars(job),
            extra_volumes=extra_volumes,
        )
        logger.info("Starting job %s: %s", job_id[:JOB_ID_DISPLAY_LENGTH], " ".join(docker_cmd))

        process = subprocess.Popen(docker_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        running_jobs[job_id] = process

        # Phase 3: stream output + wait (connections acquired per progress update)
        epoch_timestamps: list[tuple[int, float]] = []
        log_path = runs_local / OUTPUT_LOG_FILENAME
        stream_task = asyncio.create_task(
            _stream_output(process, job_id, log_path, epoch_timestamps, is_profiling=job["is_profiling_run"])
        )

        loop = asyncio.get_running_loop()
        try:
            exit_code: int = await asyncio.wait_for(
                loop.run_in_executor(None, process.wait),
                timeout=JOB_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.error("Job %s timed out after %ds", job_id[:JOB_ID_DISPLAY_LENGTH], JOB_TIMEOUT_SECONDS)
            process.terminate()
            exit_code = await loop.run_in_executor(None, process.wait)

        await stream_task
        running_jobs.pop(job_id, None)

        # Phase 4: handle completion (short-lived connection)
        async with conn() as c:
            current = await fetch_job(c, job_id, "status")
            if current and current["status"] == JobStatus.PREEMPTED:
                logger.info("Job %s was preempted, keeping status", job_id[:JOB_ID_DISPLAY_LENGTH])
                await update_job(c, job_id, exit_code=exit_code)
                await c.execute(f"NOTIFY {PG_NOTIFY_SCHEDULE}")
                await c.commit()
                return

            if exit_code != 0:
                logger.error("Job %s failed (exit=%d)", job_id[:JOB_ID_DISPLAY_LENGTH], exit_code)
                await update_job(c, job_id, status=JobStatus.FAILED, exit_code=exit_code)
                return

            is_profiling = await handle_complete(c, job_id, run_start_time, epoch_timestamps)
            if not is_profiling:
                logger.info("Job %s completed successfully", job_id[:JOB_ID_DISPLAY_LENGTH])
                await update_job(c, job_id, status=JobStatus.SUCCEEDED, exit_code=exit_code)

    except Exception:
        logger.exception("Failed to run job %s", job_id)
        try:
            async with conn() as c:
                await update_job(c, job_id, status=JobStatus.FAILED)
        except Exception:
            logger.exception("Failed to update job %s status to FAILED", job_id)
    finally:
        running_jobs.pop(job_id, None)


def dispatch_job(job_id: str) -> asyncio.Task[None]:
    """Create an asyncio task for job execution."""
    task: asyncio.Task[None] = asyncio.create_task(_run_job(job_id), name=f"job-{job_id[:JOB_ID_DISPLAY_LENGTH]}")
    _job_tasks.add(task)
    task.add_done_callback(_job_tasks.discard)
    return task

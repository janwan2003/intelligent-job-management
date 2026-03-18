#!/usr/bin/env python3
"""
Worker service for job execution.

Subscribes to NATS events and manages Docker container execution for jobs.
Supports concurrent job execution across multiple nodes.
"""

import asyncio
import json
import logging
import os
import re
import signal
import subprocess
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import nats
import psycopg
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy
from psycopg.rows import dict_row
from shared.constants import (
    DEFAULT_PROFILING_EPOCHS,
    NATS_STREAM_NAME,
    NATS_SUBJECT_COMPLETED,
    NATS_SUBJECT_PROFILING_COMPLETE,
    NATS_SUBJECT_STOP_REQUESTED,
    NATS_SUBJECT_SUBMITTED,
    NATS_SUBJECTS_PATTERN,
    OUTPUT_LOG_FILENAME,
    RUNS_DIR,
    JobStatus,
)

from constants import (
    CHECKPOINT_DIR,
    CHECKPOINT_MOUNT_PATH,
    CONSUMER_RESUME,
    CONSUMER_STOP,
    CONSUMER_SUBMITTED,
    CONTAINER_NAME_PREFIX,
    DEFAULT_NATS_URL,
    DOCKER_CMD_TIMEOUT_SECONDS,
    JOB_ID_DISPLAY_LENGTH,
    NATS_SUBJECT_RESUME_REQUESTED,
    RUNNABLE_STATUSES,
    RUNS_MOUNT_PATH,
)

# Regex to parse progress from training output, e.g. "Epoch 50/10000"
PROGRESS_RE = re.compile(r"Epoch\s+(\d+)/(\d+)")

# Maximum wall-clock time for a single job (default: 24 hours)
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", str(24 * 3600)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class JobWorker:
    """Worker that executes jobs in Docker containers."""

    def __init__(self) -> None:
        self.database_url: str | None = os.getenv("DATABASE_URL")
        self.nats_url: str = os.getenv("NATS_URL", DEFAULT_NATS_URL)
        self.host_root: str = os.getenv("HOST_ROOT", "/host")
        # HOST_PROJECT_ROOT is the actual host filesystem path that corresponds
        # to self.host_root.  docker run -v mounts are resolved by the Docker
        # daemon on the HOST, not inside this container.
        self.host_project_root: str = os.path.normpath(os.getenv("HOST_PROJECT_ROOT", self.host_root))

        self.nc: Any = None
        self.js: JetStreamContext | None = None
        self.job_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self.running: bool = True
        # Track all concurrently running jobs: job_id -> Popen process
        self.running_jobs: dict[str, subprocess.Popen[str]] = {}
        self._job_tasks: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    async def connect_db(self) -> psycopg.AsyncConnection[Any]:
        """Create a new database connection (mockable in tests)."""
        if self.database_url is None:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        return await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row)

    @asynccontextmanager
    async def _db(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        """Open a short-lived DB connection, closed automatically."""
        conn = await self.connect_db()
        try:
            yield conn
        finally:
            await conn.close()

    async def _update_job(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        **fields: Any,
    ) -> None:
        """Update arbitrary fields on a job row."""
        fields["updated_at"] = datetime.now(UTC)
        sets = ", ".join(f"{k} = %({k})s" for k in fields)
        fields["_id"] = job_id
        async with conn.cursor() as cur:
            await cur.execute(f"UPDATE jobs SET {sets} WHERE id = %(_id)s", fields)  # noqa: S608
            await conn.commit()

    # ------------------------------------------------------------------
    # Docker helpers
    # ------------------------------------------------------------------

    @staticmethod
    def build_docker_cmd(
        container_name: str,
        ckpt_host_path: str,
        runs_host_path: str,
        image: str,
        command: list[str],
        env_vars: dict[str, str] | None = None,
    ) -> list[str]:
        """Build the docker run command list.

        Separated for testability — volume paths must use HOST filesystem
        paths since the Docker daemon resolves them on the host.
        """
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

    @staticmethod
    async def _docker_run(*args: str, timeout: int = DOCKER_CMD_TIMEOUT_SECONDS) -> subprocess.CompletedProcess[str]:
        """Run a docker CLI command in a thread."""
        return await asyncio.to_thread(
            subprocess.run,
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    async def _list_containers(self) -> set[str]:
        """Return names of all containers with the IJM prefix."""
        result = await self._docker_run(
            "ps", "-a", "--filter", f"name={CONTAINER_NAME_PREFIX}", "--format", "{{.Names}}"
        )
        return set(result.stdout.strip().split("\n")) if result.stdout.strip() else set()

    async def _kill_container(self, container_name: str) -> subprocess.CompletedProcess[str]:
        """Kill a container immediately (no grace period — checkpoints are saved per-epoch)."""
        return await self._docker_run("kill", container_name)

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the worker service."""
        logger.info("Starting worker service")

        # Connect to NATS
        logger.info("Connecting to NATS at %s", self.nats_url)
        self.nc = await nats.connect(self.nats_url)
        self.js = self.nc.jetstream()

        # Ensure streams exist
        await self._ensure_streams()

        # Reconcile job states with actual container status
        await self._reconcile_job_states()

        # Pick up any QUEUED jobs that were missed while worker was down
        await self._pickup_queued_jobs()

        # Subscribe to job events
        await self._subscribe_to_events()

        # Start job runner loop
        runner_task = asyncio.create_task(self._job_runner())

        logger.info("Worker ready, waiting for jobs")

        # Graceful shutdown on SIGTERM/SIGINT
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, shutdown_event.set)

        await shutdown_event.wait()
        logger.info("Shutting down")
        self.running = False
        runner_task.cancel()
        # Wait for all running job tasks to finish
        if self._job_tasks:
            logger.info("Waiting for %d running job(s) to finish...", len(self._job_tasks))
            await asyncio.gather(*self._job_tasks, return_exceptions=True)
        if self.nc:
            await self.nc.close()

    async def _ensure_streams(self) -> None:
        """Ensure NATS JetStream streams exist."""
        if self.js is None:
            raise RuntimeError("NATS JetStream not connected")
        try:
            await self.js.add_stream(name=NATS_STREAM_NAME, subjects=[NATS_SUBJECTS_PATTERN])
            logger.info("JetStream stream '%s' ready", NATS_STREAM_NAME)
        except Exception as e:
            logger.debug("Stream setup: %s", e)

    async def _reconcile_job_states(self) -> None:
        """Reconcile job states with actual Docker container status."""
        logger.info("Reconciling job states")
        try:
            running_containers = await self._list_containers()

            async with self._db() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT id, container_name, status FROM jobs WHERE status IN (%s, %s, %s)",
                    (JobStatus.RUNNING, JobStatus.PROFILING, JobStatus.QUEUED),
                )
                jobs = await cur.fetchall()

                reconciled_count = 0
                for job in jobs:
                    if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
                        continue
                    expected = job["container_name"] or f"{CONTAINER_NAME_PREFIX}{job['id'][:JOB_ID_DISPLAY_LENGTH]}"
                    if expected not in running_containers:
                        logger.warning(
                            "Job %s marked %s but container %s not found — marking FAILED",
                            job["id"][:JOB_ID_DISPLAY_LENGTH],
                            job["status"],
                            expected,
                        )
                        await self._update_job(conn, job["id"], status=JobStatus.FAILED)
                        reconciled_count += 1

            if reconciled_count > 0:
                logger.info("Reconciled %d orphaned job(s)", reconciled_count)
            else:
                logger.info("All job states are consistent")

        except Exception as e:
            logger.error("Failed to reconcile job states: %s", e, exc_info=True)

    async def _pickup_queued_jobs(self) -> None:
        """Enqueue any QUEUED jobs that were missed while the worker was down."""
        try:
            async with self._db() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT id FROM jobs WHERE status = %s ORDER BY created_at ASC",
                    (JobStatus.QUEUED,),
                )
                rows = await cur.fetchall()

            if rows:
                logger.info("Found %d QUEUED job(s) from before startup — enqueuing", len(rows))
                for row in rows:
                    await self.job_queue.put(("run", row["id"]))
            else:
                logger.info("No missed QUEUED jobs")
        except Exception as e:
            logger.error("Failed to pick up queued jobs: %s", e, exc_info=True)

    # ------------------------------------------------------------------
    # NATS event handlers
    # ------------------------------------------------------------------

    async def _subscribe_to_events(self) -> None:
        """Subscribe to NATS job events."""
        if self.js is None:
            raise RuntimeError("NATS JetStream not connected")

        for subject, cb, durable in [
            (NATS_SUBJECT_SUBMITTED, self._handle_job_submitted, CONSUMER_SUBMITTED),
            (NATS_SUBJECT_STOP_REQUESTED, self._handle_stop_requested, CONSUMER_STOP),
            (NATS_SUBJECT_RESUME_REQUESTED, self._handle_resume_requested, CONSUMER_RESUME),
        ]:
            await self.js.subscribe(
                subject,
                cb=cb,
                durable=durable,
                config=nats.js.api.ConsumerConfig(deliver_policy=DeliverPolicy.NEW),
            )

        logger.info("Subscribed to job events")

    async def _handle_job_submitted(self, msg: Any) -> None:
        """Handle jobs.submitted event."""
        try:
            job_id = json.loads(msg.data.decode())["job_id"]
            logger.info("Job submitted: %s", job_id)
            await self.job_queue.put(("run", job_id))
            await msg.ack()
        except Exception as e:
            logger.error("Failed to handle job submission: %s", e, exc_info=True)
            await msg.nak()

    async def _handle_stop_requested(self, msg: Any) -> None:
        """Handle jobs.stop_requested event."""
        try:
            job_id = json.loads(msg.data.decode())["job_id"]
            logger.info("Stop requested for job: %s", job_id[:JOB_ID_DISPLAY_LENGTH])
            await self._stop_job(job_id)
            await msg.ack()
        except Exception as e:
            logger.error("Failed to handle stop request: %s", e, exc_info=True)
            await msg.nak()

    async def _handle_resume_requested(self, msg: Any) -> None:
        """Handle jobs.resume_requested event."""
        try:
            job_id = json.loads(msg.data.decode())["job_id"]
            logger.info("Resume requested for job: %s", job_id)
            await self.job_queue.put(("run", job_id))
            await msg.ack()
        except Exception as e:
            logger.error("Failed to handle resume request: %s", e, exc_info=True)
            await msg.nak()

    # ------------------------------------------------------------------
    # Job dispatch loop
    # ------------------------------------------------------------------

    async def _job_runner(self) -> None:
        """Main job dispatch loop — launches jobs concurrently."""
        while self.running:
            try:
                action, job_id = await asyncio.wait_for(self.job_queue.get(), timeout=1.0)

                if action == "run":
                    if job_id in self.running_jobs:
                        logger.info("Job %s already running, skipping", job_id[:JOB_ID_DISPLAY_LENGTH])
                        continue
                    task = asyncio.create_task(self._run_job(job_id))
                    self._job_tasks.add(task)
                    task.add_done_callback(self._job_tasks.discard)

            except TimeoutError:
                continue
            except Exception as e:
                logger.error("Job runner error: %s", e, exc_info=True)
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _resolve_paths(self, job_id: str) -> tuple[Path, Path, Path, Path]:
        """Return (ckpt_local, runs_local, ckpt_host, runs_host) for a job."""
        ckpt_local = Path(self.host_root) / "data" / CHECKPOINT_DIR / job_id
        runs_local = Path(self.host_root) / "data" / RUNS_DIR / job_id
        ckpt_host = Path(self.host_project_root) / "data" / CHECKPOINT_DIR / job_id
        runs_host = Path(self.host_project_root) / "data" / RUNS_DIR / job_id
        return ckpt_local, runs_local, ckpt_host, runs_host

    @staticmethod
    def _prepare_checkpoint_dir(ckpt_local: Path, *, is_profiling: bool, is_first_run: bool) -> Path:
        """Set up the checkpoint directory and return the path to mount.

        Profiling runs use an isolated subdirectory.  First full runs clear
        stale profiling checkpoints.  Resumed runs keep existing checkpoints.
        """
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

    @staticmethod
    def _build_env_vars(job: dict[str, Any]) -> dict[str, str]:
        """Build environment variables to pass into the container."""
        env: dict[str, str] = {}
        if job["is_profiling_run"]:
            env["EPOCHS_TOTAL"] = str(job.get("profiling_epochs_no") or DEFAULT_PROFILING_EPOCHS)
        elif job.get("epochs_total"):
            env["EPOCHS_TOTAL"] = str(job["epochs_total"])
        if job.get("batch_size"):
            env["BATCH_SIZE"] = str(job["batch_size"])
        return env

    async def _stream_output(
        self,
        process: subprocess.Popen[str],
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        log_path: Path,
        epoch_timestamps: list[tuple[int, float]],
        *,
        is_profiling: bool,
    ) -> None:
        """Stream container stdout to log file, parse progress, collect timestamps."""
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
                        await self._update_job(conn, job_id, progress=progress)
                        if is_profiling:
                            epoch_timestamps.append((epoch_num, time.monotonic()))
        except Exception as e:
            logger.debug("Output streaming ended: %s", e)

    async def _wait_for_process(self, process: subprocess.Popen[str], job_id: str) -> int:
        """Wait for a container process, terminating on timeout."""
        loop = asyncio.get_running_loop()
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, process.wait),
                timeout=JOB_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.error("Job %s timed out after %ds, terminating", job_id[:JOB_ID_DISPLAY_LENGTH], JOB_TIMEOUT_SECONDS)
            process.terminate()
            return await loop.run_in_executor(None, process.wait)

    async def _handle_job_completion(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        exit_code: int,
        run_start_time: datetime,
        epoch_timestamps: list[tuple[int, float]],
    ) -> None:
        """Update job status after container exits."""
        job = await self._fetch_job(conn, job_id, "status")
        if not job:
            return

        if job["status"] == JobStatus.PREEMPTED:
            logger.info("Job %s was stopped (PREEMPTED), keeping status", job_id[:JOB_ID_DISPLAY_LENGTH])
            await self._update_job(conn, job_id, exit_code=exit_code)
            return

        if exit_code != 0:
            logger.error("Job %s failed with exit code %d", job_id[:JOB_ID_DISPLAY_LENGTH], exit_code)
            await self._update_job(conn, job_id, status=JobStatus.FAILED, exit_code=exit_code)
            return

        # exit_code == 0
        is_profiling = await self._check_and_report_profiling(conn, job_id, run_start_time, epoch_timestamps)
        if is_profiling:
            logger.info("Profiling run for job %s complete, backend will re-schedule", job_id[:JOB_ID_DISPLAY_LENGTH])
        else:
            logger.info("Job %s completed successfully", job_id[:JOB_ID_DISPLAY_LENGTH])
            await self._update_job(conn, job_id, status=JobStatus.SUCCEEDED, exit_code=exit_code)

    async def _fetch_job(self, conn: psycopg.AsyncConnection[Any], job_id: str, *columns: str) -> dict[str, Any] | None:
        """Fetch specific columns for a job."""
        cols = ", ".join(columns) if columns else "*"
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT {cols} FROM jobs WHERE id = %(id)s", {"id": job_id})  # noqa: S608
            return await cur.fetchone()

    async def _run_job(self, job_id: str) -> None:
        """Execute a job in a Docker container."""
        try:
            async with self._db() as conn:
                job = await self._fetch_job(
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
                    logger.info("Job %s status is %s, skipping", job_id[:JOB_ID_DISPLAY_LENGTH], job["status"])
                    return

                # Update status
                run_status = JobStatus.PROFILING if job["is_profiling_run"] else JobStatus.RUNNING
                run_start_time = datetime.now(UTC)
                await self._update_job(conn, job_id, status=run_status, progress=None)

                # Prepare directories
                ckpt_local, runs_local, ckpt_host, runs_host = self._resolve_paths(job_id)
                ckpt_local.mkdir(parents=True, exist_ok=True)
                runs_local.mkdir(parents=True, exist_ok=True)

                is_first_run = job["exit_code"] is None
                self._prepare_checkpoint_dir(
                    ckpt_local, is_profiling=job["is_profiling_run"], is_first_run=is_first_run
                )
                ckpt_host_mount = (ckpt_host / ".profiling") if job["is_profiling_run"] else ckpt_host

                container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
                await self._update_job(conn, job_id, container_name=container_name)

                env_vars = self._build_env_vars(job)
                logger.info("Job %s env: %s", job_id[:JOB_ID_DISPLAY_LENGTH], env_vars)
                docker_cmd = self.build_docker_cmd(
                    container_name,
                    str(ckpt_host_mount),
                    str(runs_host),
                    job["image"],
                    job["command"],
                    env_vars=env_vars,
                )

                logger.info("Starting job %s: %s", job_id[:JOB_ID_DISPLAY_LENGTH], " ".join(docker_cmd))

                # Run container
                process = subprocess.Popen(docker_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                self.running_jobs[job_id] = process

                # Stream output
                epoch_timestamps: list[tuple[int, float]] = []
                log_path = runs_local / OUTPUT_LOG_FILENAME
                stream_task = asyncio.create_task(
                    self._stream_output(
                        process, conn, job_id, log_path, epoch_timestamps, is_profiling=job["is_profiling_run"]
                    )
                )

                exit_code = await self._wait_for_process(process, job_id)
                await stream_task

                self.running_jobs.pop(job_id, None)
                await self._handle_job_completion(conn, job_id, exit_code, run_start_time, epoch_timestamps)

        except Exception as e:
            logger.error("Failed to run job %s: %s", job_id, e, exc_info=True)
            try:
                async with self._db() as conn:
                    await self._update_job(conn, job_id, status=JobStatus.FAILED)
            except Exception as db_error:
                logger.error("Failed to update job status: %s", db_error, exc_info=True)
        finally:
            self.running_jobs.pop(job_id, None)
            await self._publish_completed(job_id)

    # ------------------------------------------------------------------
    # Stop handling
    # ------------------------------------------------------------------

    async def _stop_job(self, job_id: str) -> None:
        """Stop a job — whether it's tracked in-process, running in Docker, or still queued."""
        try:
            await self._stop_job_inner(job_id)
        except Exception:
            logger.exception("Error stopping job %s", job_id[:JOB_ID_DISPLAY_LENGTH])

    async def _stop_job_inner(self, job_id: str) -> None:
        # Fast path: process tracked locally — mark PREEMPTED then kill.
        # Checkpoints are saved per-epoch so no graceful shutdown needed.
        process = self.running_jobs.get(job_id)
        if process:
            logger.info("Killing tracked job %s", job_id[:JOB_ID_DISPLAY_LENGTH])
            async with self._db() as conn:
                await self._update_job(conn, job_id, status=JobStatus.PREEMPTED)
            container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
            result = await self._kill_container(container_name)
            logger.debug("Docker kill rc=%s stderr=%s", result.returncode, result.stderr)
            return

        # Slow path: not tracked locally — look up container from DB
        async with self._db() as conn:
            job = await self._fetch_job(conn, job_id, "container_name", "status")
            if not job:
                logger.warning("Job %s not found", job_id[:JOB_ID_DISPLAY_LENGTH])
                return

            container_name = job["container_name"] or f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"

            if job["status"] == JobStatus.QUEUED:
                logger.info("Job %s is QUEUED, marking as PREEMPTED", job_id[:JOB_ID_DISPLAY_LENGTH])
                await self._update_job(conn, job_id, status=JobStatus.PREEMPTED)
                return

            if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
                logger.info("Job %s status is %s, no action needed", job_id[:JOB_ID_DISPLAY_LENGTH], job["status"])
                return

            # Container should be running — kill it
            result = await self._kill_container(container_name)
            if result.returncode == 0:
                logger.info("Container %s killed", container_name)
                await self._update_job(conn, job_id, status=JobStatus.PREEMPTED)
            else:
                logger.error("Failed to kill container %s: %s", container_name, result.stderr)

    # ------------------------------------------------------------------
    # Profiling
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_profiling_duration(
        epoch_timestamps: list[tuple[int, float]] | None,
        run_start_time: datetime,
    ) -> float:
        """Compute profiling duration, excluding the first interval as warmup.

        Falls back to wall-clock duration when fewer than 3 timestamps exist.
        """
        if epoch_timestamps and len(epoch_timestamps) >= 3:
            intervals = [epoch_timestamps[i + 1][1] - epoch_timestamps[i][1] for i in range(len(epoch_timestamps) - 1)]
            steady = intervals[1:]  # drop first interval (warmup)
            mean_epoch_time = sum(steady) / len(steady)
            total_epochs = epoch_timestamps[-1][0]
            logger.info(
                "Profiling: dropped warmup interval (%.4fs), steady mean=%.4fs/epoch",
                intervals[0],
                mean_epoch_time,
            )
            return mean_epoch_time * total_epochs
        return (datetime.now(UTC) - run_start_time).total_seconds()

    async def _check_and_report_profiling(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        run_start_time: datetime,
        epoch_timestamps: list[tuple[int, float]] | None = None,
    ) -> bool:
        """Check if this was a profiling run and report results via NATS.

        Returns True if a profiling result was published.
        """
        job = await self._fetch_job(conn, job_id, "is_profiling_run", "assigned_gpu_config", "assigned_node")
        if not job or not job["is_profiling_run"]:
            return False

        duration = self._compute_profiling_duration(epoch_timestamps, run_start_time)
        total_epochs = epoch_timestamps[-1][0] if epoch_timestamps else None
        payload = {
            "job_id": job_id,
            "gpu_config": job["assigned_gpu_config"],
            "node_id": job["assigned_node"],
            "duration_seconds": duration,
        }

        if self.js:
            await self.js.publish(
                NATS_SUBJECT_PROFILING_COMPLETE,
                json.dumps(payload).encode(),
            )
            logger.info(
                "Published profiling result for job %s: %s = %.1fs (epochs=%s, warmup_excluded=%s)",
                job_id[:JOB_ID_DISPLAY_LENGTH],
                job["assigned_gpu_config"],
                duration,
                total_epochs,
                bool(epoch_timestamps and len(epoch_timestamps) >= 3),
            )
        return True

    async def _publish_completed(self, job_id: str) -> None:
        """Publish jobs.completed so the API can schedule waiting jobs immediately."""
        if self.js:
            try:
                await self.js.publish(
                    NATS_SUBJECT_COMPLETED,
                    json.dumps({"job_id": job_id}).encode(),
                )
            except Exception:
                logger.warning("Failed to publish jobs.completed for %s", job_id[:JOB_ID_DISPLAY_LENGTH])


async def main() -> None:
    """Main entry point."""
    worker = JobWorker()
    await worker.start()


if __name__ == "__main__":
    import sys

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown complete")
        sys.exit(0)

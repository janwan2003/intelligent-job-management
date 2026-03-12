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
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import nats
import psycopg
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy

from constants import (
    CHECKPOINT_DIR,
    CHECKPOINT_MOUNT_PATH,
    CONSUMER_RESUME,
    CONSUMER_STOP,
    CONSUMER_SUBMITTED,
    CONTAINER_NAME_PREFIX,
    DEFAULT_NATS_URL,
    DEFAULT_PROFILING_STEPS,
    DOCKER_CMD_TIMEOUT_SECONDS,
    DOCKER_STOP_GRACE_SECONDS,
    JOB_ID_DISPLAY_LENGTH,
    NATS_STREAM_NAME,
    NATS_SUBJECT_PROFILING_COMPLETE,
    NATS_SUBJECT_RESUME_REQUESTED,
    NATS_SUBJECT_STOP_REQUESTED,
    NATS_SUBJECT_SUBMITTED,
    NATS_SUBJECTS_PATTERN,
    OUTPUT_LOG_FILENAME,
    JobStatus,
    RUNNABLE_STATUSES,
    RUNS_DIR,
    RUNS_MOUNT_PATH,
)

# Regex to parse progress from training output, e.g. "Step 50/10000"
PROGRESS_RE = re.compile(r"Step\s+(\d+)/(\d+)")

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
        self.host_project_root: str = os.path.normpath(
            os.getenv("HOST_PROJECT_ROOT", self.host_root)
        )

        self.nc: nats.aio.client.Client | None = None
        self.js: JetStreamContext | None = None
        self.job_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self.running: bool = True
        # Track all concurrently running jobs: job_id -> Popen process
        self.running_jobs: dict[str, subprocess.Popen[str]] = {}
        self._job_tasks: set[asyncio.Task[None]] = set()

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

    async def connect_db(self) -> psycopg.AsyncConnection[Any]:
        """Create a new database connection."""
        if self.database_url is None:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        return await psycopg.AsyncConnection.connect(self.database_url)

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

        # Keep running
        try:
            await runner_task
        except KeyboardInterrupt:
            logger.info("Shutting down")
            self.running = False
            # Wait for all running job tasks to finish
            if self._job_tasks:
                logger.info(
                    "Waiting for %d running job(s) to finish...", len(self._job_tasks)
                )
                await asyncio.gather(*self._job_tasks, return_exceptions=True)
            if self.nc:
                await self.nc.close()

    async def _ensure_streams(self) -> None:
        """Ensure NATS JetStream streams exist."""
        assert self.js is not None
        try:
            await self.js.add_stream(
                name=NATS_STREAM_NAME, subjects=[NATS_SUBJECTS_PATTERN]
            )
            logger.info("JetStream stream '%s' ready", NATS_STREAM_NAME)
        except Exception as e:
            # Stream already exists
            logger.debug("Stream setup: %s", e)

    async def _reconcile_job_states(self) -> None:
        """Reconcile job states with actual Docker container status."""
        logger.info("Reconciling job states")
        conn = None
        try:
            conn = await self.connect_db()

            # Get all containers with ijm- prefix
            result = subprocess.run(
                [
                    "docker",
                    "ps",
                    "-a",
                    "--filter",
                    f"name={CONTAINER_NAME_PREFIX}",
                    "--format",
                    "{{.Names}}",
                ],
                capture_output=True,
                text=True,
                timeout=DOCKER_CMD_TIMEOUT_SECONDS,
            )
            running_containers = (
                set(result.stdout.strip().split("\n"))
                if result.stdout.strip()
                else set()
            )

            # Get all jobs marked as RUNNING, PROFILING, or QUEUED
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id, container_name, status FROM jobs WHERE status IN (%s, %s, %s)",
                    (JobStatus.RUNNING, JobStatus.PROFILING, JobStatus.QUEUED),
                )
                jobs = await cur.fetchall()

            reconciled_count = 0
            for job_id, container_name, status in jobs:
                if status in (JobStatus.RUNNING, JobStatus.PROFILING):
                    # Check if container actually exists
                    expected_container = (
                        container_name
                        or f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
                    )

                    if expected_container not in running_containers:
                        # Container is missing, mark job as failed
                        logger.warning(
                            "Job %s marked RUNNING but container %s not found - marking as FAILED",
                            job_id[:JOB_ID_DISPLAY_LENGTH],
                            expected_container,
                        )
                        async with conn.cursor() as cur:
                            await cur.execute(
                                "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                                (JobStatus.FAILED, datetime.now(timezone.utc), job_id),
                            )
                            await conn.commit()
                        reconciled_count += 1

            if reconciled_count > 0:
                logger.info("Reconciled %d orphaned job(s)", reconciled_count)
            else:
                logger.info("All job states are consistent")

        except Exception as e:
            logger.error("Failed to reconcile job states: %s", e, exc_info=True)
        finally:
            if conn:
                await conn.close()

    async def _pickup_queued_jobs(self) -> None:
        """Enqueue any QUEUED jobs that were missed while the worker was down.

        NATS durable consumers with DeliverPolicy.NEW only receive messages
        published after the subscription.  If jobs were submitted while the
        worker was offline, the events are lost.  This scan ensures those
        jobs get picked up on startup.
        """
        conn = None
        try:
            conn = await self.connect_db()
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id FROM jobs WHERE status = %s ORDER BY created_at ASC",
                    (JobStatus.QUEUED,),
                )
                rows = await cur.fetchall()

            if rows:
                logger.info(
                    "Found %d QUEUED job(s) from before startup — enqueuing", len(rows)
                )
                for (job_id,) in rows:
                    await self.job_queue.put(("run", job_id))
            else:
                logger.info("No missed QUEUED jobs")
        except Exception as e:
            logger.error("Failed to pick up queued jobs: %s", e, exc_info=True)
        finally:
            if conn:
                await conn.close()

    async def _subscribe_to_events(self) -> None:
        """Subscribe to NATS job events."""
        assert self.js is not None

        # Subscribe to job submission events (only new messages)
        await self.js.subscribe(
            NATS_SUBJECT_SUBMITTED,
            cb=self._handle_job_submitted,
            durable=CONSUMER_SUBMITTED,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=DeliverPolicy.NEW,
            ),
        )

        # Subscribe to stop requests (only new messages)
        await self.js.subscribe(
            NATS_SUBJECT_STOP_REQUESTED,
            cb=self._handle_stop_requested,
            durable=CONSUMER_STOP,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=DeliverPolicy.NEW,
            ),
        )

        # Subscribe to resume requests (only new messages)
        await self.js.subscribe(
            NATS_SUBJECT_RESUME_REQUESTED,
            cb=self._handle_resume_requested,
            durable=CONSUMER_RESUME,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=DeliverPolicy.NEW,
            ),
        )

        logger.info("Subscribed to job events")

    async def _handle_job_submitted(self, msg: Any) -> None:
        """Handle jobs.submitted event."""
        try:
            data = json.loads(msg.data.decode())
            job_id = data["job_id"]
            logger.info("Job submitted: %s", job_id)
            await self.job_queue.put(("run", job_id))
            await msg.ack()
        except Exception as e:
            logger.error("Failed to handle job submission: %s", e, exc_info=True)

    async def _handle_stop_requested(self, msg: Any) -> None:
        """Handle jobs.stop_requested event."""
        try:
            logger.debug("Received stop message from NATS: %s", msg.data)
            data = json.loads(msg.data.decode())
            job_id = data["job_id"]
            logger.info("Stop requested for job: %s", job_id)

            process = self.running_jobs.get(job_id)
            if process:
                logger.info(
                    "Interrupting running job: %s", job_id[:JOB_ID_DISPLAY_LENGTH]
                )

                # Update status to PREEMPTED BEFORE stopping container
                conn = await self.connect_db()
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            (JobStatus.PREEMPTED, datetime.now(timezone.utc), job_id),
                        )
                        await conn.commit()
                    logger.info(
                        "Marked job %s as PREEMPTED", job_id[:JOB_ID_DISPLAY_LENGTH]
                    )
                finally:
                    await conn.close()

                # Now stop the container
                container_name = (
                    f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
                )
                result = subprocess.run(
                    [
                        "docker",
                        "stop",
                        "-t",
                        str(DOCKER_STOP_GRACE_SECONDS),
                        container_name,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=DOCKER_CMD_TIMEOUT_SECONDS,
                )
                logger.debug(
                    "Docker stop result - returncode: %s, stdout: %s, stderr: %s",
                    result.returncode,
                    result.stdout,
                    result.stderr,
                )
            else:
                logger.info(
                    "Job %s not tracked as running, handling normally",
                    job_id[:JOB_ID_DISPLAY_LENGTH],
                )
                await self._stop_job(job_id)

            logger.debug("Acknowledging stop message")
            await msg.ack()
            logger.debug("Stop message acknowledged")
        except Exception as e:
            logger.error("Failed to handle stop request: %s", e, exc_info=True)

    async def _handle_resume_requested(self, msg: Any) -> None:
        """Handle jobs.resume_requested event."""
        try:
            data = json.loads(msg.data.decode())
            job_id = data["job_id"]
            logger.info("Resume requested for job: %s", job_id)
            await self.job_queue.put(("run", job_id))
            await msg.ack()
        except Exception as e:
            logger.error("Failed to handle resume request: %s", e, exc_info=True)

    async def _job_runner(self) -> None:
        """Main job dispatch loop — launches jobs concurrently."""
        while self.running:
            try:
                # Get next job from queue
                action, job_id = await asyncio.wait_for(
                    self.job_queue.get(), timeout=1.0
                )

                if action == "run":
                    task = asyncio.create_task(self._run_job(job_id))
                    self._job_tasks.add(task)
                    task.add_done_callback(self._job_tasks.discard)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Job runner error: %s", e, exc_info=True)
                await asyncio.sleep(1)

    async def _update_progress(
        self, conn: psycopg.AsyncConnection[Any], job_id: str, progress: str
    ) -> None:
        """Update job progress in the database."""
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE jobs SET progress = %s, updated_at = %s WHERE id = %s",
                (progress, datetime.now(timezone.utc), job_id),
            )
            await conn.commit()

    async def _run_job(self, job_id: str) -> None:
        """Execute a job in a Docker container."""
        conn = None
        try:
            # Connect to database and fetch job details
            conn = await self.connect_db()
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT image, command, status, is_profiling_run, profiling_epochs_no "
                    "FROM jobs WHERE id = %s",
                    (job_id,),
                )
                result = await cur.fetchone()

                if not result:
                    logger.error("Job %s not found", job_id)
                    return

                (
                    image,
                    command,
                    current_status,
                    is_profiling_run,
                    profiling_epochs_no,
                ) = result

            # Check if job was cancelled while in the queue
            if current_status not in RUNNABLE_STATUSES:
                logger.info(
                    "Job %s status is %s, skipping execution",
                    job_id[:JOB_ID_DISPLAY_LENGTH],
                    current_status,
                )
                return

            # Update status: PROFILING for profiling runs, RUNNING for real execution
            run_status = JobStatus.PROFILING if is_profiling_run else JobStatus.RUNNING
            run_start_time = datetime.now(timezone.utc)
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                    (run_status, run_start_time, job_id),
                )
                await conn.commit()

            # Prepare directories (using container-internal path)
            ckpt_local = Path(self.host_root) / "data" / CHECKPOINT_DIR / job_id
            runs_local = Path(self.host_root) / "data" / RUNS_DIR / job_id
            ckpt_local.mkdir(parents=True, exist_ok=True)
            runs_local.mkdir(parents=True, exist_ok=True)

            # Profiling runs use a separate checkpoint directory so they don't
            # overwrite the real training checkpoint (which must be preserved
            # for resume).  Standard runs use the normal checkpoint dir.
            if is_profiling_run:
                ckpt_local_mount = ckpt_local / ".profiling"
                ckpt_local_mount.mkdir(parents=True, exist_ok=True)
                # Clear previous profiling checkpoint data
                for f in ckpt_local_mount.iterdir():
                    if f.is_file():
                        f.unlink(missing_ok=True)
                logger.info(
                    "Using isolated profiling checkpoint dir for job %s",
                    job_id[:JOB_ID_DISPLAY_LENGTH],
                )
            else:
                ckpt_local_mount = ckpt_local

            # Volume mount paths for docker run (resolved by Docker daemon on HOST)
            ckpt_host = Path(self.host_project_root) / "data" / CHECKPOINT_DIR / job_id
            runs_host = Path(self.host_project_root) / "data" / RUNS_DIR / job_id
            # Profiling gets its own isolated checkpoint mount path
            ckpt_host_mount = (
                (ckpt_host / ".profiling") if is_profiling_run else ckpt_host
            )

            container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"

            # Store container name in DB
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET container_name = %s WHERE id = %s",
                    (container_name, job_id),
                )
                await conn.commit()

            # For profiling runs, limit the training to profiling_epochs_no steps
            env_vars: dict[str, str] = {}
            if is_profiling_run:
                max_steps = profiling_epochs_no or DEFAULT_PROFILING_STEPS
                env_vars["MAX_STEPS"] = str(max_steps)

            # Build docker run command
            docker_cmd = self.build_docker_cmd(
                container_name,
                str(ckpt_host_mount),
                str(runs_host),
                image,
                command,
                env_vars=env_vars,
            )

            logger.info("Starting job %s: %s", job_id, " ".join(docker_cmd))

            # Run container
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )

            # Track the process for stop handling
            self.running_jobs[job_id] = process
            logger.debug(
                "Tracking process - job_id: %s, PID: %s",
                job_id[:JOB_ID_DISPLAY_LENGTH],
                process.pid,
            )

            # Log file for this job
            log_path = runs_local / OUTPUT_LOG_FILENAME

            # Stream output: write to log file, parse progress, log to console
            async def stream_output() -> None:
                loop = asyncio.get_event_loop()
                assert conn is not None
                try:
                    stdout = process.stdout
                    if stdout is None:
                        return
                    with open(log_path, "a") as log_file:
                        while True:
                            line = await loop.run_in_executor(None, stdout.readline)
                            if not line:
                                break
                            stripped = line.rstrip()
                            logger.info(
                                "[Job %s] %s", job_id[:JOB_ID_DISPLAY_LENGTH], stripped
                            )
                            log_file.write(line)
                            log_file.flush()
                            # Parse progress
                            match = PROGRESS_RE.search(stripped)
                            if match:
                                progress = f"{match.group(1)}/{match.group(2)}"
                                await self._update_progress(conn, job_id, progress)
                except Exception as e:
                    logger.debug("Output streaming ended: %s", e)

            # Start streaming task and wait for process completion
            stream_task = asyncio.create_task(stream_output())

            loop = asyncio.get_event_loop()
            exit_code = await loop.run_in_executor(None, process.wait)

            # Wait for streaming to finish reading remaining output
            await stream_task

            # Clear tracking
            self.running_jobs.pop(job_id, None)

            # Check current status first - don't override PREEMPTED
            async with conn.cursor() as cur:
                await cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
                current_status_row = await cur.fetchone()
                current_status = current_status_row[0] if current_status_row else None

            # Only update status if not already PREEMPTED
            if current_status == JobStatus.PREEMPTED:
                logger.info(
                    "Job %s was stopped (PREEMPTED), keeping that status", job_id
                )
                # Just update exit code
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE jobs SET exit_code = %s, updated_at = %s WHERE id = %s",
                        (exit_code, datetime.now(timezone.utc), job_id),
                    )
                    await conn.commit()
            else:
                # Update job status based on exit code
                if exit_code == 0:
                    # Check if this was a profiling run
                    is_profiling = await self._check_and_report_profiling(
                        conn, job_id, run_start_time
                    )
                    if is_profiling:
                        logger.info(
                            "Profiling run for job %s complete, backend will re-schedule",
                            job_id[:JOB_ID_DISPLAY_LENGTH],
                        )
                    else:
                        status = JobStatus.SUCCEEDED
                        logger.info("Job %s completed successfully", job_id)
                        async with conn.cursor() as cur:
                            await cur.execute(
                                "UPDATE jobs SET status = %s, exit_code = %s, updated_at = %s WHERE id = %s",
                                (status, exit_code, datetime.now(timezone.utc), job_id),
                            )
                            await conn.commit()
                else:
                    status = JobStatus.FAILED
                    logger.error("Job %s failed with exit code %d", job_id, exit_code)
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE jobs SET status = %s, exit_code = %s, updated_at = %s WHERE id = %s",
                            (status, exit_code, datetime.now(timezone.utc), job_id),
                        )
                        await conn.commit()

        except Exception as e:
            logger.error("Failed to run job %s: %s", job_id, e, exc_info=True)
            # Mark as failed
            if conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            (JobStatus.FAILED, datetime.now(timezone.utc), job_id),
                        )
                        await conn.commit()
                except Exception as db_error:
                    logger.error(
                        "Failed to update job status: %s", db_error, exc_info=True
                    )
        finally:
            # Clear tracking
            self.running_jobs.pop(job_id, None)
            if conn:
                await conn.close()

    async def _check_and_report_profiling(
        self, conn: psycopg.AsyncConnection[Any], job_id: str, run_start_time: datetime
    ) -> bool:
        """Check if this was a profiling run and report results via NATS.

        Returns ``True`` if a profiling result was published (backend handles
        re-scheduling).  Returns ``False`` for standard runs.
        """
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT is_profiling_run, assigned_gpu_config, assigned_node "
                "FROM jobs WHERE id = %s",
                (job_id,),
            )
            meta = await cur.fetchone()

        if not meta or not meta[0]:
            return False

        duration = (datetime.now(timezone.utc) - run_start_time).total_seconds()
        payload = {
            "job_id": job_id,
            "gpu_config": meta[1],
            "node_id": meta[2],
            "duration_seconds": duration,
        }

        if self.js:
            await self.js.publish(
                NATS_SUBJECT_PROFILING_COMPLETE,
                json.dumps(payload).encode(),
            )
            logger.info(
                "Published profiling result for job %s: %s = %.1fs",
                job_id[:JOB_ID_DISPLAY_LENGTH],
                meta[1],
                duration,
            )
        return True

    async def _stop_job(self, job_id: str) -> None:
        """Stop a running job."""
        conn = None
        try:
            # Get container name and current status
            conn = await self.connect_db()
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT container_name, status FROM jobs WHERE id = %s", (job_id,)
                )
                result = await cur.fetchone()

                if not result:
                    logger.warning("Job %s not found in database", job_id)
                    return

                container_name = (
                    result[0]
                    or f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
                )
                current_status = result[1]

            # Check if container actually exists
            check_result = subprocess.run(
                [
                    "docker",
                    "ps",
                    "-a",
                    "--filter",
                    f"name={container_name}",
                    "--format",
                    "{{.Names}}",
                ],
                capture_output=True,
                text=True,
                timeout=DOCKER_CMD_TIMEOUT_SECONDS,
            )

            container_exists = container_name in check_result.stdout

            if not container_exists:
                logger.warning("Container %s does not exist", container_name)

                if current_status == JobStatus.QUEUED:
                    # Job hasn't started yet — mark as PREEMPTED directly
                    logger.info(
                        "Job %s is QUEUED, marking as PREEMPTED",
                        job_id[:JOB_ID_DISPLAY_LENGTH],
                    )
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            (JobStatus.PREEMPTED, datetime.now(timezone.utc), job_id),
                        )
                        await conn.commit()
                elif current_status in (JobStatus.RUNNING, JobStatus.PROFILING):
                    # If job was marked as RUNNING/PROFILING, it's orphaned - mark as FAILED
                    logger.warning(
                        "Job %s was marked %s but container is missing - marking as FAILED",
                        job_id[:JOB_ID_DISPLAY_LENGTH],
                        current_status,
                    )
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            (JobStatus.FAILED, datetime.now(timezone.utc), job_id),
                        )
                        await conn.commit()
                else:
                    logger.info(
                        "Job %s status is %s, no action needed",
                        job_id[:JOB_ID_DISPLAY_LENGTH],
                        current_status,
                    )
                return

            # Container exists, stop it
            logger.info("Stopping container %s", container_name)
            result = subprocess.run(
                [
                    "docker",
                    "stop",
                    "-t",
                    str(DOCKER_STOP_GRACE_SECONDS),
                    container_name,
                ],
                capture_output=True,
                text=True,
                timeout=DOCKER_CMD_TIMEOUT_SECONDS,
            )

            if result.returncode == 0:
                logger.info("Container %s stopped successfully", container_name)
                # Update status
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                        (JobStatus.PREEMPTED, datetime.now(timezone.utc), job_id),
                    )
                    await conn.commit()
            else:
                logger.error("Failed to stop container: %s", result.stderr)

        except Exception as e:
            logger.error("Failed to stop job %s: %s", job_id, e, exc_info=True)
        finally:
            if conn:
                await conn.close()


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

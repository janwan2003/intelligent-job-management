#!/usr/bin/env python3
"""
Worker service for job execution.

Subscribes to NATS events and manages Docker container execution for jobs.
Implements FIFO scheduling with single-concurrency (one job at a time).
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import nats
import psycopg
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class JobWorker:
    """Worker that executes jobs in Docker containers."""

    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL")
        self.nats_url = os.getenv("NATS_URL", "nats://nats:4222")
        self.host_root = os.getenv("HOST_ROOT", "/host")

        self.nc = None
        self.js: JetStreamContext = None
        self.job_queue = asyncio.Queue()
        self.running = True
        self.current_job_id = None  # Track currently running job
        self.current_process = None  # Track current subprocess

    async def connect_db(self):
        """Create database connection pool."""
        return await psycopg.AsyncConnection.connect(self.database_url)

    async def start(self):
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
            await self.nc.close()

    async def _ensure_streams(self):
        """Ensure NATS JetStream streams exist."""
        try:
            await self.js.add_stream(name="JOBS", subjects=["jobs.>"])
            logger.info("JetStream stream 'JOBS' ready")
        except Exception as e:
            # Stream already exists
            logger.debug("Stream setup: %s", e)

    async def _reconcile_job_states(self):
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
                    "name=ijm-",
                    "--format",
                    "{{.Names}}",
                ],
                capture_output=True,
                text=True,
            )
            running_containers = (
                set(result.stdout.strip().split("\n"))
                if result.stdout.strip()
                else set()
            )

            # Get all jobs marked as RUNNING or QUEUED
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, container_name, status 
                    FROM jobs 
                    WHERE status IN ('RUNNING', 'QUEUED')
                    """
                )
                jobs = await cur.fetchall()

            reconciled_count = 0
            for job_id, container_name, status in jobs:
                if status == "RUNNING":
                    # Check if container actually exists
                    expected_container = container_name or f"ijm-{job_id[:8]}"

                    if expected_container not in running_containers:
                        # Container is missing, mark job as failed
                        logger.warning(
                            "Job %s marked RUNNING but container %s not found - marking as FAILED",
                            job_id[:8],
                            expected_container,
                        )
                        async with conn.cursor() as cur:
                            await cur.execute(
                                "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                                ("FAILED", datetime.now(timezone.utc), job_id),
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

    async def _subscribe_to_events(self):
        """Subscribe to NATS job events."""
        # Subscribe to job submission events (only new messages)
        await self.js.subscribe(
            "jobs.submitted",
            cb=self._handle_job_submitted,
            durable="worker-submitted",
            config=nats.js.api.ConsumerConfig(
                deliver_policy=DeliverPolicy.NEW,
            ),
        )

        # Subscribe to stop requests (only new messages)
        await self.js.subscribe(
            "jobs.stop_requested",
            cb=self._handle_stop_requested,
            durable="worker-stop",
            config=nats.js.api.ConsumerConfig(
                deliver_policy=DeliverPolicy.NEW,
            ),
        )

        # Subscribe to resume requests (only new messages)
        await self.js.subscribe(
            "jobs.resume_requested",
            cb=self._handle_resume_requested,
            durable="worker-resume",
            config=nats.js.api.ConsumerConfig(
                deliver_policy=DeliverPolicy.NEW,
            ),
        )

        logger.info("Subscribed to job events")

    async def _handle_job_submitted(self, msg):
        """Handle jobs.submitted event."""
        try:
            data = json.loads(msg.data.decode())
            job_id = data["job_id"]
            logger.info("Job submitted: %s", job_id)
            await self.job_queue.put(("run", job_id))
            await msg.ack()
        except Exception as e:
            logger.error("Failed to handle job submission: %s", e, exc_info=True)

    async def _handle_stop_requested(self, msg):
        """Handle jobs.stop_requested event."""
        try:
            logger.debug("Received stop message from NATS: %s", msg.data)
            data = json.loads(msg.data.decode())
            job_id = data["job_id"]
            logger.info("Stop requested for job: %s", job_id)
            logger.debug("Current job ID: %s, Current process: %s", self.current_job_id, self.current_process)

            # If this job is currently running, stop it immediately
            if self.current_job_id == job_id and self.current_process:
                logger.info("Interrupting currently running job: %s", job_id[:8])
                
                # Update status to PREEMPTED BEFORE stopping container
                conn = await self.connect_db()
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            ("PREEMPTED", datetime.now(timezone.utc), job_id),
                        )
                        await conn.commit()
                    logger.info("Marked job %s as PREEMPTED", job_id[:8])
                finally:
                    await conn.close()
                
                # Now stop the container
                container_name = f"ijm-{job_id[:8]}"
                result = subprocess.run(
                    ["docker", "stop", "-t", "30", container_name],
                    capture_output=True,
                    text=True,
                )
                logger.debug(
                    "Docker stop result - returncode: %s, stdout: %s, stderr: %s",
                    result.returncode,
                    result.stdout,
                    result.stderr,
                )
            else:
                logger.info(
                    "Job %s not currently running, handling normally",
                    job_id[:8],
                )
                # Job is not running, handle normally
                await self._stop_job(job_id)

            logger.debug("Acknowledging stop message")
            await msg.ack()
            logger.debug("Stop message acknowledged")
        except Exception as e:
            logger.error("Failed to handle stop request: %s", e, exc_info=True)

    async def _handle_resume_requested(self, msg):
        """Handle jobs.resume_requested event."""
        try:
            data = json.loads(msg.data.decode())
            job_id = data["job_id"]
            logger.info("Resume requested for job: %s", job_id)
            await self.job_queue.put(("run", job_id))
            await msg.ack()
        except Exception as e:
            logger.error("Failed to handle resume request: %s", e, exc_info=True)

    async def _job_runner(self):
        """Main job execution loop (FIFO, single concurrency)."""
        while self.running:
            try:
                # Get next job from queue
                action, job_id = await asyncio.wait_for(
                    self.job_queue.get(), timeout=1.0
                )

                if action == "run":
                    await self._run_job(job_id)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Job runner error: %s", e, exc_info=True)
                await asyncio.sleep(1)

    async def _run_job(self, job_id: str):
        """Execute a job in a Docker container."""
        conn = None
        try:
            # Set this as the current job
            self.current_job_id = job_id

            # Connect to database and fetch job details
            conn = await self.connect_db()
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT image, command FROM jobs WHERE id = %s", (job_id,)
                )
                result = await cur.fetchone()

                if not result:
                    logger.error("Job %s not found", job_id)
                    return

                image, command = result

            # Update status to RUNNING
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                    ("RUNNING", datetime.now(timezone.utc), job_id),
                )
                await conn.commit()

            # Prepare directories
            ckpt_host = Path(self.host_root) / "data" / "checkpoints" / job_id
            runs_host = Path(self.host_root) / "data" / "runs" / job_id
            ckpt_host.mkdir(parents=True, exist_ok=True)
            runs_host.mkdir(parents=True, exist_ok=True)

            container_name = f"ijm-{job_id[:8]}"

            # Store container name in DB
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET container_name = %s WHERE id = %s",
                    (container_name, job_id),
                )
                await conn.commit()

            # Build docker run command
            docker_cmd = [
                "docker",
                "run",
                "--rm",
                "--name",
                container_name,
                "-v",
                f"{ckpt_host}:/checkpoints",
                "-v",
                f"{runs_host}:/runs",
                image,
            ] + command

            logger.info("Starting job %s: %s", job_id, ' '.join(docker_cmd))
        
            # Run container
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )

            # Track the process
            self.current_process = process
            logger.debug("Tracking process - job_id: %s, PID: %s", job_id[:8], process.pid)

            # Stream output asynchronously to not block event loop
            async def stream_output():
                loop = asyncio.get_event_loop()
                while True:
                    # Read line in executor to not block
                    line = await loop.run_in_executor(None, process.stdout.readline)
                    if not line:
                        break
                    logger.info("[Job %s] %s", job_id[:8], line.rstrip())

            # Start streaming task
            asyncio.create_task(stream_output())

            # Wait for process to complete (also async)
            loop = asyncio.get_event_loop()
            exit_code = await loop.run_in_executor(None, process.wait)

            # Clear tracking
            self.current_process = None
            self.current_job_id = None

            # Check current status first - don't override PREEMPTED
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT status FROM jobs WHERE id = %s", (job_id,)
                )
                current_status_row = await cur.fetchone()
                current_status = current_status_row[0] if current_status_row else None

            # Only update status if not already PREEMPTED
            if current_status == "PREEMPTED":
                logger.info("Job %s was stopped (PREEMPTED), keeping that status", job_id)
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
                    status = "SUCCEEDED"
                    logger.info("Job %s completed successfully", job_id)
                else:
                    status = "FAILED"
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
                            ("FAILED", datetime.now(timezone.utc), job_id),
                        )
                        await conn.commit()
                except Exception as db_error:
                    logger.error("Failed to update job status: %s", db_error, exc_info=True)
        finally:
            # Clear tracking
            self.current_process = None
            self.current_job_id = None
            if conn:
                await conn.close()

    async def _stop_job(self, job_id: str):
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

                container_name = result[0] or f"ijm-{job_id[:8]}"
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
            )

            container_exists = container_name in check_result.stdout

            if not container_exists:
                logger.warning("Container %s does not exist", container_name)

                # If job was marked as RUNNING, it's orphaned - mark as FAILED
                if current_status == "RUNNING":
                    logger.warning(
                        "Job %s was marked RUNNING but container is missing - marking as FAILED",
                        job_id[:8],
                    )
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            ("FAILED", datetime.now(timezone.utc), job_id),
                        )
                        await conn.commit()
                else:
                    logger.info(
                        "Job %s status is %s, no action needed",
                        job_id[:8],
                        current_status,
                    )
                return

            # Container exists, stop it
            logger.info("Stopping container %s", container_name)
            result = subprocess.run(
                ["docker", "stop", "-t", "30", container_name],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                logger.info("Container %s stopped successfully", container_name)
                # Update status
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                        ("PREEMPTED", datetime.now(timezone.utc), job_id),
                    )
                    await conn.commit()
            else:
                logger.error("Failed to stop container: %s", result.stderr)

        except Exception as e:
            logger.error("Failed to stop job %s: %s", job_id, e, exc_info=True)
        finally:
            if conn:
                await conn.close()


async def main():
    """Main entry point."""
    worker = JobWorker()
    await worker.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown complete")
        sys.exit(0)

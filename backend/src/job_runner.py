"""Job execution engine — runs training containers and manages their lifecycle.

Merged from the standalone worker service. Uses an Executor interface to
decouple container management (Docker, SLURM, mock) from orchestration logic.
"""

import asyncio
import logging
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json
from shared.constants import DEFAULT_PROFILING_EPOCHS, OUTPUT_LOG_FILENAME, RUNS_DIR, JobStatus

from src.executors import Executor, JobHandle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONTAINER_NAME_PREFIX = "ijm-"
JOB_ID_DISPLAY_LENGTH = 8
CHECKPOINT_DIR = "checkpoints"
RUNNABLE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.RUNNING})

# Regex to parse progress from training output, e.g. "Epoch 3/20"
PROGRESS_RE = re.compile(r"Epoch\s+(\d+)/(\d+)")

# Maximum wall-clock time for a single job (default: 24 hours)
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", str(24 * 3600)))


# ---------------------------------------------------------------------------
# JobRunner
# ---------------------------------------------------------------------------


class JobRunner:
    """Manages concurrent job execution using an Executor backend.

    This is the merged equivalent of the old standalone worker service.
    It runs as a set of asyncio tasks inside the API process.
    """

    def __init__(
        self,
        executor: Executor,
        get_conn: Any,  # callable returning async context manager for DB connections
        schedule_lock: asyncio.Lock,
        on_profiling_complete: Any = None,  # callback(conn, job_id, gpu_config, node_id, duration)
        on_job_completed: Any = None,  # callback()
    ) -> None:
        self.executor = executor
        self.get_conn = get_conn
        self.schedule_lock = schedule_lock
        self.on_profiling_complete = on_profiling_complete
        self.on_job_completed = on_job_completed

        self.host_root: str = os.getenv("HOST_ROOT", "/host")
        self.host_project_root: str = os.path.normpath(os.getenv("HOST_PROJECT_ROOT", self.host_root))

        self.job_queue: asyncio.Queue[str] = asyncio.Queue()
        self.running: bool = True
        self.running_jobs: dict[str, JobHandle] = {}
        self._job_tasks: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------------
    # Public interface (called by routers)
    # ------------------------------------------------------------------

    async def enqueue(self, job_id: str) -> None:
        """Enqueue a job for execution."""
        await self.job_queue.put(job_id)

    async def stop(self, job_id: str) -> None:
        """Stop a running job."""
        try:
            await self._stop_job(job_id)
        except Exception:
            logger.exception("Error stopping job %s", job_id[:JOB_ID_DISPLAY_LENGTH])

    # ------------------------------------------------------------------
    # Lifecycle (started by app lifespan)
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the job runner background tasks."""
        await self._reconcile_job_states()
        await self._pickup_queued_jobs()
        self._runner_task = asyncio.create_task(self._dispatch_loop())
        logger.info("Job runner started")

    async def shutdown(self) -> None:
        """Graceful shutdown — wait for running jobs."""
        self.running = False
        self._runner_task.cancel()
        if self._job_tasks:
            logger.info("Waiting for %d running job(s) to finish...", len(self._job_tasks))
            await asyncio.gather(*self._job_tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    async def _update_job(self, conn: psycopg.AsyncConnection[Any], job_id: str, **fields: Any) -> None:
        fields["updated_at"] = datetime.now(UTC)
        sets = ", ".join(f"{k} = %({k})s" for k in fields)
        fields["_id"] = job_id
        async with conn.cursor() as cur:
            await cur.execute(f"UPDATE jobs SET {sets} WHERE id = %(_id)s", fields)  # noqa: S608
            await conn.commit()

    async def _fetch_job(self, conn: psycopg.AsyncConnection[Any], job_id: str, *columns: str) -> dict[str, Any] | None:
        cols = ", ".join(columns) if columns else "*"
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"SELECT {cols} FROM jobs WHERE id = %(id)s", {"id": job_id})  # noqa: S608
            return await cur.fetchone()

    # ------------------------------------------------------------------
    # Startup recovery
    # ------------------------------------------------------------------

    async def _reconcile_job_states(self) -> None:
        """Mark RUNNING/PROFILING jobs as FAILED if their container is gone."""
        logger.info("Reconciling job states")
        try:
            running_containers = await self.executor.list_running(CONTAINER_NAME_PREFIX)

            async with self.get_conn() as conn:
                cur = conn.cursor(row_factory=dict_row)
                await cur.execute(
                    "SELECT id, container_name, status FROM jobs WHERE status IN (%s, %s, %s)",
                    (JobStatus.RUNNING, JobStatus.PROFILING, JobStatus.QUEUED),
                )
                jobs = await cur.fetchall()

                reconciled = 0
                for job in jobs:
                    if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
                        continue
                    expected = job["container_name"] or f"{CONTAINER_NAME_PREFIX}{job['id'][:JOB_ID_DISPLAY_LENGTH]}"
                    if expected not in running_containers:
                        logger.warning(
                            "Job %s marked %s but container missing — marking FAILED",
                            job["id"][:JOB_ID_DISPLAY_LENGTH],
                            job["status"],
                        )
                        await self._update_job(conn, job["id"], status=JobStatus.FAILED)
                        reconciled += 1

            logger.info("Reconciled %d orphaned job(s)", reconciled) if reconciled else logger.info(
                "All job states consistent"
            )
        except Exception as e:
            logger.error("Failed to reconcile: %s", e, exc_info=True)

    async def _pickup_queued_jobs(self) -> None:
        """Enqueue QUEUED jobs that were missed (e.g. after restart)."""
        try:
            async with self.get_conn() as conn:
                cur = await conn.execute(
                    "SELECT id FROM jobs WHERE status = %s ORDER BY created_at ASC",
                    (JobStatus.QUEUED,),
                )
                rows = await cur.fetchall()

            if rows:
                logger.info("Found %d QUEUED job(s) — enqueuing", len(rows))
                for row in rows:
                    await self.job_queue.put(row[0])
        except Exception as e:
            logger.error("Failed to pick up queued jobs: %s", e, exc_info=True)

    # ------------------------------------------------------------------
    # Dispatch loop
    # ------------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        """Main loop — pull job IDs from queue and launch them concurrently."""
        while self.running:
            try:
                job_id = await asyncio.wait_for(self.job_queue.get(), timeout=1.0)
                if job_id in self.running_jobs:
                    logger.info("Job %s already running, skipping", job_id[:JOB_ID_DISPLAY_LENGTH])
                    continue
                task = asyncio.create_task(self._run_job(job_id))
                self._job_tasks.add(task)
                task.add_done_callback(self._job_tasks.discard)
            except TimeoutError:
                continue
            except Exception as e:
                logger.error("Dispatch error: %s", e, exc_info=True)
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _resolve_paths(self, job_id: str) -> tuple[Path, Path, Path, Path]:
        """Return (ckpt_local, runs_local, ckpt_host, runs_host)."""
        ckpt_local = Path(self.host_root) / "data" / CHECKPOINT_DIR / job_id
        runs_local = Path(self.host_root) / "data" / RUNS_DIR / job_id
        ckpt_host = Path(self.host_project_root) / "data" / CHECKPOINT_DIR / job_id
        runs_host = Path(self.host_project_root) / "data" / RUNS_DIR / job_id
        return ckpt_local, runs_local, ckpt_host, runs_host

    @staticmethod
    def _prepare_checkpoint_dir(ckpt_local: Path, *, is_profiling: bool, is_first_run: bool) -> Path:
        """Set up checkpoint directory. Profiling uses isolated .profiling/ subdir."""
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

    async def _stream_and_parse(
        self,
        handle: JobHandle,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        log_path: Path,
        epoch_timestamps: list[tuple[int, float]],
        *,
        is_profiling: bool,
    ) -> None:
        """Stream container output, parse progress, collect profiling timestamps."""
        try:
            with open(log_path, "a") as log_file:
                async for line in self.executor.stream_logs(handle):
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

    async def _run_job(self, job_id: str) -> None:
        """Execute a single job."""
        from src.executors.docker import CHECKPOINT_MOUNT_PATH, RUNS_MOUNT_PATH

        try:
            async with self.get_conn() as conn:
                job = await self._fetch_job(
                    conn,
                    job_id,
                    "job_id",
                    "image",
                    "command",
                    "status",
                    "is_profiling_run",
                    "profiling_epochs_no",
                    "exit_code",
                    "epochs_total",
                    "batch_size",
                    "assigned_node",
                    "directory_to_mount",
                )
                if not job:
                    logger.error("Job %s not found", job_id)
                    return
                if job["status"] not in RUNNABLE_STATUSES:
                    logger.info("Job %s status is %s, skipping", job_id[:JOB_ID_DISPLAY_LENGTH], job["status"])
                    return

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

                # Shared data (datasets) mounted read-only under /runs/data
                shared_data_local = Path(self.host_root) / "data" / "shared" / "data"
                shared_data_host = Path(self.host_project_root) / "data" / "shared" / "data"
                volumes = {
                    str(ckpt_host_mount): CHECKPOINT_MOUNT_PATH,
                    str(runs_host): RUNS_MOUNT_PATH,
                }
                if shared_data_local.exists():
                    volumes[str(shared_data_host)] = "/runs/data"
                if job.get("directory_to_mount"):
                    volumes[job["directory_to_mount"]] = "/workspace"
                handle = await self.executor.run(
                    container_name,
                    job["image"],
                    job["command"],
                    volumes,
                    env_vars,
                )
                self.running_jobs[job_id] = handle

                # Stream output and wait
                epoch_timestamps: list[tuple[int, float]] = []
                log_path = runs_local / OUTPUT_LOG_FILENAME
                stream_task = asyncio.create_task(
                    self._stream_and_parse(
                        handle, conn, job_id, log_path, epoch_timestamps, is_profiling=job["is_profiling_run"]
                    )
                )

                exit_code = await self.executor.wait(handle, JOB_TIMEOUT_SECONDS)
                await stream_task

                self.running_jobs.pop(job_id, None)
                await self._handle_completion(conn, job_id, exit_code, run_start_time, epoch_timestamps)

        except Exception as e:
            logger.error("Failed to run job %s: %s", job_id, e, exc_info=True)
            try:
                async with self.get_conn() as conn:
                    await self._update_job(conn, job_id, status=JobStatus.FAILED)
            except Exception as db_err:
                logger.error("Failed to update job status: %s", db_err, exc_info=True)
        finally:
            self.running_jobs.pop(job_id, None)
            if self.on_job_completed:
                try:
                    await self.on_job_completed()
                except Exception:
                    logger.warning("on_job_completed callback failed for %s", job_id[:JOB_ID_DISPLAY_LENGTH])

    # ------------------------------------------------------------------
    # Completion handling
    # ------------------------------------------------------------------

    async def _handle_completion(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        exit_code: int,
        run_start_time: datetime,
        epoch_timestamps: list[tuple[int, float]],
    ) -> None:
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
            # Release in-flight profiling claim so the config can be retried
            if job.get("is_profiling_run") and job.get("assigned_gpu_config"):
                await conn.execute(
                    "DELETE FROM profiling_results WHERE job_id = %s AND gpu_config = %s::jsonb AND duration_seconds IS NULL",
                    (job.get("job_id", job_id), Json(job["assigned_gpu_config"])),
                )
            return

        # Success — check if profiling
        is_profiling = await self._report_profiling(conn, job_id, run_start_time, epoch_timestamps)
        if is_profiling:
            logger.info("Profiling run for job %s complete", job_id[:JOB_ID_DISPLAY_LENGTH])
        else:
            logger.info("Job %s completed successfully", job_id[:JOB_ID_DISPLAY_LENGTH])
            await self._update_job(conn, job_id, status=JobStatus.SUCCEEDED, exit_code=exit_code)

    # ------------------------------------------------------------------
    # Profiling
    # ------------------------------------------------------------------

    @staticmethod
    def compute_profiling_duration(
        epoch_timestamps: list[tuple[int, float]] | None,
        run_start_time: datetime,
    ) -> float:
        """Compute profiling duration, excluding first interval as warmup."""
        if epoch_timestamps and len(epoch_timestamps) >= 3:
            intervals = [epoch_timestamps[i + 1][1] - epoch_timestamps[i][1] for i in range(len(epoch_timestamps) - 1)]
            steady = intervals[1:]
            mean_epoch_time = sum(steady) / len(steady)
            total_epochs = epoch_timestamps[-1][0]
            logger.info("Profiling: dropped warmup (%.4fs), steady mean=%.4fs/epoch", intervals[0], mean_epoch_time)
            return mean_epoch_time * total_epochs
        return (datetime.now(UTC) - run_start_time).total_seconds()

    async def _report_profiling(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        run_start_time: datetime,
        epoch_timestamps: list[tuple[int, float]] | None = None,
    ) -> bool:
        """If this was a profiling run, report results via callback. Returns True if profiling."""
        job = await self._fetch_job(conn, job_id, "job_id", "is_profiling_run", "assigned_gpu_config", "assigned_node")
        if not job or not job["is_profiling_run"]:
            return False

        duration = self.compute_profiling_duration(epoch_timestamps, run_start_time)
        total_epochs = epoch_timestamps[-1][0] if epoch_timestamps else None
        logger.info(
            "Profiling result for job %s: %s = %.1fs (epochs=%s, warmup_excluded=%s)",
            job_id[:JOB_ID_DISPLAY_LENGTH],
            job["assigned_gpu_config"],
            duration,
            total_epochs,
            bool(epoch_timestamps and len(epoch_timestamps) >= 3),
        )

        if self.on_profiling_complete:
            await self.on_profiling_complete(
                conn,
                job_id,
                job["assigned_gpu_config"],
                job["assigned_node"],
                duration,
                job_type_id=job.get("job_id"),
            )
        return True

    # ------------------------------------------------------------------
    # Stop handling
    # ------------------------------------------------------------------

    async def _stop_job(self, job_id: str) -> None:
        # Fast path: tracked locally — kill immediately
        handle = self.running_jobs.get(job_id)
        if handle:
            logger.info("Killing tracked job %s", job_id[:JOB_ID_DISPLAY_LENGTH])
            async with self.get_conn() as conn:
                await self._update_job(conn, job_id, status=JobStatus.PREEMPTED)
            await self.executor.kill(handle)
            return

        # Slow path: look up from DB
        async with self.get_conn() as conn:
            job = await self._fetch_job(conn, job_id, "container_name", "status")
            if not job:
                logger.warning("Job %s not found", job_id[:JOB_ID_DISPLAY_LENGTH])
                return

            if job["status"] == JobStatus.QUEUED:
                logger.info("Job %s is QUEUED, marking PREEMPTED", job_id[:JOB_ID_DISPLAY_LENGTH])
                await self._update_job(conn, job_id, status=JobStatus.PREEMPTED)
                return

            if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
                logger.info("Job %s status is %s, no action needed", job_id[:JOB_ID_DISPLAY_LENGTH], job["status"])
                return

            container_name = job["container_name"] or f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
            killed = await self.executor.kill(JobHandle(container_name=container_name))
            if killed:
                logger.info("Container %s killed", container_name)
                await self._update_job(conn, job_id, status=JobStatus.PREEMPTED)
            else:
                logger.error("Failed to kill container %s", container_name)

"""Job execution engine — runs training containers and manages their lifecycle.

Uses an Executor interface to decouple container management
(Docker, SLURM, mock) from orchestration logic.
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
from src.executors.docker import CHECKPOINT_MOUNT_PATH, RUNS_MOUNT_PATH

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
        on_profiling_complete: Any = None,  # callback(job_id, gpu_config, node_id, duration, job_type_id)
        on_job_completed: Any = None,  # callback()
    ) -> None:
        self.executor = executor
        self.get_conn = get_conn
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

    async def stop(self, job_id: str, *, reason: str = "user") -> None:
        """Stop a running job.

        ``reason="user"``: status → PREEMPTED, awaits manual resume.
        ``reason="auto"``: status → QUEUED with assignment cleared, picked up
        again by the next scheduler pass.
        """
        try:
            await self._stop_job(job_id, reason=reason)
        except Exception:
            logger.exception("Error stopping job %s", job_id[:JOB_ID_DISPLAY_LENGTH])

    # ------------------------------------------------------------------
    # Lifecycle (started by app lifespan)
    # ------------------------------------------------------------------

    async def start(self, local_node_ids: frozenset[str] | None = None) -> None:
        """Start the job runner background tasks.

        *local_node_ids* — only reconcile/pick-up jobs assigned to these nodes
        (or unassigned). Used by ``JobDispatcher`` so remote-worker jobs are
        owned by their respective workers.
        """
        await self.reconcile_job_states(local_node_ids)
        await self.pickup_queued_jobs(local_node_ids)
        self.start_dispatch_loop()

    def start_dispatch_loop(self) -> None:
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

    async def reconcile_job_states(self, local_node_ids: frozenset[str] | None = None) -> None:
        """Mark RUNNING/PROFILING jobs as FAILED if their container is gone.

        When *local_node_ids* is provided only jobs assigned to those nodes
        (or unassigned) are checked — remote-worker jobs are left alone.
        """
        logger.info("Reconciling job states")
        try:
            running_containers = await self.executor.list_running(CONTAINER_NAME_PREFIX)

            async with self.get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
                if local_node_ids is not None:
                    await cur.execute(
                        "SELECT id, container_name, status FROM jobs WHERE status IN (%s, %s)"
                        " AND (assigned_node IS NULL OR assigned_node = ANY(%s))",
                        (JobStatus.RUNNING, JobStatus.PROFILING, list(local_node_ids)),
                    )
                else:
                    await cur.execute(
                        "SELECT id, container_name, status FROM jobs WHERE status IN (%s, %s)",
                        (JobStatus.RUNNING, JobStatus.PROFILING),
                    )
                jobs = await cur.fetchall()

                orphaned: list[str] = []
                for job in jobs:
                    expected = job["container_name"] or f"{CONTAINER_NAME_PREFIX}{job['id'][:JOB_ID_DISPLAY_LENGTH]}"
                    if expected not in running_containers:
                        logger.warning(
                            "Job %s marked %s but container missing — marking FAILED",
                            job["id"][:JOB_ID_DISPLAY_LENGTH],
                            job["status"],
                        )
                        orphaned.append(job["id"])

                if orphaned:
                    await cur.execute(
                        "UPDATE jobs SET status = %s, updated_at = %s WHERE id = ANY(%s)",
                        (JobStatus.FAILED, datetime.now(UTC), orphaned),
                    )
                    await conn.commit()

            if orphaned:
                logger.info("Reconciled %d orphaned job(s)", len(orphaned))
            else:
                logger.info("All job states consistent")
        except Exception:
            logger.exception("Failed to reconcile job states")

    async def pickup_queued_jobs(self, local_node_ids: frozenset[str] | None = None) -> None:
        """Enqueue QUEUED jobs that were missed (e.g. after restart).

        When *local_node_ids* is provided only jobs assigned to those nodes
        (or unassigned) are picked up — remote-worker jobs are left alone.
        """
        try:
            async with self.get_conn() as conn:
                if local_node_ids is not None:
                    cur = await conn.execute(
                        "SELECT id FROM jobs WHERE status = %s"
                        " AND (assigned_node IS NULL OR assigned_node = ANY(%s))"
                        " ORDER BY created_at ASC",
                        (JobStatus.QUEUED, list(local_node_ids)),
                    )
                else:
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
    def _prepare_checkpoint_dir(ckpt_local: Path, *, is_profiling: bool) -> None:
        """Set up checkpoint directory. Profiling uses isolated ``.profiling/``
        subdir which is always wiped (fresh short run, shouldn't inherit
        state).  For real runs we never delete files: the trainer overwrites
        ``latest.pt`` atomically and a missing file means "start fresh".
        Wiping on a perceived first-run lost live checkpoints when two
        dispatches raced (the second saw exit_code=None during the brief
        window before Phase 4 wrote it, classified itself first-run, and
        nuked the dir mid-resume)."""
        target = (ckpt_local / ".profiling") if is_profiling else ckpt_local
        target.mkdir(parents=True, exist_ok=True)
        if is_profiling:
            for f in target.iterdir():
                if f.is_file():
                    f.unlink(missing_ok=True)

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
                        async with self.get_conn() as conn:
                            await self._update_job(conn, job_id, progress=progress)
                        if is_profiling:
                            epoch_timestamps.append((epoch_num, time.monotonic()))
        except Exception:
            # An error here means we lose the rest of the container output and
            # any further epoch timestamps — surface it instead of swallowing.
            logger.warning(
                "Output streaming for job %s ended unexpectedly", job_id[:JOB_ID_DISPLAY_LENGTH], exc_info=True
            )

    async def _run_job(self, job_id: str) -> None:
        """Execute a single job."""
        try:
            # Phase 1: fetch job + atomically claim it (short-lived connection).
            # We use a conditional UPDATE so concurrent stop()/dispatch can't
            # race us into clobbering a PREEMPTED status with RUNNING.
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

                run_status = JobStatus.PROFILING if job["is_profiling_run"] else JobStatus.RUNNING
                run_start_time = datetime.now(UTC)
                container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"

                cur = await conn.execute(
                    "UPDATE jobs SET status = %s, container_name = %s, updated_at = %s "
                    "WHERE id = %s AND status = ANY(%s) RETURNING id",
                    (run_status, container_name, run_start_time, job_id, list(RUNNABLE_STATUSES)),
                )
                claimed = await cur.fetchone()
                await conn.commit()
                if not claimed:
                    logger.info(
                        "Job %s no longer in a runnable status (was %s), skipping",
                        job_id[:JOB_ID_DISPLAY_LENGTH],
                        job["status"],
                    )
                    return

            # Phase 2: prepare dirs and launch container (no DB connection held)
            ckpt_local, runs_local, ckpt_host, runs_host = self._resolve_paths(job_id)
            ckpt_local.mkdir(parents=True, exist_ok=True)
            runs_local.mkdir(parents=True, exist_ok=True)

            self._prepare_checkpoint_dir(ckpt_local, is_profiling=job["is_profiling_run"])
            ckpt_host_mount = (ckpt_host / ".profiling") if job["is_profiling_run"] else ckpt_host

            env_vars = self._build_env_vars(job)
            logger.info("Job %s env: %s", job_id[:JOB_ID_DISPLAY_LENGTH], env_vars)

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

            # Phase 3: stream output and wait (no DB connection held;
            # _stream_and_parse acquires its own short-lived connections)
            epoch_timestamps: list[tuple[int, float]] = []
            log_path = runs_local / OUTPUT_LOG_FILENAME
            stream_task = asyncio.create_task(
                self._stream_and_parse(handle, job_id, log_path, epoch_timestamps, is_profiling=job["is_profiling_run"])
            )

            exit_code = await self.executor.wait(handle, JOB_TIMEOUT_SECONDS)
            await stream_task

            self.running_jobs.pop(job_id, None)

            # Phase 4: handle completion (short-lived connection)
            async with self.get_conn() as conn:
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
                    # Pass job_id so the callback can release the slot
                    # semaphore for this job's specific (node, gpu_count).
                    # Older callbacks took no args; the wired-up callback in
                    # app.py now takes job_id.
                    await self.on_job_completed(job_id)
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
        # Fetch the columns we may need for FAILED branch up-front so we can
        # release the profiling claim atomically with the FAILED update below.
        job = await self._fetch_job(conn, job_id, "status", "job_id", "is_profiling_run", "assigned_gpu_config")
        if not job:
            return

        if job["status"] == JobStatus.PREEMPTED:
            logger.info("Job %s was stopped (PREEMPTED), keeping status", job_id[:JOB_ID_DISPLAY_LENGTH])
            await self._update_job(conn, job_id, exit_code=exit_code)
            return

        if exit_code != 0:
            logger.error("Job %s failed with exit code %d", job_id[:JOB_ID_DISPLAY_LENGTH], exit_code)
            # Mark FAILED + release any in-flight profiling claim atomically.
            # Doing both in one transaction guarantees the DELETE actually
            # commits — previously _update_job committed, then the DELETE ran
            # in a fresh implicit transaction that the pool rolled back on
            # connection return, permanently locking the config.
            now = datetime.now(UTC)
            async with conn.transaction():
                await conn.execute(
                    "UPDATE jobs SET status = %s, exit_code = %s, updated_at = %s WHERE id = %s",
                    (JobStatus.FAILED, exit_code, now, job_id),
                )
                if job.get("is_profiling_run") and job.get("assigned_gpu_config"):
                    cur = await conn.execute(
                        "DELETE FROM profiling_results "
                        "WHERE job_id = %s AND gpu_config = %s::jsonb "
                        "AND instance_id = %s AND duration_seconds IS NULL",
                        (job["job_id"], Json(job["assigned_gpu_config"]), job_id),
                    )
                    if cur.rowcount == 0:
                        logger.warning(
                            "Profiling claim for failed job %s (%s) not found — possibly already released",
                            job_id[:JOB_ID_DISPLAY_LENGTH],
                            job["assigned_gpu_config"],
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

    # Need ≥ 2 timestamps: one ending the warmup epoch and one ending a
    # steady-state epoch, giving 1 post-warmup interval to average.
    MIN_EPOCH_TIMESTAMPS_FOR_STEADY = 2

    @staticmethod
    def compute_profiling_duration(
        epoch_timestamps: list[tuple[int, float]] | None,
        run_start_time: datetime,
    ) -> float:
        """Compute mean per-epoch duration, excluding the first (warmup) epoch.

        ``epoch_timestamps[0]`` marks the end of epoch 1 — its own duration
        can't be measured (no monotonic start sample), so all intervals
        between timestamps are already post-warmup samples.
        """
        if epoch_timestamps and len(epoch_timestamps) >= JobRunner.MIN_EPOCH_TIMESTAMPS_FOR_STEADY:
            intervals = [epoch_timestamps[i + 1][1] - epoch_timestamps[i][1] for i in range(len(epoch_timestamps) - 1)]
            mean_epoch_time = sum(intervals) / len(intervals)
            logger.info(
                "Profiling: warmup epoch excluded, mean=%.4fs/epoch over %d sample(s)",
                mean_epoch_time,
                len(intervals),
            )
            return mean_epoch_time
        # Fall back to wall-clock when we don't have enough samples for a
        # warmup-corrected mean.
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
        warmup_excluded = bool(epoch_timestamps and len(epoch_timestamps) >= self.MIN_EPOCH_TIMESTAMPS_FOR_STEADY)
        logger.info(
            "Profiling result for job %s: %s = %.1fs (epochs=%s, warmup_excluded=%s)",
            job_id[:JOB_ID_DISPLAY_LENGTH],
            job["assigned_gpu_config"],
            duration,
            total_epochs,
            warmup_excluded,
        )

        if self.on_profiling_complete:
            await self.on_profiling_complete(
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

    async def _stop_job(self, job_id: str, *, reason: str = "user") -> None:
        # ``reason="user"`` → PREEMPTED (sticky, manual resume).
        # ``reason="auto"`` → QUEUED + cleared assignment (auto-handled by
        # the next scheduler pass).
        if reason == "auto":
            stop_kwargs: dict[str, Any] = {
                "status": JobStatus.QUEUED,
                "assigned_node": None,
                "assigned_gpu_config": None,
                "container_name": None,
            }
        else:
            stop_kwargs = {"status": JobStatus.PREEMPTED}

        # Fast path: tracked locally — kill first, mark only on success.
        # Marking the DB before the kill would leave the row lying about the
        # actual state if executor.kill raises (e.g. daemon hiccup).
        handle = self.running_jobs.get(job_id)
        if handle:
            logger.info("Killing tracked job %s (reason=%s)", job_id[:JOB_ID_DISPLAY_LENGTH], reason)
            killed = await self.executor.kill(handle)
            if not killed:
                logger.error("Failed to kill tracked job %s", job_id[:JOB_ID_DISPLAY_LENGTH])
                return
            async with self.get_conn() as conn:
                await self._update_job(conn, job_id, **stop_kwargs)
            return

        # Slow path: look up from DB
        async with self.get_conn() as conn:
            job = await self._fetch_job(conn, job_id, "container_name", "status")
            if not job:
                logger.warning("Job %s not found", job_id[:JOB_ID_DISPLAY_LENGTH])
                return

            if job["status"] == JobStatus.QUEUED:
                logger.info("Job %s is QUEUED, applying %s stop", job_id[:JOB_ID_DISPLAY_LENGTH], reason)
                await self._update_job(conn, job_id, **stop_kwargs)
                return

            if job["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
                logger.info("Job %s status is %s, no action needed", job_id[:JOB_ID_DISPLAY_LENGTH], job["status"])
                return

            container_name = job["container_name"] or f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
            killed = await self.executor.kill(JobHandle(container_name=container_name))
            if killed:
                logger.info("Container %s killed (reason=%s)", container_name, reason)
                await self._update_job(conn, job_id, **stop_kwargs)
            else:
                logger.error("Failed to kill container %s", container_name)

"""FastAPI application factory, lifespan, and CORS setup."""

import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool
from shared.constants import PG_NOTIFY_SCHEDULE, JobStatus

import src.state as state
from src.cluster import cluster
from src.constants import CORS_ALLOWED_ORIGINS, DEFAULT_DATABASE_URL
from src.executors import create_executor
from src.job_dispatcher import JobDispatcher
from src.job_runner import JobRunner
from src.optimizer import optimize
from src.profiling import scheduler
from src.routers import router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Application lifespan manager."""
    # Load cluster configuration (nodes + GPU energy costs)
    cluster.load_nodes()
    cluster.load_gpu_energy_costs()

    # Get configuration from environment
    database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

    # Connect to database (connection pool)
    masked_url = database_url.split("@")[-1] if "@" in database_url else database_url
    logger.info("Connecting to database: %s", masked_url)
    state.pool = AsyncConnectionPool(conninfo=database_url, min_size=2, max_size=50, open=False)
    await state.pool.open()

    # Create tables, indexes, and constraints from schema.sql
    schema_path = Path(__file__).resolve().parent.parent / "schema.sql"
    schema_sql = schema_path.read_text()
    async with state.get_conn() as conn, conn.transaction():
        await conn.execute(schema_sql)
    logger.info("Database initialized")

    # ------------------------------------------------------------------
    # Job runner
    # ------------------------------------------------------------------

    executor_name = os.getenv("EXECUTOR", "docker")
    executor = create_executor(executor_name)
    logger.info("Executor: %s", executor_name)

    async def _on_profiling_complete(
        _conn: object,
        job_id: str,
        gpu_config: dict[str, int],
        node_id: str,
        duration: float,
        job_type_id: str | None = None,
    ) -> None:
        """Handle profiling result — insert to DB, re-schedule job."""
        type_id = job_type_id or job_id
        async with state.schedule_lock, state.get_conn() as pconn, pconn.transaction():
            now = datetime.now(UTC)
            await pconn.execute(
                """UPDATE profiling_results
                   SET duration_seconds = %s, node_id = %s
                   WHERE job_id = %s AND gpu_config = %s::jsonb AND duration_seconds IS NULL""",
                (duration, node_id, type_id, Json(gpu_config)),
            )
            schedule_result = await scheduler.schedule_job(
                pconn,
                job_id,
                job_type_id=type_id,
            )
            await pconn.execute(
                "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                (JobStatus.QUEUED, now, job_id),
            )

        logger.info(
            "Recorded profiling result for job %s (type=%s): %s = %.1fs", job_id[:8], type_id, gpu_config, duration
        )
        if schedule_result.node_id is not None:
            await state.job_runner.enqueue(job_id)
            logger.info(
                "Re-queued job %s (%s mode, node=%s)", job_id[:8], schedule_result.mode, schedule_result.node_id
            )
        else:
            logger.info("No node available for job %s after profiling — watcher will retry", job_id[:8])

    async def _on_job_completed() -> None:
        """When any job finishes, try to schedule waiting QUEUED jobs."""
        await _schedule_waiting_jobs()

    local_runner = JobRunner(
        executor=executor,
        get_conn=state.get_conn,
        schedule_lock=state.schedule_lock,
        on_profiling_complete=_on_profiling_complete,
        on_job_completed=_on_job_completed,
    )
    state.job_runner = JobDispatcher(local_runner, state.get_conn, cluster)
    await state.job_runner.start()

    # ------------------------------------------------------------------
    # Fallback watcher: catch anything missed (runs infrequently)
    # ------------------------------------------------------------------

    async def _schedule_waiting_jobs() -> None:
        """Run the optimizer and greedy scheduler to assign/preempt jobs.

        Phase 1 (optimizer): sends ALL active jobs + currentScheduling.
        The optimizer may preempt running jobs to make room for urgent ones.
        Phase 2 (greedy): handles profiling runs and jobs without profiling data.
        """
        async with state.schedule_lock:
            # --- Phase 1: optimizer with preemption support ---
            optimizer_handled: set[str] = set()
            to_enqueue: list[tuple[str, str]] = []

            async with state.get_conn() as conn:
                node_gpu_usage = await scheduler._get_node_gpu_usage(conn)
                opt_result = await optimize(conn, node_gpu_usage)

            # Phase 1a: preempt jobs the optimizer dropped, then wait
            # for containers to actually die before proceeding.
            if opt_result.preempt:
                for job_id in opt_result.preempt:
                    logger.info("Optimizer preempting job %s", job_id[:8])
                    try:
                        await state.job_runner.stop(job_id)
                    except Exception:
                        logger.warning("Failed to stop preempted job %s", job_id[:8])
                    optimizer_handled.add(job_id)

                # Wait for containers to actually die, then re-queue the
                # preempted jobs.  Poll up to 15s for status to become
                # PREEMPTED (remote workers via HTTP can be slow).
                logger.info("Waiting for %d preempted container(s) to stop...", len(opt_result.preempt))
                for _ in range(15):
                    await asyncio.sleep(1)
                    async with state.get_conn() as conn:
                        cur = await conn.execute(
                            "SELECT count(*) FROM jobs WHERE id = ANY(%s) AND status != %s",
                            (opt_result.preempt, JobStatus.PREEMPTED),
                        )
                        row = await cur.fetchone()
                        not_preempted = row[0] if row else 0
                    if not_preempted == 0:
                        break

                # Re-queue them so they get rescheduled (and resume
                # from checkpoint) on the next cycle.
                async with state.get_conn() as conn:
                    now = datetime.now(UTC)
                    await conn.execute(
                        """UPDATE jobs SET status = %s, assigned_node = NULL,
                           assigned_gpu_config = NULL, updated_at = %s
                           WHERE id = ANY(%s) AND status = %s""",
                        (JobStatus.QUEUED, now, opt_result.preempt, JobStatus.PREEMPTED),
                    )

            # Phase 1b: apply optimizer assignments
            for a in opt_result.assignments:
                async with state.get_conn() as conn:
                    now = datetime.now(UTC)
                    cur = await conn.execute(
                        """UPDATE jobs
                           SET assigned_node = %s, assigned_gpu_config = %s,
                               is_profiling_run = FALSE, updated_at = %s
                           WHERE id = %s AND status = %s AND assigned_node IS NULL
                           RETURNING id""",
                        (a.node_id, Json(a.gpu_config), now, a.instance_id, JobStatus.QUEUED),
                    )
                    if await cur.fetchone():
                        to_enqueue.append((a.instance_id, a.node_id))
                        logger.info(
                            "Optimizer assigned job %s → node %s %s", a.instance_id[:8], a.node_id, a.gpu_config
                        )
                optimizer_handled.add(a.instance_id)

            async with state.get_conn() as conn:
                cur = await conn.execute(
                    "SELECT id, job_id FROM jobs WHERE status = %s AND assigned_node IS NULL ORDER BY created_at ASC",
                    (JobStatus.QUEUED,),
                )
                unassigned = [(row[0], row[1]) for row in await cur.fetchall()]

            for instance_id, type_id in unassigned:
                if instance_id in optimizer_handled:
                    continue
                async with state.get_conn() as conn:
                    result = await scheduler.schedule_job(conn, instance_id, job_type_id=type_id)
                    if result.node_id is not None:
                        now = datetime.now(UTC)
                        await conn.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            (JobStatus.QUEUED, now, instance_id),
                        )
                        to_enqueue.append((instance_id, result.node_id))

            for instance_id, node_id in to_enqueue:
                await state.job_runner.enqueue(instance_id)
                logger.info("Scheduled waiting job %s on node %s", instance_id[:8], node_id)

    async def _queue_watcher() -> None:
        """Safety net: retry scheduling every 60 s in case something was missed."""
        while True:
            await asyncio.sleep(60)
            try:
                await _schedule_waiting_jobs()
            except Exception:
                logger.exception("Queue watcher error")

    async def _notify_listener() -> None:
        """Wake the scheduler immediately when a worker sends NOTIFY after profiling."""
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
                    await conn.execute(f"LISTEN {PG_NOTIFY_SCHEDULE}")
                    async for _ in conn.notifies():
                        logger.debug("Received schedule notification — running scheduler")
                        asyncio.create_task(_schedule_waiting_jobs())
            except Exception:
                logger.warning("Notify listener lost connection, reconnecting in 5s")
                await asyncio.sleep(5)

    watcher_task = asyncio.create_task(_queue_watcher())
    listener_task = asyncio.create_task(_notify_listener())

    yield

    # Cleanup
    watcher_task.cancel()
    listener_task.cancel()
    await state.job_runner.shutdown()
    if state.pool:
        await state.pool.close()


app = FastAPI(
    title="Intelligent Job Management Platform",
    description="API for Intelligent Job Management",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS configuration for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

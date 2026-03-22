"""FastAPI application factory, lifespan, and CORS setup."""

import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from psycopg.types.json import Json  # type: ignore[import-not-found]
from psycopg_pool import AsyncConnectionPool  # type: ignore[import-not-found]
from shared.constants import JobStatus

import src.state as state
from src.cluster import cluster
from src.constants import CORS_ALLOWED_ORIGINS, DEFAULT_DATABASE_URL
from src.executors import create_executor
from src.job_runner import JobRunner
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
    import re

    masked_url = re.sub(r"://[^@]+@", "://*****@", database_url)
    logger.info("Connecting to database: %s", masked_url)
    state.pool = AsyncConnectionPool(conninfo=database_url, min_size=2, max_size=10, open=False)
    await state.pool.open()

    # Create tables, indexes, and constraints from schema.sql
    schema_path = Path(__file__).resolve().parent.parent / "schema.sql"
    schema_sql = schema_path.read_text()
    async with state.get_conn() as conn, conn.transaction():
        await conn.execute(schema_sql)
    logger.info("Database initialized")

    # ------------------------------------------------------------------
    # Job runner (replaces the old worker service + NATS)
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
                   WHERE job_id = %s AND gpu_config = %s AND duration_seconds IS NULL""",
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

    state.job_runner = JobRunner(
        executor=executor,
        get_conn=state.get_conn,
        schedule_lock=state.schedule_lock,
        on_profiling_complete=_on_profiling_complete,
        on_job_completed=_on_job_completed,
    )
    await state.job_runner.start()

    # ------------------------------------------------------------------
    # Fallback watcher: catch anything missed (runs infrequently)
    # ------------------------------------------------------------------

    async def _schedule_waiting_jobs() -> None:
        """Try to assign nodes to QUEUED jobs with assigned_node IS NULL."""
        async with state.schedule_lock:
            async with state.get_conn() as conn:
                cur = await conn.execute(
                    "SELECT id FROM jobs WHERE status = %s AND assigned_node IS NULL ORDER BY created_at ASC",
                    (JobStatus.QUEUED,),
                )
                unassigned = [row[0] for row in await cur.fetchall()]

            for job_id in unassigned:
                async with state.get_conn() as conn:
                    result = await scheduler.schedule_job(conn, job_id)
                    if result.node_id is not None:
                        now = datetime.now(UTC)
                        await conn.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            (JobStatus.QUEUED, now, job_id),
                        )
                        await state.job_runner.enqueue(job_id)
                        logger.info("Scheduled waiting job %s on node %s", job_id[:8], result.node_id)

    async def _queue_watcher() -> None:
        """Safety net: retry scheduling every 60 s in case something was missed."""
        while True:
            await asyncio.sleep(60)
            try:
                await _schedule_waiting_jobs()
            except Exception:
                logger.exception("Queue watcher error")

    watcher_task = asyncio.create_task(_queue_watcher())

    yield

    # Cleanup
    watcher_task.cancel()
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

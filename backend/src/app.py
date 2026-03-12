"""FastAPI application factory, lifespan, and CORS setup."""

import asyncio
import json
import logging
import os
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

import nats  # type: ignore[import-not-found]
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from nats.js.errors import BadRequestError  # type: ignore[import-not-found]
from psycopg.types.json import Json  # type: ignore[import-not-found]
from psycopg_pool import AsyncConnectionPool  # type: ignore[import-not-found]

import src.state as state
from src.cluster import cluster
from src.constants import (
    CORS_ALLOWED_ORIGINS,
    DEFAULT_DATABASE_URL,
    DEFAULT_NATS_URL,
    NATS_CONSUMER_PROFILING,
    NATS_STREAM_NAME,
    NATS_SUBJECT_PROFILING_COMPLETE,
    NATS_SUBJECT_SUBMITTED,
    NATS_SUBJECTS_PATTERN,
    JobStatus,
)
from src.profiling import scheduler
from src.routers import router

logger = logging.getLogger(__name__)

_VALID_STATUSES = ", ".join(f"'{s.value}'" for s in JobStatus)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Application lifespan manager."""
    # Load cluster configuration (nodes + GPU energy costs)
    cluster.load_nodes()
    cluster.load_gpu_energy_costs()

    # Get configuration from environment
    database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    nats_url = os.getenv("NATS_URL", DEFAULT_NATS_URL)

    # Connect to database (connection pool)
    masked_url = re.sub(r"://[^@]+@", "://*****@", database_url)
    logger.info("Connecting to database: %s", masked_url)
    state.pool = AsyncConnectionPool(conninfo=database_url, min_size=2, max_size=10)
    await state.pool.open()

    # Create tables, indexes, and constraints
    async with state.get_conn() as conn, conn.transaction():
        cur = conn.cursor()
        await cur.execute(f"""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    image TEXT NOT NULL,
                    command JSONB NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ({_VALID_STATUSES})),
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    container_name TEXT,
                    exit_code INT,
                    progress TEXT,
                    priority INT DEFAULT 3,
                    deadline TIMESTAMPTZ,
                    batch_size INT,
                    epochs_total INT,
                    profiling_epochs_no INT,
                    assigned_node TEXT,
                    required_memory_gb INT
                )
            """)
        # Migration for existing databases
        for col, col_type in [
            ("progress", "TEXT"),
            ("priority", "INT DEFAULT 3"),
            ("deadline", "TIMESTAMPTZ"),
            ("batch_size", "INT"),
            ("epochs_total", "INT"),
            ("profiling_epochs_no", "INT"),
            ("assigned_node", "TEXT"),
            ("required_memory_gb", "INT"),
            ("assigned_gpu_config", "JSONB"),
            ("estimated_duration", "FLOAT"),
            ("is_profiling_run", "BOOLEAN DEFAULT FALSE"),
        ]:
            await cur.execute(f"ALTER TABLE jobs ADD COLUMN IF NOT EXISTS {col} {col_type}")  # noqa: S608

        # Indexes
        await cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
        await cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC)")

        # Create profiling_results table
        await cur.execute("""
                CREATE TABLE IF NOT EXISTS profiling_results (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    gpu_config JSONB NOT NULL,
                    node_id TEXT NOT NULL,
                    duration_seconds FLOAT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                )
            """)
        await cur.execute("CREATE INDEX IF NOT EXISTS idx_profiling_results_job_id ON profiling_results(job_id)")
    logger.info("Database initialized")

    # Connect to NATS
    logger.info("Connecting to NATS: %s", nats_url)
    state.nc = await nats.connect(nats_url)
    state.js = state.nc.jetstream()

    # Ensure JetStream stream exists
    try:
        await state.js.add_stream(name=NATS_STREAM_NAME, subjects=[NATS_SUBJECTS_PATTERN])
        logger.info("NATS JetStream initialized")
    except BadRequestError:
        logger.info("NATS JetStream stream 'JOBS' already exists")

    # Subscribe to profiling completion events from the worker
    profiling_sub = await state.js.subscribe(
        NATS_SUBJECT_PROFILING_COMPLETE,
        durable=NATS_CONSUMER_PROFILING,
    )

    async def _profiling_listener() -> None:
        """Background task: handle profiling run completions."""
        assert state.js is not None
        async for msg in profiling_sub.messages:
            try:
                # Limit retries to avoid infinite redelivery loops
                meta = await msg.metadata()
                if meta.num_delivered and meta.num_delivered > 3:
                    logger.error("Profiling message exceeded retry limit (%d), acking to discard", meta.num_delivered)
                    await msg.ack()
                    continue

                data = json.loads(msg.data)
                job_id: str = data["job_id"]
                gpu_config: dict[str, int] = data["gpu_config"]
                node_id: str = data["node_id"]
                duration: float = data["duration_seconds"]

                async with state.get_conn() as conn:
                    # Record profiling result
                    result_id = str(uuid4())
                    now = datetime.now(UTC)
                    await conn.execute(
                        """INSERT INTO profiling_results
                           (id, job_id, gpu_config, node_id, duration_seconds, created_at)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (result_id, job_id, Json(gpu_config), node_id, duration, now),
                    )
                    logger.info(
                        "Recorded profiling result for job %s: %s = %.1fs",
                        job_id[:8],
                        gpu_config,
                        duration,
                    )

                    # After profiling one config, go straight to standard run on best config
                    schedule_result = await scheduler.schedule_standard_run(conn, job_id)

                    # Re-queue the job
                    await conn.execute(
                        "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                        (JobStatus.QUEUED, now, job_id),
                    )

                # Notify the worker
                await state.js.publish(
                    NATS_SUBJECT_SUBMITTED,
                    json.dumps({"job_id": job_id}).encode(),
                )
                logger.info(
                    "Re-queued job %s (%s mode, config=%s)",
                    job_id[:8],
                    schedule_result.mode,
                    schedule_result.gpu_config,
                )
                await msg.ack()
            except Exception:
                logger.exception("Error handling profiling completion")
                await msg.nak()

    profiling_task = asyncio.create_task(_profiling_listener())

    yield

    # Cleanup
    profiling_task.cancel()
    await profiling_sub.unsubscribe()

    if state.pool:
        await state.pool.close()
    if state.nc:
        await state.nc.close()


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

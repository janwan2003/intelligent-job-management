"""FastAPI application factory, lifespan, and CORS setup."""

import asyncio
import json
import logging
import os
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
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
    NATS_CONSUMER_COMPLETED,
    NATS_CONSUMER_PROFILING,
    NATS_STREAM_NAME,
    NATS_SUBJECT_COMPLETED,
    NATS_SUBJECT_PROFILING_COMPLETE,
    NATS_SUBJECT_SUBMITTED,
    NATS_SUBJECTS_PATTERN,
    JobStatus,
    nats_job_payload,
)
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
    nats_url = os.getenv("NATS_URL", DEFAULT_NATS_URL)

    # Connect to database (connection pool)
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
        """Background task: handle profiling run completions.

        Wraps the message loop in a retry so a transient NATS disconnect
        doesn't silently kill the task.
        """
        assert state.js is not None
        while True:
            try:
                async for msg in profiling_sub.messages:
                    await _handle_profiling_msg(msg)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Profiling listener crashed — restarting in 2 s")
                await asyncio.sleep(2)

    async def _handle_profiling_msg(msg: Any) -> None:
        """Process a single profiling-complete NATS message."""
        assert state.js is not None
        meta = msg.metadata
        if meta.num_delivered and meta.num_delivered > 3:
            logger.error("Profiling message exceeded retry limit (%d), discarding", meta.num_delivered)
            await msg.ack()
            return

        # Parse payload — reject malformed messages immediately
        try:
            data = json.loads(msg.data)
            job_id: str = data["job_id"]
            gpu_config: dict[str, int] = data["gpu_config"]
            node_id: str = data["node_id"]
            duration: float = data["duration_seconds"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            logger.error("Malformed profiling message, discarding: %s", exc)
            await msg.ack()
            return

        try:
            # All DB work in a single transaction for atomicity
            async with state.schedule_lock, state.get_conn() as conn, conn.transaction():
                result_id = str(uuid4())
                now = datetime.now(UTC)

                # Idempotent insert — ON CONFLICT skips duplicate (job_id, gpu_config)
                await conn.execute(
                    """INSERT INTO profiling_results
                       (id, job_id, gpu_config, node_id, duration_seconds, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (job_id, gpu_config) DO NOTHING""",
                    (result_id, job_id, Json(gpu_config), node_id, duration, now),
                )

                # Decide next step: more profiling or standard run
                schedule_result = await scheduler.schedule_job(conn, job_id, profiled_this_round=1)

                await conn.execute(
                    "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                    (JobStatus.QUEUED, now, job_id),
                )

            logger.info("Recorded profiling result for job %s: %s = %.1fs", job_id[:8], gpu_config, duration)

            if schedule_result.node_id is not None:
                await state.js.publish(NATS_SUBJECT_SUBMITTED, nats_job_payload(job_id))
                logger.info(
                    "Re-queued job %s (%s mode, config=%s, node=%s)",
                    job_id[:8],
                    schedule_result.mode,
                    schedule_result.gpu_config,
                    schedule_result.node_id,
                )
            else:
                logger.info(
                    "No node available for job %s after profiling — leaving QUEUED, watcher will retry",
                    job_id[:8],
                )

            await msg.ack()
        except Exception:
            logger.exception("Error handling profiling completion for job %s", job_id[:8])
            await msg.nak()

    profiling_task = asyncio.create_task(_profiling_listener())

    # ------------------------------------------------------------------
    # Schedule waiting QUEUED jobs (shared by completed listener + watcher)
    # ------------------------------------------------------------------

    async def _schedule_waiting_jobs() -> None:
        """Try to assign nodes to QUEUED jobs with assigned_node IS NULL."""
        assert state.js is not None
        scheduled: list[tuple[str, str]] = []  # (job_id, node_id)

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
                        scheduled.append((job_id, result.node_id))

        # Publish NATS events outside the lock to avoid holding it during I/O
        for job_id, node_id in scheduled:
            await state.js.publish(NATS_SUBJECT_SUBMITTED, nats_job_payload(job_id))
            logger.info("Scheduled waiting job %s on node %s", job_id[:8], node_id)

    # ------------------------------------------------------------------
    # Event-driven: react to jobs.completed (node just freed up)
    # ------------------------------------------------------------------

    completed_sub = await state.js.subscribe(
        NATS_SUBJECT_COMPLETED,
        durable=NATS_CONSUMER_COMPLETED,
    )

    async def _completed_listener() -> None:
        """When a job finishes, immediately try to schedule waiting jobs."""
        while True:
            try:
                async for msg in completed_sub.messages:
                    try:
                        await _schedule_waiting_jobs()
                    except Exception:
                        logger.exception("Error scheduling after job completion")
                    await msg.ack()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Completed listener crashed — restarting in 2 s")
                await asyncio.sleep(2)

    completed_task = asyncio.create_task(_completed_listener())

    # ------------------------------------------------------------------
    # Fallback watcher: catch anything missed (runs infrequently)
    # ------------------------------------------------------------------

    async def _queue_watcher() -> None:
        """Safety net: retry scheduling every 60 s in case an event was missed."""
        while True:
            await asyncio.sleep(60)
            try:
                await _schedule_waiting_jobs()
            except Exception:
                logger.exception("Queue watcher error")

    watcher_task = asyncio.create_task(_queue_watcher())

    yield

    # Cleanup
    profiling_task.cancel()
    completed_task.cancel()
    watcher_task.cancel()
    await profiling_sub.unsubscribe()
    await completed_sub.unsubscribe()

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

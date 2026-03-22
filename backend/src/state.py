"""Mutable global state for the IJM backend.

Set during app lifespan, read by routers.  Kept in a separate module to
avoid circular imports between ``app.py`` (which sets state) and routers
(which read state).
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import psycopg  # type: ignore[import-not-found]
from fastapi import HTTPException
from psycopg_pool import AsyncConnectionPool  # type: ignore[import-not-found]

# Global state — initialised in app.lifespan()
pool: AsyncConnectionPool | None = None
job_runner: Any = None

# Serialises all scheduling decisions so two coroutines cannot assign the same node
schedule_lock: asyncio.Lock = asyncio.Lock()


@asynccontextmanager
async def get_conn() -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    """Acquire a connection from the pool (async context manager)."""
    if pool is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    async with pool.connection() as conn:
        yield conn


def require_runner() -> Any:
    """Return the JobRunner or raise 503 if not initialized."""
    if job_runner is None:
        raise HTTPException(status_code=503, detail="Job runner not initialized")
    return job_runner

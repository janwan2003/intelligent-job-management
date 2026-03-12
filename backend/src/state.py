"""Mutable global state for the IJM backend.

Set during app lifespan, read by routers.  Kept in a separate module to
avoid circular imports between ``app.py`` (which sets state) and routers
(which read state).
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import psycopg  # type: ignore[import-not-found]
from fastapi import HTTPException
from nats.js import JetStreamContext  # type: ignore[import-not-found]
from psycopg_pool import AsyncConnectionPool  # type: ignore[import-not-found]

# Global state — initialised in app.lifespan()
pool: AsyncConnectionPool | None = None
nc: Any = None
js: JetStreamContext | None = None


@asynccontextmanager
async def get_conn() -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    """Acquire a connection from the pool (async context manager)."""
    if pool is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    async with pool.connection() as conn:
        yield conn


def require_js() -> JetStreamContext:
    """Return the JetStream context or raise 503 if not initialized."""
    if js is None:
        raise HTTPException(status_code=503, detail="NATS not initialized")
    return js

"""Mutable global state for the IJM backend.

Set during app lifespan, read by routers.  Kept in a separate module to
avoid circular imports between ``app.py`` (which sets state) and routers
(which read state).
"""

from typing import Any

import psycopg  # type: ignore[import-not-found]
from fastapi import HTTPException
from nats.js import JetStreamContext  # type: ignore[import-not-found]

# Global state — initialised in app.lifespan()
db_pool: psycopg.AsyncConnection[Any] | None = None
nc: Any = None
js: JetStreamContext | None = None


def require_db() -> psycopg.AsyncConnection[Any]:
    """Return the database connection or raise 503 if not initialized."""
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return db_pool


def require_js() -> JetStreamContext:
    """Return the JetStream context or raise 503 if not initialized."""
    if js is None:
        raise HTTPException(status_code=503, detail="NATS not initialized")
    return js

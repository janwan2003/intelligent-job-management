"""Mutable global state for the IJM backend.

Set during app lifespan, read by routers.  Kept in a separate module to
avoid circular imports between ``app.py`` (which sets state) and routers
(which read state).
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from fastapi import HTTPException
from psycopg_pool import AsyncConnectionPool

# Global state — initialised in app.lifespan()
pool: AsyncConnectionPool | None = None
job_runner: Any = None
# Per-node GPU slot semaphores; populated in app.lifespan() after reconcile().
# ``Any`` not ``NodeSlots`` to avoid a circular import (NodeSlots imports
# from shared.constants which doesn't depend on state).
node_slots: Any = None
# Registry of in-flight ``_dispatch_when_slot_free`` tasks, keyed by
# ``instance_id``.  Promoted out of app.py's lifespan closure so the
# ``/admin/dispatch-tasks`` endpoint can read it.  Also consulted by the
# reaper to coordinate cancellation rather than double-releasing.
dispatch_tasks: dict[str, asyncio.Task[None]] = {}

# Migrate plans that are mid-flight: the optimiser told us to move an
# instance from one node to another, but the row is currently RUNNING on
# the source so the apply step's ``WHERE assigned_node IS NULL`` UPDATE
# can't write the new destination yet.  We stash the planned assignment
# here; ``_apply_pending_migrates`` (invoked from the slot listener)
# reads it back and writes the new assignment once the source ``/stop``
# has drained the row back to QUEUED with assigned_node=NULL.  Keyed by
# ``instance_id``; ``Any`` is the ``optimizer.Assignment`` dataclass
# (typed loosely to avoid a circular import — state must not import
# optimizer).
pending_migrates: dict[str, Any] = {}

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

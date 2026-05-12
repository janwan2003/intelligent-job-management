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

# Serialises all scheduling decisions so two coroutines cannot assign the same node
schedule_lock: asyncio.Lock = asyncio.Lock()

# Jobs that ``_preempt_and_release`` has been spawned for but whose terminal
# DB commit hasn't landed yet.  ``ProfilingScheduler.get_node_gpu_usage``
# subtracts their (node, gpu_config) from the live usage map so scheduler
# decisions reflect the post-eviction state immediately.  Without this,
# profile-preempts that need to free 2+ GPUs race the staggered commits:
# the first eviction commits, the scheduler re-runs, sees only N-1 slots
# free, falls back to a 1-GPU standard placement on the same victim, and
# the second eviction's commit lands into a re-occupied slot.
# ``_preempt_and_release`` populates it before spawning the stop and
# clears it after the stop commit lands.
pending_evictions: dict[str, tuple[str, dict[str, int]]] = {}


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

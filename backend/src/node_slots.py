"""Per-node GPU slot semaphores for async-coordinated dispatch.

A permit on ``NodeSlots[node_id]`` represents one available GPU on that node.
Jobs ``acquire`` a permit before the API issues ``/run`` to the worker, and
the permit is ``release``d when the slot becomes free again — i.e. when the
worker confirms a kill+persist (auto-preempt or user-stop) or when the job
reaches a terminal status (SUCCEEDED / FAILED).

This replaces the previous "poll the DB every 100 ms for ``assigned_node IS
NULL``" pattern in ``_schedule_waiting_jobs``: each dispatch task awaits its
own slot semaphore, so a slow preempt on one node no longer gates dispatches
to other nodes.

The semaphore lives in API process memory only — no DB-backed state, no
cross-process sharing (the API is the singleton consumer).  The DB remains
the durable source of truth; ``reconcile()`` re-establishes the in-memory
counts from a fresh DB query at startup, so an API restart loses nothing.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from shared.constants import JobStatus

from src.cluster import ClusterManager

logger = logging.getLogger(__name__)


class NodeSlots:
    """asyncio.Semaphore-per-node, capacity = total GPUs on that node.

    Invariant (after every quiescent point):
        permits_available[node]
            == total_gpus[node]
               - sum(gpus for jobs WHERE
                       status IN (RUNNING, PROFILING)
                       OR (status = QUEUED AND assigned_node IS NOT NULL))
    """

    def __init__(self, cluster: ClusterManager) -> None:
        self._totals: dict[str, int] = {}
        self._sems: dict[str, asyncio.Semaphore] = {}
        for raw in cluster.nodes:
            node_id = raw["id"]
            total = sum(int(r["gpu_count"]) for r in raw.get("resources", []))
            self._totals[node_id] = total
            self._sems[node_id] = asyncio.Semaphore(total)
        logger.info(
            "NodeSlots initialised: %s",
            dict(self._totals),
        )

    # ------------------------------------------------------------------
    # Capacity introspection (for logging / drift detection)
    # ------------------------------------------------------------------

    def total(self, node_id: str) -> int:
        return self._totals.get(node_id, 0)

    def available(self, node_id: str) -> int:
        sem = self._sems.get(node_id)
        if sem is None:
            return 0
        # asyncio.Semaphore exposes ``_value`` as a public-enough attribute
        # for diagnostics; it's stable across CPython versions we target.
        return sem._value  # noqa: SLF001

    # ------------------------------------------------------------------
    # Acquire / release
    # ------------------------------------------------------------------

    async def acquire(self, node_id: str, n: int = 1) -> None:
        """Block until *n* permits are available on *node_id*, then take them.

        ``asyncio.Semaphore`` is FIFO-ish: tasks waiting on ``acquire`` are
        woken in the order they suspended.  Multiple permits are taken
        sequentially — for n=2 we may briefly hold 1 permit while waiting
        for the second, which is fine because we never release partials.
        """
        sem = self._sems.get(node_id)
        if sem is None:
            raise KeyError(f"unknown node_id {node_id!r}")
        for _ in range(n):
            await sem.acquire()

    def release(self, node_id: str, n: int = 1) -> None:
        """Return *n* permits to *node_id*'s pool.

        Tolerant of unknown node_ids (logs and ignores) so a stale
        ``ijm_slot_freed`` notification from a node that was removed from
        the cluster config doesn't crash the listener.
        """
        sem = self._sems.get(node_id)
        if sem is None:
            logger.warning("release() for unknown node_id %s — ignored", node_id)
            return
        for _ in range(n):
            sem.release()

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    async def reconcile(self, get_conn: Any) -> None:
        """Pre-acquire permits for every currently-occupied slot.

        Called once at API startup, after ``__init__`` (which created the
        semaphores at full capacity).  Iterates over jobs whose containers
        are actually running on a worker (status RUNNING / PROFILING) and
        takes one permit per GPU they hold.

        Note: ``QUEUED`` rows with an ``assigned_node`` are *not* counted —
        they have no live container, so they don't actually occupy a slot.
        The next ``_dispatch_when_slot_free`` task will acquire a permit at
        the moment it posts ``/run``, which is when the slot truly becomes
        occupied.  Counting QUEUED+assigned rows here would over-reserve
        capacity for orphaned dispatches (left over from API crashes or
        stuck migrations).
        """
        async with get_conn() as conn:
            cur = await conn.execute(
                "SELECT assigned_node, assigned_gpu_config FROM jobs "
                "WHERE assigned_node IS NOT NULL "
                "  AND assigned_gpu_config IS NOT NULL "
                "  AND status IN (%s, %s)",
                (JobStatus.RUNNING, JobStatus.PROFILING),
            )
            rows = await cur.fetchall()

        # Sum used GPUs per node first; we need a count check before
        # awaiting acquires (asyncio.Semaphore has no acquire_nowait, and
        # awaiting on a drained semaphore would block forever during
        # startup if the DB is over-subscribed).
        used_per_node: dict[str, int] = {}
        for row in rows:
            node_id = row[0]
            gpu_config = row[1] or {}
            n_gpus = sum(int(v) for v in gpu_config.values())
            if node_id not in self._sems:
                logger.warning(
                    "reconcile: job assigned to unknown node %s — skipping",
                    node_id,
                )
                continue
            used_per_node[node_id] = used_per_node.get(node_id, 0) + n_gpus

        for node_id, used in used_per_node.items():
            total = self._totals[node_id]
            if used > total:
                logger.error(
                    "reconcile: node %s over-subscribed (db=%d, total=%d) — clamping to total; manual cleanup required",
                    node_id,
                    used,
                    total,
                )
                used = total
            sem = self._sems[node_id]
            for _ in range(used):
                # Semaphore was just constructed at full capacity, so this
                # never blocks (we already verified used <= total above).
                await sem.acquire()

        logger.info(
            "NodeSlots reconciled: available=%s",
            {n: self.available(n) for n in self._sems},
        )

    # ------------------------------------------------------------------
    # Drift detection (optional periodic safety check)
    # ------------------------------------------------------------------

    async def detect_drift(self, get_conn: Any) -> dict[str, tuple[int, int]]:
        """Return ``{node: (in_memory_used, db_used)}`` for nodes that disagree.

        Empty dict means in-memory state matches DB.  Useful as a 5-minute
        sanity check to catch leaked permits from crashed dispatch tasks.
        """
        async with get_conn() as conn:
            cur = await conn.execute(
                "SELECT assigned_node, assigned_gpu_config FROM jobs "
                "WHERE assigned_node IS NOT NULL "
                "  AND assigned_gpu_config IS NOT NULL "
                "  AND status IN (%s, %s, %s)",
                (JobStatus.RUNNING, JobStatus.PROFILING, JobStatus.QUEUED),
            )
            rows = await cur.fetchall()

        db_used: dict[str, int] = {}
        for row in rows:
            db_used[row[0]] = db_used.get(row[0], 0) + sum(int(v) for v in (row[1] or {}).values())

        drift: dict[str, tuple[int, int]] = {}
        for node_id, total in self._totals.items():
            mem_used = total - self.available(node_id)
            db_count = db_used.get(node_id, 0)
            if mem_used != db_count:
                drift[node_id] = (mem_used, db_count)
        return drift


# Re-exported as a singleton via ``state.node_slots`` (set during lifespan).
__all__ = ["NodeSlots"]

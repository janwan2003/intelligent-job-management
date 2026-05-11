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
import os
from typing import Any

from shared.constants import JobStatus

from src.cluster import ClusterManager

logger = logging.getLogger(__name__)

# Toggle slot.acquire/release log lines between INFO and DEBUG without a
# code change.  Default DEBUG (quiet); set IJM_SLOT_VERBOSE=1 to surface
# every permit transition.  Leak diagnoses are much faster with this on.
_SLOT_LOG_LEVEL = logging.INFO if os.getenv("IJM_SLOT_VERBOSE") else logging.DEBUG


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
        # Explicit "permits in use" counter mirroring the semaphore.  Avoids
        # reaching into ``asyncio.Semaphore._value`` for diagnostics, which
        # is private API and would silently miss queued waiters.  Incremented
        # alongside every successful ``acquire`` and decremented on ``release``;
        # the two stay in lockstep because both ops execute synchronously
        # inside the event loop after the await point.
        self._used: dict[str, int] = {}
        # Per-node lock so that ``acquire(n)`` for n > 1 is atomic — without
        # this, two callers asking for n=2 each on a 2-permit node can both
        # acquire 1 permit and then deadlock waiting for the second.
        self._acquire_locks: dict[str, asyncio.Lock] = {}
        # Lifetime counters for /admin/slots — per-node would balloon the
        # output for clusters with many nodes; cluster-wide totals are
        # enough to answer "is slot churn normal?" at a glance.
        self._metrics: dict[str, int] = {
            "acquire_count": 0,
            "release_count": 0,
            "acquire_oversub_count": 0,
            "release_underflow_count": 0,
            "drift_recovery_count": 0,
        }
        for raw in cluster.nodes:
            node_id = raw["id"]
            total = sum(int(r["gpu_count"]) for r in raw.get("resources", []))
            self._totals[node_id] = total
            self._sems[node_id] = asyncio.Semaphore(total)
            self._used[node_id] = 0
            self._acquire_locks[node_id] = asyncio.Lock()
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
        if node_id not in self._totals:
            return 0
        return self._totals[node_id] - self._used.get(node_id, 0)

    # ------------------------------------------------------------------
    # Acquire / release
    # ------------------------------------------------------------------

    async def acquire(self, node_id: str, n: int = 1) -> None:
        """Block until *n* permits are available on *node_id*, then take them.

        Held under a per-node ``asyncio.Lock`` so multi-permit acquires don't
        deadlock against each other (two concurrent ``acquire(2)`` on a
        2-permit node would otherwise each take 1 permit and stall).  Cancel-
        safe: a CancelledError between partial acquires releases the partials
        before propagating, leaving ``_used`` and the semaphore consistent.
        """
        sem = self._sems.get(node_id)
        if sem is None:
            raise KeyError(f"unknown node_id {node_id!r}")
        lock = self._acquire_locks[node_id]
        async with lock:
            taken = 0
            try:
                for _ in range(n):
                    await sem.acquire()
                    taken += 1
                    self._used[node_id] = self._used.get(node_id, 0) + 1
            except BaseException:
                # Roll back partial acquires so the caller's error path doesn't
                # have to know how many were taken.
                for _ in range(taken):
                    sem.release()
                    self._used[node_id] = max(0, self._used.get(node_id, 0) - 1)
                raise
        total = self._totals.get(node_id, 0)
        self._metrics["acquire_count"] += 1
        if self._used[node_id] > total:
            self._metrics["acquire_oversub_count"] += 1
            logger.error(
                "SLOT-OVERSUB: %s used=%d > total=%d after acquire(%d)",
                node_id,
                self._used[node_id],
                total,
                n,
            )
        logger.log(
            _SLOT_LOG_LEVEL,
            "slot.acquire %s n=%d → used=%d/%d sem._value=%d",
            node_id,
            n,
            self._used.get(node_id, 0),
            total,
            sem._value,  # noqa: SLF001
        )

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
        before_used = self._used.get(node_id, 0)
        for _ in range(n):
            sem.release()
            self._used[node_id] = max(0, self._used.get(node_id, 0) - 1)
        total = self._totals.get(node_id, 0)
        self._metrics["release_count"] += 1
        if before_used == 0:
            self._metrics["release_underflow_count"] += 1
            logger.error(
                "SLOT-UNDERFLOW: %s release(%d) called when used=0 (sem._value=%d)",
                node_id,
                n,
                sem._value,  # noqa: SLF001
            )
        logger.log(
            _SLOT_LOG_LEVEL,
            "slot.release %s n=%d → used=%d/%d sem._value=%d",
            node_id,
            n,
            self._used.get(node_id, 0),
            total,
            sem._value,  # noqa: SLF001
        )

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
            # Pre-acquire only for rows whose container is actively occupying
            # a GPU at restart time (RUNNING/PROFILING).  Earlier we also
            # included QUEUED-with-assigned_node, but those permits leak if
            # the row is later deleted (DELETE removes the row before any
            # NOTIFY would fire, so the pre-acquired permit stays held).
            # The narrow definition of "occupying a slot" matches what the
            # worker emits ``ijm_slot_freed`` for — keeps acquire/release
            # symmetric.
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
                self._used[node_id] = self._used.get(node_id, 0) + 1

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
            # Only RUNNING/PROFILING jobs hold a permit.  QUEUED rows with
            # an assigned_node have not yet acquired (the dispatch task does
            # that just before posting /run), so including them would be a
            # false-positive drift any time something is queued.
            cur = await conn.execute(
                "SELECT assigned_node, assigned_gpu_config FROM jobs "
                "WHERE assigned_node IS NOT NULL "
                "  AND assigned_gpu_config IS NOT NULL "
                "  AND status IN (%s, %s)",
                (JobStatus.RUNNING, JobStatus.PROFILING),
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

    async def recover_from_drift(self, get_conn: Any) -> dict[str, int]:
        """Force in-memory ``_used`` to match DB authoritative count.

        Idempotent: releases excess permits or acquires missing ones until
        the in-memory state agrees with the DB.  Returns the per-node delta
        applied (positive = acquired, negative = released).

        Use this after any event that may have desynced state — listener
        reconnect (lost NOTIFYs during downtime), manual /admin/reconcile,
        or a periodic drift heartbeat.  Without it, a dropped tunnel can
        leak permits across the silent window.
        """
        drift = await self.detect_drift(get_conn)
        deltas: dict[str, int] = {}
        for node_id, (mem_used, db_used) in drift.items():
            sem = self._sems[node_id]
            delta = db_used - mem_used
            if delta > 0:
                # DB knows of more occupied slots than memory does — acquire.
                # Use acquire_nowait equivalent: the semaphore must currently
                # have at least ``delta`` permits free, which it does by
                # definition (mem_used + delta = db_used <= total).
                for _ in range(delta):
                    await sem.acquire()
                    self._used[node_id] = self._used.get(node_id, 0) + 1
            else:
                for _ in range(-delta):
                    sem.release()
                    self._used[node_id] = max(0, self._used.get(node_id, 0) - 1)
            deltas[node_id] = delta
            self._metrics["drift_recovery_count"] += 1
            logger.warning(
                "recover_from_drift: %s adjusted by %+d (mem_used %d→%d, db_used=%d)",
                node_id,
                delta,
                mem_used,
                mem_used + delta,
                db_used,
            )
        return deltas

    def metrics(self) -> dict[str, int]:
        """Lifetime counter snapshot for /admin/slots."""
        return dict(self._metrics)


# Re-exported as a singleton via ``state.node_slots`` (set during lifespan).
__all__ = ["NodeSlots"]

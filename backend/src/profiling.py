"""Profiling scheduler for the IJM backend.

Manages incremental GPU configuration profiling before real job execution.
"""

import itertools
import logging
import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.types.json import Json
from shared.constants import JobStatus

from src.cluster import cluster
from src.constants import DEFAULT_PROFILING_CONFIGS_PER_JOB
from src.models import NodeConfig, ScheduleResult
from src.utils.gpu import config_key

logger = logging.getLogger(__name__)


class ProfilingScheduler:
    """Incremental profiling strategy scheduler.

    Each scheduling round profiles up to ``configs_per_job`` new GPU
    configurations before switching to a standard run.  Across multiple
    rounds every valid configuration is eventually visited.

    The per-round limit is controlled by the ``PROFILING_CONFIGS_PER_JOB``
    environment variable (default 1).
    """

    def __init__(self) -> None:
        self.configs_per_job = int(os.getenv("PROFILING_CONFIGS_PER_JOB", str(DEFAULT_PROFILING_CONFIGS_PER_JOB)))

    @staticmethod
    def _node_configs(*, is_for_profiling: bool) -> dict[str, dict[str, int]]:
        """Generate all non-zero GPU configs for eligible nodes, deduplicated by key.

        - ``is_for_profiling=True``: only nodes with ``isForProfiling`` set.
        - ``is_for_profiling=False``: **all** nodes (every node can run standard jobs).
        """
        configs: dict[str, dict[str, int]] = {}
        for raw in cluster.nodes:
            node = NodeConfig.model_validate(raw)
            if not node.resources:
                continue
            if is_for_profiling and not node.is_for_profiling:
                continue
            ranges = [range(res.gpu_count + 1) for res in node.resources]
            for combo in itertools.product(*ranges):
                if all(c == 0 for c in combo):
                    continue
                parts = {res.gpu_type: count for res, count in zip(node.resources, combo, strict=True) if count > 0}
                configs.setdefault(config_key(parts), parts)
        return configs

    def get_valid_configurations(self) -> list[dict[str, int]]:
        """Derive GPU configurations worth profiling: intersection of profiling and production node capabilities.

        Returns a deduplicated list sorted (1) by total GPU count ascending,
        then (2) by GPU type name alphabetically.  The secondary sort is
        load-bearing for scenario determinism: ``profiling.keys() &
        production.keys()`` is a ``set`` whose iteration order is not
        insertion-stable, so without the secondary key two equal-sum configs
        (e.g. ``QuadroP600$\times$1`` and ``A40$\times$1``) could come back
        in different order across runs / Python versions, and the
        e2e "which config gets profiled first" prediction would flip.
        Alphabetical = ``A40`` < ``QuadroP600``, so single-GPU A40 ranks
        before single-GPU P600.
        """
        production = self._node_configs(is_for_profiling=False)
        profiling = self._node_configs(is_for_profiling=True)

        configs = [profiling[key] for key in profiling.keys() & production.keys()]
        configs.sort(key=lambda c: (sum(c.values()), sorted(c.keys())))
        return configs

    async def get_node_gpu_usage(self, conn: psycopg.AsyncConnection[Any]) -> dict[str, dict[str, int]]:
        """Query allocated GPUs per node from currently running/profiling jobs.

        Subtracts ``state.pending_evictions`` (jobs whose ``/stop`` is in
        flight but the worker's terminal commit hasn't landed yet) so
        scheduling decisions reflect the post-eviction state immediately.
        Without this, a profile-preempt that needs to free 2+ GPUs
        deadlocks: the first eviction commits, the scheduler re-runs,
        sees N-1 slots free, falls back to a 1-GPU standard placement
        on the same victim, and the second eviction's commit lands into
        a re-occupied slot.

        Returns ``{node_id: {gpu_type: allocated_count, ...}, ...}``.
        """
        from src import state

        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, assigned_node, assigned_gpu_config FROM jobs "
                "WHERE status IN (%s, %s, %s) AND assigned_node IS NOT NULL AND assigned_gpu_config IS NOT NULL",
                (JobStatus.RUNNING, JobStatus.PROFILING, JobStatus.QUEUED),
            )
            rows = await cur.fetchall()

        usage: dict[str, dict[str, int]] = {}
        for job_id, node_id, gpu_config in rows:
            if job_id in state.pending_evictions:
                continue  # treat the slot as already freed
            node_usage = usage.setdefault(node_id, {})
            for gpu_type, count in gpu_config.items():
                node_usage[gpu_type] = node_usage.get(gpu_type, 0) + count
        return usage

    def _find_node_for_config(
        self,
        gpu_config: dict[str, int],
        *,
        is_for_profiling: bool,
        node_gpu_usage: dict[str, dict[str, int]] | None = None,
    ) -> NodeConfig | None:
        """Find a node with enough *remaining* GPUs for the given config.

        Subtracts already-allocated GPUs (from *node_gpu_usage*) from each
        node's total capacity before checking fit.  Prefers smallest total
        surplus so jobs pack tightly.

        - Standard runs (``is_for_profiling=False``): profiling-designated nodes
          are excluded so production jobs never land on them.
        - Profiling runs (``is_for_profiling=True``): only profiling-designated
          nodes are considered.
        """
        if node_gpu_usage is None:
            node_gpu_usage = {}
        required = gpu_config
        candidates: list[tuple[NodeConfig, int]] = []  # (node, total_surplus)
        for raw in cluster.nodes:
            node = NodeConfig.model_validate(raw)
            used = node_gpu_usage.get(node.id, {})
            remaining = {res.gpu_type: res.gpu_count - used.get(res.gpu_type, 0) for res in node.resources}
            total_surplus = 0
            match = True
            for gpu_type, needed in required.items():
                avail = remaining.get(gpu_type, 0)
                if avail < needed:
                    match = False
                    break
                total_surplus += avail - needed
            if match:
                candidates.append((node, total_surplus))

        if is_for_profiling:
            # Profiling runs MUST use a profiling-designated node
            candidates = [(n, s) for n, s in candidates if n.is_for_profiling]
        # Standard runs can use any node (including profiling-designated ones)

        candidates.sort(key=lambda pair: pair[1])
        return candidates[0][0] if candidates else None

    async def get_profiled_configs(
        self, conn: psycopg.AsyncConnection[Any], job_id: str, *, completed_only: bool = False
    ) -> list[dict[str, int]]:
        """Get configs claimed or completed for this job type.

        With ``completed_only=True``, only returns configs with a recorded duration
        (used by ``_find_available_config`` to pick a config for standard runs).
        By default returns all claimed configs (including in-flight profiling runs),
        so the scheduler naturally skips them.
        """
        if completed_only:
            query = (
                "SELECT DISTINCT gpu_config FROM profiling_results WHERE job_id = %s AND duration_seconds IS NOT NULL"
            )
        else:
            query = "SELECT DISTINCT gpu_config FROM profiling_results WHERE job_id = %s"
        async with conn.cursor() as cur:
            await cur.execute(query, (job_id,))
            rows = await cur.fetchall()
        return [row[0] for row in rows]

    async def _find_available_config(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        node_gpu_usage: dict[str, dict[str, int]] | None = None,
    ) -> tuple[dict[str, int], NodeConfig] | None:
        """Find a profiled config that has an available production node.

        Iterates profiled configs in canonical sort order (fewest GPUs first,
        then GPU type name alphabetically — same key
        :py:meth:`get_valid_configurations` uses) and returns the first
        ``(config, node)`` pair for which a non-profiling node has room.
        The sort matters: without it, the iteration order is whatever the
        DB returns for ``SELECT DISTINCT gpu_config``, which can land the
        scheduler on a strictly-worse multi-GPU placement (e.g.
        ``lstm-small`` on ``A40$\times$2`` when both A40 slots are free,
        even though ``A40$\times$1`` finishes faster AND is cheaper).
        """
        profiled = await self.get_profiled_configs(conn, job_id, completed_only=True)
        profiled.sort(key=lambda c: (sum(c.values()), sorted(c.keys())))
        for gpu_config in profiled:
            node = self._find_node_for_config(gpu_config, is_for_profiling=False, node_gpu_usage=node_gpu_usage)
            if node:
                return gpu_config, node
        return None

    async def _persist_assignment(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        result: ScheduleResult,
        type_id: str = "",
        instance_id: str = "",
    ) -> None:
        """Write the scheduling decision to the jobs table.

        Profile claims are inserted upfront by ``_atomic_claim_remaining``;
        this method only updates the ``jobs`` row.
        """
        del type_id, instance_id  # unused; kept for signature stability
        now = datetime.now(UTC)
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE jobs
                   SET assigned_node = %s, assigned_gpu_config = %s,
                       is_profiling_run = %s, updated_at = %s
                   WHERE id = %s""",
                (
                    result.node_id,
                    Json(result.gpu_config) if result.gpu_config else None,
                    result.is_profiling_run,
                    now,
                    job_id,
                ),
            )

    async def schedule_standard_run(self, conn: psycopg.AsyncConnection[Any], job_id: str) -> ScheduleResult:
        """Schedule a standard (non-profiling) run using any available profiled config.

        Called after a profiling run completes to immediately transition to real
        execution on any configuration that has been measured.
        """
        node_gpu_usage = await self.get_node_gpu_usage(conn)
        available = await self._find_available_config(conn, job_id, node_gpu_usage)
        if available:
            gpu_config, node = available
        else:
            gpu_config, node = None, None

        result = ScheduleResult(
            mode="standard",
            gpu_config=gpu_config,
            node_id=node.id if node else None,
            is_profiling_run=False,
        )

        await self._persist_assignment(conn, job_id, result)

        logger.info(
            "Scheduled standard run for job %s: config=%s, node=%s",
            job_id[:8],
            result.gpu_config,
            result.node_id,
        )
        return result

    async def _count_profiled_this_round(self, conn: psycopg.AsyncConnection[Any], instance_id: str) -> int:
        """Count how many profiling configs this specific job instance has claimed."""
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM profiling_results WHERE instance_id = %s",
                (instance_id,),
            )
            row = await cur.fetchone()
        return row[0] if row else 0

    async def _sweep_stale_claims(self, conn: psycopg.AsyncConnection[Any], type_id: str) -> int:
        """Backward-compat wrapper: sweep stale claims, scoped to one type.

        New callers should prefer :meth:`sweep_all_stale_claims` (no type
        scope) so claims for *every* type are reclaimed on every scheduler
        round, not just on submission.
        """
        return await self._sweep_stale_claims_filtered(conn, type_id=type_id)

    async def sweep_all_stale_claims(self, conn: psycopg.AsyncConnection[Any]) -> int:
        """Reclaim every stale profile claim across all types.

        Called at the top of every ``_schedule_waiting_jobs`` round.
        Sweeping on submission alone (the old behaviour) wasn't enough: if
        a profile run got repurposed mid-flight (e.g., the optimizer
        re-assigned its row to a standard run, so ``is_profiling_run``
        flipped to FALSE), or the owning job finished while ``schedule_job``
        wasn't being called for that type, the claim row sat forever and
        ``remaining`` undercounted that config — permanently blocking
        re-profile attempts.
        """
        return await self._sweep_stale_claims_filtered(conn, type_id=None)

    async def _sweep_stale_claims_filtered(self, conn: psycopg.AsyncConnection[Any], *, type_id: str | None) -> int:
        """Shared body for the scoped and unscoped sweeps.

        A claim row (``duration_seconds IS NULL``) is stale when no live
        profile run owns it.  "Live profile run" means a job that:
          - exists, AND
          - has ``is_profiling_run = TRUE``, AND
          - is in QUEUED (claim written, awaiting dispatch) or PROFILING
            (worker running the measurement) — *not* RUNNING (RUNNING is a
            standard run, so the row was repurposed and the claim is stale).
        Anything else: the claim is garbage and we delete it.
        """
        if type_id is None:
            query = """DELETE FROM profiling_results pr
                       WHERE pr.duration_seconds IS NULL
                         AND (pr.instance_id IS NULL
                              OR NOT EXISTS (
                                  SELECT 1 FROM jobs j
                                  WHERE j.id = pr.instance_id
                                    AND j.is_profiling_run = TRUE
                                    AND j.status IN (%s, %s)
                              ))"""
            params: tuple[Any, ...] = (JobStatus.QUEUED, JobStatus.PROFILING)
            scope_desc = "all types"
        else:
            query = """DELETE FROM profiling_results pr
                       WHERE pr.job_id = %s
                         AND pr.duration_seconds IS NULL
                         AND (pr.instance_id IS NULL
                              OR NOT EXISTS (
                                  SELECT 1 FROM jobs j
                                  WHERE j.id = pr.instance_id
                                    AND j.is_profiling_run = TRUE
                                    AND j.status IN (%s, %s)
                              ))"""
            params = (type_id, JobStatus.QUEUED, JobStatus.PROFILING)
            scope_desc = f"type {type_id}"
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            deleted = getattr(cur, "rowcount", 0) or 0
        if deleted > 0:
            logger.info("Cleared %d stale profile claim(s) (%s)", deleted, scope_desc)
        return deleted

    async def try_preempt_for_profile(
        self,
        conn: psycopg.AsyncConnection[Any],
        instance_id: str,
        type_id: str,
    ) -> tuple[list[str], str | None, dict[str, int] | None]:
        """Find the minimum set of running jobs to evict so this unprofiled
        config can fit on a profiling-capable node.

        Used by the scheduler loop's Phase 1c when a config is still
        unprofiled for a job type and the cluster has no idle slot big enough
        for it.  The user's policy is "profiling always wins": evict the
        cheapest set of low-priority non-profiling jobs on a node whose
        *total* capacity could host the target config, then let the next
        scheduling round dispatch the profile run into the freed slots.

        Returns the **list** of victim instance_ids (possibly empty if no
        feasible eviction).  Multi-victim is required when the target config
        wants more GPUs than any single victim holds — e.g. profiling
        ``A40×2`` while both A40 slots on polimi-gpu are held by separate
        ``A40×1`` jobs needs both jobs evicted.  The earlier single-victim
        version was a silent ceiling on profile coverage: 2-GPU configs
        could never claim slots once 1-GPU configs had each grabbed one.

        HARD RULES (unchanged):
        - Never evict ``status='PROFILING'`` rows (the filter is
          ``status='RUNNING'`` only; defensive guard in
          ``_preempt_and_release`` repeats the check).
        - Only consider nodes whose *total* capacity could fit the target.
        - Pick the cheapest set: lowest priority first, then least progress.
        """
        all_configs = self.get_valid_configurations()
        profiled = await self.get_profiled_configs(conn, type_id)
        profiled_keys = {config_key(c) for c in profiled}
        remaining = [c for c in all_configs if config_key(c) not in profiled_keys]
        if not remaining:
            return [], None, None
        target = remaining[0]

        node_gpu_usage = await self.get_node_gpu_usage(conn)

        # Find the first profile-capable node where evicting enough low-priority
        # jobs covers the shortfall.
        for raw in cluster.nodes:
            node = NodeConfig.model_validate(raw)
            if not node.is_for_profiling:
                continue
            totals = {res.gpu_type: res.gpu_count for res in node.resources}
            if not all(totals.get(gt, 0) >= n for gt, n in target.items()):
                continue
            used = node_gpu_usage.get(node.id, {})
            free = {gt: totals.get(gt, 0) - used.get(gt, 0) for gt in totals}
            need = {gt: max(0, target.get(gt, 0) - free.get(gt, 0)) for gt in target}
            if all(n == 0 for n in need.values()):
                # Already fits — schedule_job should have placed it directly.
                return [], None, None

            async with conn.cursor() as cur:
                await cur.execute(
                    """SELECT id, priority, progress, assigned_gpu_config FROM jobs
                       WHERE assigned_node = %s AND status = %s AND id != %s
                       ORDER BY priority ASC NULLS FIRST, progress ASC NULLS FIRST""",
                    (node.id, JobStatus.RUNNING, instance_id),
                )
                candidates = await cur.fetchall()

            chosen: list[str] = []
            freed = dict.fromkeys(need, 0)
            for cid, _prio, _prog, cfg in candidates:
                cfg = cfg or {}
                # Skip victims that hold no GPU of any still-needed type.
                if not any(cfg.get(gt, 0) > 0 and freed[gt] < need[gt] for gt in need):
                    continue
                chosen.append(str(cid))
                for gt, cnt in cfg.items():
                    if gt in freed:
                        freed[gt] += int(cnt)
                if all(freed[gt] >= need[gt] for gt in need):
                    break

            if all(freed[gt] >= need[gt] for gt in need):
                logger.info(
                    "Profile-preempt: evicting %d victim(s) on %s to free %s for unprofiled %s (target=%s)",
                    len(chosen),
                    node.id,
                    need,
                    type_id,
                    target,
                )
                return chosen, node.id, target

        return [], None, None

    async def _get_in_flight_claims(
        self, conn: psycopg.AsyncConnection[Any], instance_id: str
    ) -> list[tuple[dict[str, int], str]]:
        """Return (gpu_config, node_id) for this instance's outstanding claims.

        An outstanding claim is a ``profiling_results`` row owned by this
        instance whose ``duration_seconds`` is still NULL — i.e., the cell
        has been claimed but the measurement hasn't been recorded.
        """
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT gpu_config, node_id FROM profiling_results WHERE instance_id = %s AND duration_seconds IS NULL",
                (instance_id,),
            )
            rows = await cur.fetchall()
        return [(row[0], row[1]) for row in rows]

    async def _atomic_claim_remaining(
        self,
        conn: psycopg.AsyncConnection[Any],
        type_id: str,
        instance_id: str,
        max_claims: int,
    ) -> list[tuple[dict[str, int], str]]:
        """Atomically claim up to ``max_claims`` unprofiled cells for this instance.

        For each cell, picks the canonically-first profile-capable node with
        enough total capacity (current usage ignored — dispatch will wait on
        the per-node slot semaphore if the node happens to be busy).  The
        claims go into ``profiling_results`` in a single statement with
        ``ON CONFLICT (job_id, gpu_config) DO NOTHING``, so two instances
        of the same type racing to claim the last cell can both call this
        helper safely — the loser just gets nothing back.

        Returns the list of (gpu_config, node_id) the instance actually
        owns after the INSERT.
        """
        if max_claims <= 0:
            return []

        all_configs = self.get_valid_configurations()
        profiled = await self.get_profiled_configs(conn, type_id)
        profiled_keys = {config_key(c) for c in profiled}
        unclaimed = [c for c in all_configs if config_key(c) not in profiled_keys]
        if not unclaimed:
            return []

        # Pair each unclaimed cell with its target node.  Pass empty usage so
        # busy nodes don't disqualify cells — dispatch waits on the slot semaphore.
        unclaimed_with_node: list[tuple[dict[str, int], str]] = []
        for cfg in unclaimed:
            target_node = self._find_node_for_config(cfg, is_for_profiling=True, node_gpu_usage={})
            if target_node is None:
                continue
            unclaimed_with_node.append((cfg, target_node.id))

        # Sort so same-node cells cluster (an instance with budget=2 stays
        # co-located) and order is deterministic across runs.
        unclaimed_with_node.sort(key=lambda x: (x[1], sum(x[0].values()), sorted(x[0].keys())))

        to_claim = unclaimed_with_node[:max_claims]

        if not to_claim:
            return []

        now = datetime.now(UTC)
        placeholders: list[str] = []
        params: list[Any] = []
        for cfg, node_id in to_claim:
            placeholders.append("(%s, %s, %s, %s, %s, NULL, %s)")
            params.extend([str(uuid4()), type_id, instance_id, Json(cfg), node_id, now])

        sql = (
            "INSERT INTO profiling_results "
            "(id, job_id, instance_id, gpu_config, node_id, duration_seconds, created_at) "
            "VALUES " + ", ".join(placeholders) + " "
            "ON CONFLICT (job_id, gpu_config) DO NOTHING "
            "RETURNING gpu_config, node_id"
        )
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        owned = [(row[0], row[1]) for row in rows]
        if len(owned) < len(to_claim):
            logger.info(
                "Instance %s atomic-claimed %d/%d cell(s) for type=%s (others won the race)",
                instance_id[:8],
                len(owned),
                len(to_claim),
                type_id,
            )
        elif owned:
            logger.info(
                "Instance %s atomic-claimed %d cell(s) for type=%s: %s",
                instance_id[:8],
                len(owned),
                type_id,
                [c for c, _ in owned],
            )
        return owned

    async def schedule_job(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        *,
        job_type_id: str | None = None,
    ) -> ScheduleResult:
        """Assign a configuration to *job_id* — profile or standard depending on
        what cells this instance owns or can still claim.
        """
        type_id = job_type_id or job_id
        await self._sweep_stale_claims(conn, type_id)
        node_gpu_usage = await self.get_node_gpu_usage(conn)
        all_configs = self.get_valid_configurations()

        gpu_config: dict[str, int] | None = None
        node: NodeConfig | None = None
        mode = "standard"
        is_profiling_run = False

        if not all_configs:
            logger.warning(
                "No valid configurations found — cluster has %d node(s). "
                "Job %s will run in standard mode without profiling.",
                len(cluster.nodes),
                job_id[:8],
            )

        in_flight = await self._get_in_flight_claims(conn, job_id)
        if in_flight:
            in_flight_sorted = sorted(in_flight, key=lambda x: (sum(x[0].values()), sorted(x[0].keys())))
            for cfg, _claim_node_id in in_flight_sorted:
                candidate_node = self._find_node_for_config(cfg, is_for_profiling=True, node_gpu_usage=node_gpu_usage)
                if candidate_node is not None:
                    gpu_config, node = cfg, candidate_node
                    mode, is_profiling_run = "profiling", True
                    break

        # If no in-flight claim was ready, atomically claim more cells.
        # _count_profiled_this_round counts claimed+completed, so a claim = budget used.
        if not is_profiling_run:
            already_claimed = await self._count_profiled_this_round(conn, job_id)
            budget_left = self.configs_per_job - already_claimed
            if budget_left > 0:
                owned = await self._atomic_claim_remaining(conn, type_id, job_id, budget_left)
                if owned:
                    owned_sorted = sorted(owned, key=lambda x: (sum(x[0].values()), sorted(x[0].keys())))
                    for cfg, _claim_node_id in owned_sorted:
                        candidate_node = self._find_node_for_config(
                            cfg, is_for_profiling=True, node_gpu_usage=node_gpu_usage
                        )
                        if candidate_node is not None:
                            gpu_config, node = cfg, candidate_node
                            mode, is_profiling_run = "profiling", True
                            break

        # Counters read AFTER the claim above so new claims show up.
        profiled = await self.get_profiled_configs(conn, type_id)
        profiled_keys = {config_key(c) for c in profiled}
        remaining = [c for c in all_configs if config_key(c) not in profiled_keys]
        profiled_this_round = await self._count_profiled_this_round(conn, job_id)

        logger.info(
            "Scheduling job %s (type=%s): %d total configs, %d profiled/claimed, %d remaining, %d this instance (limit %d)",
            job_id[:8],
            type_id,
            len(all_configs),
            len(profiled),
            len(remaining),
            profiled_this_round,
            self.configs_per_job,
        )

        result = ScheduleResult(
            mode=mode,
            gpu_config=gpu_config,
            node_id=node.id if node else None,
            is_profiling_run=is_profiling_run,
        )

        await self._persist_assignment(conn, job_id, result, type_id=type_id, instance_id=job_id)

        logger.info(
            "Scheduled job %s: mode=%s, config=%s, node=%s",
            job_id[:8],
            result.mode,
            result.gpu_config,
            result.node_id,
        )
        return result


# Singleton instance (reads PROFILING_CONFIGS_PER_JOB from env at import time)
scheduler: ProfilingScheduler = ProfilingScheduler()

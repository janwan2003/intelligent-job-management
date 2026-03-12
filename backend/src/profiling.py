"""Profiling scheduler for the IJM backend.

Manages incremental GPU configuration profiling before real job execution.
"""

import itertools
import logging
import random
from datetime import UTC, datetime
from typing import Any

import psycopg  # type: ignore[import-not-found]
from psycopg.types.json import Json  # type: ignore[import-not-found]

from src.cluster import cluster
from src.models import NodeConfig, ScheduleResult
from src.utils.gpu import config_key

logger = logging.getLogger(__name__)


class ProfilingScheduler:
    """Incremental profiling strategy scheduler.

    Each submission profiles ONE previously-untested hardware configuration.
    Once all configs have been tested across submissions, the job runs on
    the fastest configuration found so far.
    """

    def get_valid_configurations(self) -> list[dict[str, int]]:
        """Derive all valid GPU configurations from cluster nodes.

        For each node, computes the cartesian product of ``[0..count]`` for
        each resource group, excludes all-zeros, and builds config dicts.
        This naturally handles mixed-GPU nodes (e.g. A40 + L40S).
        Returns a deduplicated list.
        """
        seen: set[str] = set()
        configs: list[dict[str, int]] = []
        for raw in cluster.nodes:
            node = NodeConfig.model_validate(raw)
            if not node.resources:
                continue
            ranges = [range(res.gpu_count + 1) for res in node.resources]
            for combo in itertools.product(*ranges):
                if all(c == 0 for c in combo):
                    continue
                parts = {node.resources[i].gpu_type: combo[i] for i in range(len(node.resources)) if combo[i] > 0}
                key = config_key(parts)
                if key not in seen:
                    seen.add(key)
                    configs.append(parts)
        return configs

    def _find_node_for_config(self, gpu_config: dict[str, int], *, is_for_profiling: bool) -> NodeConfig | None:
        """Find a node that can provide the given config.

        Checks each node has **all** required GPU types with sufficient
        counts.  Prefers smallest total surplus.

        - Standard runs (``is_for_profiling=False``): profiling-designated nodes
          are excluded so production jobs never land on them.
        - Profiling runs (``is_for_profiling=True``): profiling-designated nodes
          are preferred; falls back to any matching node when none can satisfy
          the config (e.g. large configs that exceed the profiling node's capacity).
        """
        required = gpu_config
        candidates: list[tuple[NodeConfig, int]] = []  # (node, total_surplus)
        for raw in cluster.nodes:
            node = NodeConfig.model_validate(raw)
            available = {res.gpu_type: res.gpu_count for res in node.resources}
            total_surplus = 0
            match = True
            for gpu_type, needed in required.items():
                avail = available.get(gpu_type, 0)
                if avail < needed:
                    match = False
                    break
                total_surplus += avail - needed
            if match:
                candidates.append((node, total_surplus))

        if not is_for_profiling:
            # Standard runs must never use the profiling-designated node
            candidates = [(n, s) for n, s in candidates if not n.is_for_profiling]
        else:
            # Profiling runs prefer the profiling-designated node
            profiling_preferred = [(n, s) for n, s in candidates if n.is_for_profiling]
            if profiling_preferred:
                candidates = profiling_preferred

        candidates.sort(key=lambda pair: pair[1])
        return candidates[0][0] if candidates else None

    def _find_any_available_node(self) -> NodeConfig | None:
        """Find any non-profiling node with at least one GPU group."""
        for raw in cluster.nodes:
            node = NodeConfig.model_validate(raw)
            if not node.is_for_profiling and node.resources:
                return node
        return None

    async def get_profiled_configs(self, conn: psycopg.AsyncConnection[Any], job_id: str) -> list[dict[str, int]]:
        """Get configs already profiled for this job."""
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT DISTINCT gpu_config FROM profiling_results WHERE job_id = %s",
                (job_id,),
            )
            rows = await cur.fetchall()
        return [row[0] for row in rows]

    async def _find_best_available_config(
        self, conn: psycopg.AsyncConnection[Any], job_id: str
    ) -> tuple[dict[str, int], NodeConfig, float | None] | None:
        """Find the fastest profiled config that has an available production node.

        Iterates profiling results from fastest to slowest and returns the first
        ``(config, node, duration)`` for which a non-profiling node can be found.
        Returns ``None`` when there are no profiling results or no node is
        currently available for any profiled config.
        """
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT gpu_config, duration_seconds FROM profiling_results "
                "WHERE job_id = %s ORDER BY duration_seconds ASC",
                (job_id,),
            )
            rows = await cur.fetchall()
        for gpu_config, duration in rows:
            node = self._find_node_for_config(gpu_config, is_for_profiling=False)
            if node:
                return gpu_config, node, float(duration) if duration is not None else None
        return None

    async def _estimate_duration(
        self, conn: psycopg.AsyncConnection[Any], job_id: str, gpu_config: dict[str, int]
    ) -> float | None:
        """Estimate duration for a config.  Exact match first, then average."""
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT duration_seconds FROM profiling_results WHERE job_id = %s AND gpu_config = %s::jsonb",
                (job_id, Json(gpu_config)),
            )
            row = await cur.fetchone()
            if row:
                return float(row[0])

            await cur.execute(
                "SELECT AVG(duration_seconds) FROM profiling_results WHERE job_id = %s",
                (job_id,),
            )
            row = await cur.fetchone()
            return float(row[0]) if row and row[0] is not None else None

    async def _persist_assignment(
        self, conn: psycopg.AsyncConnection[Any], job_id: str, result: ScheduleResult
    ) -> None:
        """Write the scheduling decision to the jobs table."""
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE jobs
                   SET assigned_node = %s, assigned_gpu_config = %s,
                       estimated_duration = %s, is_profiling_run = %s, updated_at = %s
                   WHERE id = %s""",
                (
                    result.node_id,
                    Json(result.gpu_config) if result.gpu_config else None,
                    result.estimated_duration,
                    result.is_profiling_run,
                    datetime.now(UTC),
                    job_id,
                ),
            )

    async def schedule_standard_run(self, conn: psycopg.AsyncConnection[Any], job_id: str) -> ScheduleResult:
        """Schedule a standard (non-profiling) run using the best profiled config so far.

        Called after a profiling run completes to immediately transition to real
        execution on the best configuration discovered so far.
        """
        best = await self._find_best_available_config(conn, job_id)
        if best:
            gpu_config, node, eta = best
        else:
            gpu_config, node, eta = None, None, None

        result = ScheduleResult(
            mode="standard",
            gpu_config=gpu_config,
            node_id=node.id if node else None,
            estimated_duration=eta,
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

    async def schedule_job(self, conn: psycopg.AsyncConnection[Any], job_id: str) -> ScheduleResult:
        """Assign a configuration to *job_id* — profiles ONE untested config or runs on best.

        Called on job submission and resume. Picks one un-profiled config for
        profiling, or the best config for a standard run if all are profiled.
        """
        all_configs = self.get_valid_configurations()
        profiled = await self.get_profiled_configs(conn, job_id)
        profiled_keys = {config_key(c) for c in profiled}
        remaining = [c for c in all_configs if config_key(c) not in profiled_keys]

        logger.info(
            "Scheduling job %s: %d total configs, %d profiled, %d remaining",
            job_id[:8],
            len(all_configs),
            len(profiled),
            len(remaining),
        )

        if not all_configs:
            logger.warning(
                "No valid configurations found — cluster has %d node(s). "
                "Job %s will run in standard mode without profiling.",
                len(cluster.nodes),
                job_id[:8],
            )

        gpu_config: dict[str, int] | None
        if remaining:
            # Exploration: pick a random un-profiled config
            gpu_config = random.choice(remaining)
            node = self._find_node_for_config(gpu_config, is_for_profiling=True)
            eta = await self._estimate_duration(conn, job_id, gpu_config)
            result = ScheduleResult(
                mode="profiling",
                gpu_config=gpu_config,
                node_id=node.id if node else None,
                estimated_duration=eta,
                is_profiling_run=True,
            )
        else:
            # All configs profiled — pick the fastest one with an available node
            best = await self._find_best_available_config(conn, job_id)
            if best:
                gpu_config, node, eta = best
            else:
                gpu_config, node, eta = None, None, None
            result = ScheduleResult(
                mode="standard",
                gpu_config=gpu_config,
                node_id=node.id if node else None,
                estimated_duration=eta,
                is_profiling_run=False,
            )

        await self._persist_assignment(conn, job_id, result)

        logger.info(
            "Scheduled job %s: mode=%s, config=%s, node=%s",
            job_id[:8],
            result.mode,
            result.gpu_config,
            result.node_id,
        )
        return result


# Singleton instance
scheduler: ProfilingScheduler = ProfilingScheduler()

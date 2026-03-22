"""Profiling scheduler for the IJM backend.

Manages incremental GPU configuration profiling before real job execution.
"""

import itertools
import logging
import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg  # type: ignore[import-not-found]
from psycopg.types.json import Json  # type: ignore[import-not-found]
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

        Returns a deduplicated list sorted by total GPU count ascending.
        """
        production = self._node_configs(is_for_profiling=False)
        profiling = self._node_configs(is_for_profiling=True)

        configs = [profiling[key] for key in profiling.keys() & production.keys()]
        configs.sort(key=lambda c: sum(c.values()))
        return configs

    async def _get_node_gpu_usage(self, conn: psycopg.AsyncConnection[Any]) -> dict[str, dict[str, int]]:
        """Query allocated GPUs per node from currently running/profiling jobs.

        Returns ``{node_id: {gpu_type: allocated_count, ...}, ...}``.
        """
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT assigned_node, assigned_gpu_config FROM jobs "
                "WHERE status IN (%s, %s) AND assigned_node IS NOT NULL AND assigned_gpu_config IS NOT NULL",
                (JobStatus.RUNNING, JobStatus.PROFILING),
            )
            rows = await cur.fetchall()

        usage: dict[str, dict[str, int]] = {}
        for node_id, gpu_config in rows:
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
        """Find any profiled config that has an available production node.

        Returns the first ``(config, node)`` pair for which a non-profiling
        node can be found, or ``None`` when no profiling results exist or no
        node is currently available for any profiled config.
        """
        profiled = await self.get_profiled_configs(conn, job_id, completed_only=True)
        for gpu_config in profiled:
            node = self._find_node_for_config(gpu_config, is_for_profiling=False, node_gpu_usage=node_gpu_usage)
            if node:
                return gpu_config, node
        return None

    async def _persist_assignment(
        self, conn: psycopg.AsyncConnection[Any], job_id: str, result: ScheduleResult, type_id: str = ""
    ) -> None:
        """Write the scheduling decision to the jobs table.

        For profiling runs, also claim the config in profiling_results
        (duration_seconds = NULL marks it as in-flight).
        """
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
            if result.is_profiling_run and result.gpu_config and result.node_id:
                await cur.execute(
                    """INSERT INTO profiling_results (id, job_id, gpu_config, node_id, duration_seconds, created_at)
                       VALUES (%s, %s, %s, %s, NULL, %s)
                       ON CONFLICT (job_id, gpu_config) DO NOTHING""",
                    (str(uuid4()), type_id or job_id, Json(result.gpu_config), result.node_id, now),
                )

    async def schedule_standard_run(self, conn: psycopg.AsyncConnection[Any], job_id: str) -> ScheduleResult:
        """Schedule a standard (non-profiling) run using any available profiled config.

        Called after a profiling run completes to immediately transition to real
        execution on any configuration that has been measured.
        """
        node_gpu_usage = await self._get_node_gpu_usage(conn)
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

    async def _count_profiled_this_round(self, conn: psycopg.AsyncConnection[Any], job_id: str, type_id: str) -> int:
        """Count how many profiling results were created since this job instance was submitted."""
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT COUNT(*) FROM profiling_results pr
                   JOIN jobs j ON j.id = %s
                   WHERE pr.job_id = %s AND pr.created_at >= j.created_at""",
                (job_id, type_id),
            )
            row = await cur.fetchone()
        return row[0] if row else 0

    async def schedule_job(
        self,
        conn: psycopg.AsyncConnection[Any],
        job_id: str,
        *,
        job_type_id: str | None = None,
    ) -> ScheduleResult:
        """Assign a configuration to *job_id* — profile or run depending on progress.

        Called on job submission, resume, and after each profiling run completes.
        Each submission profiles up to ``configs_per_job`` new configs (counted
        since the job instance's creation time), then schedules a standard run.

        *job_type_id* is the ANDREAS-style type identifier used to correlate
        profiling results across submissions.  Falls back to *job_id* if not provided.
        """
        type_id = job_type_id or job_id
        node_gpu_usage = await self._get_node_gpu_usage(conn)
        all_configs = self.get_valid_configurations()
        profiled = await self.get_profiled_configs(conn, type_id)
        profiled_keys = {config_key(c) for c in profiled}
        remaining = [c for c in all_configs if config_key(c) not in profiled_keys]
        profiled_this_round = await self._count_profiled_this_round(conn, job_id, type_id)

        logger.info(
            "Scheduling job %s (type=%s): %d total configs, %d profiled, %d remaining, %d this round (limit %d)",
            job_id[:8],
            type_id,
            len(all_configs),
            len(profiled),
            len(remaining),
            profiled_this_round,
            self.configs_per_job,
        )

        if not all_configs:
            logger.warning(
                "No valid configurations found — cluster has %d node(s). "
                "Job %s will run in standard mode without profiling.",
                len(cluster.nodes),
                job_id[:8],
            )

        gpu_config: dict[str, int] | None
        if remaining and profiled_this_round < self.configs_per_job:
            # Exploration: pick the smallest un-profiled config (fewest total GPUs)
            gpu_config = remaining[0]
            node = self._find_node_for_config(gpu_config, is_for_profiling=True, node_gpu_usage=node_gpu_usage)
            result = ScheduleResult(
                mode="profiling",
                gpu_config=gpu_config,
                node_id=node.id if node else None,
                is_profiling_run=True,
            )
        else:
            # Enough configs profiled (or none remain) — pick any available one
            available = await self._find_available_config(conn, type_id, node_gpu_usage)
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

        await self._persist_assignment(conn, job_id, result, type_id=type_id)

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

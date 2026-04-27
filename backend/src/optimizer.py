"""Client for the GPUspb external optimizer service.

Translates IJM's active jobs, profiling results, and cluster state into the
format expected by POST /optimizer/v5.  Sends ``currentScheduling`` so the
optimizer can decide whether to preempt running jobs for more urgent ones.
"""

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import psycopg
from shared.constants import DEFAULT_PROFILING_EPOCHS, JobStatus

from src.cluster import cluster
from src.constants import DEFAULT_EPOCHS_TOTAL, DEFAULT_JOB_PRIORITY, PRIORITY_MAX, PRIORITY_MIN
from src.models import NodeConfig

logger = logging.getLogger(__name__)

OPTIMIZER_URL: str | None = os.getenv("OPTIMIZER_URL")

# GPUspb timestamp format (dots in time, not colons)
_TIME_FMT = "%a %d %b %Y, %H.%M.%S"

_PROGRESS_RE = re.compile(r"^(\d+)/(\d+)$")


@dataclass
class Assignment:
    """Optimizer's decision for one job."""

    instance_id: str
    node_id: str
    gpu_config: dict[str, int]


@dataclass
class OptimizerResult:
    """Full result from the optimizer, including preemption decisions."""

    assignments: list[Assignment] = field(default_factory=list)
    preempt: list[str] = field(default_factory=list)  # instance_ids to stop
    estimated_cost: float = 0.0


def _format_time(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.strftime(_TIME_FMT)


def _parse_progress(progress: str | None) -> int:
    """Parse '5/20' → 5 (current epoch). Returns 0 if not parseable."""
    if not progress:
        return 0
    m = _PROGRESS_RE.match(progress)
    return int(m.group(1)) if m else 0


def _build_nodes_payload(
    node_gpu_usage: dict[str, dict[str, int]],
) -> dict[str, dict[str, Any]]:
    """Build the optimizer 'nodes' dict from cluster config and current usage.

    ``free_nGPUs`` reflects actual availability (total minus running jobs).
    The optimizer uses this together with ``currentScheduling`` to decide
    assignments — ``free_nGPUs`` must already subtract currentScheduling usage.
    """
    nodes: dict[str, dict[str, Any]] = {}
    for raw in cluster.nodes:
        node = NodeConfig.model_validate(raw)
        used = node_gpu_usage.get(node.id, {})
        for res in node.resources:
            free = max(0, res.gpu_count - used.get(res.gpu_type, 0))
            entry_id = f"{node.id}_{res.gpu_type}" if len(node.resources) > 1 else node.id
            nodes[entry_id] = {
                "GPUtype": res.gpu_type,
                "free_nGPUs": free,
                "total_nGPUs": res.gpu_count,
                "cost": node.cost,
            }
    return nodes


def _build_node_map(nodes_payload: dict[str, dict[str, Any]]) -> dict[str, tuple[str, str]]:
    """Build reverse mapping: optimizer node ID → (real node ID, gpu_type)."""
    node_map: dict[str, tuple[str, str]] = {}
    real_ids = {raw.get("id") for raw in cluster.nodes}
    for entry_id, entry in nodes_payload.items():
        real_id = entry_id.rsplit("_", 1)[0] if "_" in entry_id else entry_id
        if real_id in real_ids:
            node_map[entry_id] = (real_id, entry["GPUtype"])
    return node_map


def _opt_node_for_real(real_node_id: str, gpu_type: str, nodes_payload: dict[str, dict[str, Any]]) -> str | None:
    """Map a real node_id + gpu_type back to the optimizer's node entry ID."""
    # Direct match
    if real_node_id in nodes_payload and nodes_payload[real_node_id].get("GPUtype") == gpu_type:
        return real_node_id
    # Virtual entry for mixed nodes
    virtual = f"{real_node_id}_{gpu_type}"
    if virtual in nodes_payload:
        return virtual
    return None


async def optimize(
    conn: psycopg.AsyncConnection[Any],
    node_gpu_usage: dict[str, dict[str, int]],
) -> OptimizerResult:
    """Call the optimizer with ALL active jobs and currentScheduling.

    Returns assignments (what should run) and preempt list (what should stop).
    On error, returns empty result so caller falls back to the greedy scheduler.
    """
    if not OPTIMIZER_URL:
        return OptimizerResult()

    # Fetch ALL active jobs (QUEUED + RUNNING + PROFILING on non-profiling nodes)
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT id, job_id, status, priority, deadline, created_at, "
            "epochs_total, profiling_epochs_no, assigned_node, assigned_gpu_config, progress "
            "FROM jobs WHERE status IN (%s, %s, %s) AND is_profiling_run = FALSE "
            "ORDER BY created_at ASC",
            (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.PROFILING),
        )
        active_jobs = await cur.fetchall()

    if not active_jobs:
        return OptimizerResult()

    # Batch-fetch profiling results
    type_ids = list({row[1] for row in active_jobs})
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT job_id, gpu_config, duration_seconds FROM profiling_results "
            "WHERE job_id = ANY(%s) AND duration_seconds IS NOT NULL",
            (type_ids,),
        )
        all_profiling = await cur.fetchall()

    profiling_by_type: dict[str, list[tuple[dict[str, int], float]]] = {}
    for type_id, gpu_config, duration in all_profiling:
        profiling_by_type.setdefault(type_id, []).append((gpu_config, duration))

    nodes_payload = _build_nodes_payload(node_gpu_usage)
    if not nodes_payload:
        return OptimizerResult()

    # Build jobs payload + currentScheduling
    jobs_payload: dict[str, dict[str, Any]] = {}
    current_scheduling: dict[str, dict[str, Any]] = {}

    for row in active_jobs:
        instance_id = row[0]
        job_type_id, status = row[1], row[2]
        priority, deadline, created_at = row[3], row[4], row[5]
        epochs_total, profiling_epochs = row[6], row[7]
        assigned_node, assigned_gpu_config, progress = row[8], row[9], row[10]

        profiling_rows = profiling_by_type.get(job_type_id)
        if not profiling_rows:
            continue  # No profiling data → greedy scheduler handles it

        prof_epochs = profiling_epochs or DEFAULT_PROFILING_EPOCHS
        total_epochs = epochs_total or DEFAULT_EPOCHS_TOTAL

        # For running jobs, use remaining epochs
        current_epoch = _parse_progress(progress) if status == JobStatus.RUNNING else 0
        remaining_epochs = max(1, total_epochs - current_epoch)

        # Build ProfilingData with remaining execution times
        profiling_data: dict[str, dict[str, float]] = {}
        for gpu_config, duration in profiling_rows:
            for gpu_type, num_gpus in gpu_config.items():
                time_per_epoch = duration / prof_epochs
                remaining_time = time_per_epoch * remaining_epochs
                profiling_data.setdefault(gpu_type, {})[str(num_gpus)] = remaining_time

        dl = deadline or created_at.replace(tzinfo=UTC) + timedelta(hours=24)
        jobs_payload[instance_id] = {
            "SubmissionTime": _format_time(created_at),
            "Deadline": _format_time(dl),
            "Priority": max(PRIORITY_MIN, min(PRIORITY_MAX, priority or DEFAULT_JOB_PRIORITY)),
            "Epochs": str(remaining_epochs),
            "ProfilingData": profiling_data,
        }

        # Add running/profiling jobs to currentScheduling
        if status in (JobStatus.RUNNING, JobStatus.PROFILING) and assigned_node and assigned_gpu_config:
            for gpu_type, n_gpus in assigned_gpu_config.items():
                opt_node = _opt_node_for_real(assigned_node, gpu_type, nodes_payload)
                if opt_node:
                    current_scheduling[instance_id] = {
                        "node": opt_node,
                        "GPUtype": gpu_type,
                        "nGPUs": n_gpus,
                    }
                break  # Only first GPU type for currentScheduling entry

    if not jobs_payload:
        return OptimizerResult()

    request_body: dict[str, Any] = {
        "jobs": jobs_payload,
        "nodes": nodes_payload,
        "GPUcosts": cluster.gpu_energy_costs,
        "currentTime": _format_time(datetime.now(UTC)),
        "method": os.getenv("OPTIMIZER_METHOD", "RG"),
    }
    if current_scheduling:
        request_body["currentScheduling"] = current_scheduling

    logger.info(
        "Optimizer request: %d jobs (%d running), %d nodes",
        len(jobs_payload),
        len(current_scheduling),
        len(nodes_payload),
    )

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{OPTIMIZER_URL}/optimizer/v5", json=request_body)
            resp.raise_for_status()
            result = resp.json()
    except Exception:
        logger.exception("Optimizer call failed — falling back to greedy")
        return OptimizerResult()

    # Parse response
    node_map = _build_node_map(nodes_payload)
    assigned_ids: set[str] = set()
    assignments: list[Assignment] = []

    for opt_job_id, opt_assignment in result.get("jobs", {}).items():
        opt_node = opt_assignment.get("node", "")
        n_gpus = opt_assignment.get("nGPUs", 0)
        tardiness = opt_assignment.get("expected_tardiness", 0)
        if tardiness > 0:
            logger.warning("Job %s will miss deadline by %.1f hours", opt_job_id[:8], tardiness)
        if not opt_node or n_gpus <= 0 or opt_node not in node_map:
            continue
        real_node_id, gpu_type = node_map[opt_node]
        assignments.append(Assignment(instance_id=opt_job_id, node_id=real_node_id, gpu_config={gpu_type: n_gpus}))
        assigned_ids.add(opt_job_id)

    # Jobs in currentScheduling but NOT in optimizer response → preempt
    preempt = [job_id for job_id in current_scheduling if job_id not in assigned_ids]

    logger.info(
        "Optimizer: %d assignment(s), %d preemption(s), cost=%.2f",
        len(assignments),
        len(preempt),
        result.get("estimated_cost", 0),
    )
    if preempt:
        logger.info("Optimizer wants to preempt: %s", [jid[:8] for jid in preempt])

    return OptimizerResult(assignments=assignments, preempt=preempt, estimated_cost=result.get("estimated_cost", 0))

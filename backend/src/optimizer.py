"""Client for the GPUspb external optimizer service.

Translates IJM's job queue, profiling results, and cluster state into the
format expected by POST /optimizer/v5, calls the optimizer, and returns
a list of (instance_id, node_id, gpu_config) assignments.
"""

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import psycopg
from shared.constants import JobStatus

from src.cluster import cluster
from src.models import NodeConfig

logger = logging.getLogger(__name__)

OPTIMIZER_URL: str | None = os.getenv("OPTIMIZER_URL")

# GPUspb timestamp format (dots in time, not colons)
_TIME_FMT = "%a %d %b %Y, %H.%M.%S"


@dataclass
class Assignment:
    """Result of the optimizer for one job."""

    instance_id: str
    node_id: str
    gpu_config: dict[str, int]


def _format_time(dt: datetime) -> str:
    """Convert a datetime to the GPUspb time format."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.strftime(_TIME_FMT)


def _build_nodes_payload(
    node_gpu_usage: dict[str, dict[str, int]],
) -> dict[str, dict[str, Any]]:
    """Build the optimizer 'nodes' dict from cluster config and current usage.

    Mixed-GPU nodes are split into separate virtual entries per GPU type
    (e.g. node-mixed-01 with A40+L40S becomes node-mixed-01_A40 and
    node-mixed-01_L40S).
    """
    nodes: dict[str, dict[str, Any]] = {}
    for raw in cluster.nodes:
        node = NodeConfig.model_validate(raw)
        if node.is_for_profiling:
            continue
        used = node_gpu_usage.get(node.id, {})
        for res in node.resources:
            free = res.gpu_count - used.get(res.gpu_type, 0)
            if free <= 0:
                continue
            entry_id = f"{node.id}_{res.gpu_type}" if len(node.resources) > 1 else node.id
            nodes[entry_id] = {
                "GPUtype": res.gpu_type,
                "free_nGPUs": free,
                "total_nGPUs": res.gpu_count,
                "cost": node.cost,
            }
    return nodes


def _build_gpu_costs() -> dict[str, dict[str, float]]:
    """Return GPU costs in the optimizer's format (keys are strings)."""
    return cluster.gpu_energy_costs


def _node_id_from_optimizer(opt_node_id: str) -> str:
    """Map an optimizer node ID back to our real node ID.

    Virtual entries like 'node-mixed-01_A40' → 'node-mixed-01'.
    """
    for raw in cluster.nodes:
        node = NodeConfig.model_validate(raw)
        if node.id == opt_node_id:
            return node.id
        if len(node.resources) > 1:
            for res in node.resources:
                if f"{node.id}_{res.gpu_type}" == opt_node_id:
                    return node.id
    return opt_node_id


def _gpu_type_for_opt_node(opt_node_id: str, nodes_payload: dict[str, dict[str, Any]]) -> str:
    """Look up the GPU type for an optimizer node entry."""
    entry = nodes_payload.get(opt_node_id, {})
    return str(entry.get("GPUtype", ""))


async def optimize(
    conn: psycopg.AsyncConnection[Any],
    node_gpu_usage: dict[str, dict[str, int]],
) -> list[Assignment]:
    """Call the external optimizer for all QUEUED jobs that have profiling data.

    Returns a (possibly empty) list of assignments.  On any error, logs a
    warning and returns [] so the caller can fall back to the greedy scheduler.
    """
    if not OPTIMIZER_URL:
        return []

    # Gather QUEUED jobs with no assigned node
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT id, job_id, priority, deadline, created_at, epochs_total, profiling_epochs_no "
            "FROM jobs WHERE status = %s AND assigned_node IS NULL ORDER BY created_at ASC",
            (JobStatus.QUEUED,),
        )
        queued_jobs = await cur.fetchall()

    if not queued_jobs:
        return []

    # For each job, fetch completed profiling results
    jobs_payload: dict[str, dict[str, Any]] = {}
    job_map: dict[str, tuple[str, int, int]] = {}  # opt_job_id → (instance_id, epochs_total, profiling_epochs)

    for instance_id, job_type_id, priority, deadline, created_at, epochs_total, profiling_epochs in queued_jobs:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT gpu_config, duration_seconds FROM profiling_results "
                "WHERE job_id = %s AND duration_seconds IS NOT NULL",
                (job_type_id,),
            )
            profiling_rows = await cur.fetchall()

        if not profiling_rows:
            continue  # No profiling data yet — skip, greedy scheduler will handle

        # Build ProfilingData: {gpu_type: {num_gpus: total_execution_time}}
        prof_epochs = profiling_epochs or 3
        total_epochs = epochs_total or 20
        profiling_data: dict[str, dict[str, float]] = {}
        for gpu_config, duration in profiling_rows:
            for gpu_type, num_gpus in gpu_config.items():
                # Extrapolate profiling duration to full job
                total_time = (duration / prof_epochs) * total_epochs
                profiling_data.setdefault(gpu_type, {})[str(num_gpus)] = total_time

        # Default deadline: 24h from submission if not set
        dl = deadline or created_at.replace(tzinfo=UTC) + __import__("datetime").timedelta(hours=24)

        jobs_payload[instance_id] = {
            "SubmissionTime": _format_time(created_at),
            "Deadline": _format_time(dl),
            "Priority": max(1, min(5, priority or 3)),
            "Epochs": str(total_epochs),
            "ProfilingData": profiling_data,
        }
        job_map[instance_id] = (instance_id, total_epochs, prof_epochs)

    if not jobs_payload:
        return []

    nodes_payload = _build_nodes_payload(node_gpu_usage)
    if not nodes_payload:
        logger.info("Optimizer: no available nodes, skipping")
        return []

    request_body = {
        "jobs": jobs_payload,
        "nodes": nodes_payload,
        "GPUcosts": _build_gpu_costs(),
        "currentTime": _format_time(datetime.now(UTC)),
        "method": os.getenv("OPTIMIZER_METHOD", "RG"),
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{OPTIMIZER_URL}/optimizer/v5", json=request_body)
            resp.raise_for_status()
            result = resp.json()
    except Exception:
        logger.exception("Optimizer call failed — falling back to greedy")
        return []

    # Parse response
    assignments: list[Assignment] = []
    opt_jobs = result.get("jobs", {})
    for opt_job_id, assignment in opt_jobs.items():
        opt_node = assignment.get("node", "")
        n_gpus = assignment.get("nGPUs", 0)
        if not opt_node or n_gpus <= 0:
            continue

        real_node_id = _node_id_from_optimizer(opt_node)
        gpu_type = _gpu_type_for_opt_node(opt_node, nodes_payload)
        if not gpu_type:
            continue

        assignments.append(
            Assignment(
                instance_id=opt_job_id,
                node_id=real_node_id,
                gpu_config={gpu_type: n_gpus},
            )
        )

    logger.info(
        "Optimizer returned %d assignment(s) for %d job(s) (cost=%.2f)",
        len(assignments),
        len(jobs_payload),
        result.get("estimated_cost", 0),
    )
    return assignments

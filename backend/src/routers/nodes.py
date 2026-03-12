"""Node and GPU configuration endpoints."""

from typing import Any

from fastapi import APIRouter

import src.state as state
from src.cluster import cluster
from src.constants import JobStatus, NodeStatusEnum
from src.models import NodeConfig, NodeStatus
from src.profiling import scheduler
from src.utils.gpu import config_key

router = APIRouter()


@router.get("/nodes", response_model=list[NodeStatus])
async def list_nodes() -> list[NodeStatus]:
    """List all cluster nodes with their current status."""
    async with state.get_conn() as conn:
        # Find which nodes are currently busy (have RUNNING or PROFILING jobs assigned)
        assigned: dict[str, list[str]] = {}
        cur = await conn.execute(
            "SELECT assigned_node, id FROM jobs WHERE status IN (%s, %s) AND assigned_node IS NOT NULL",
            (JobStatus.RUNNING, JobStatus.PROFILING),
        )
        for row in await cur.fetchall():
            assigned.setdefault(row[0], []).append(row[1])

    result: list[NodeStatus] = []
    for node_data in cluster.nodes:
        node = NodeConfig.model_validate(node_data)
        job_ids = assigned.get(node.id, [])
        result.append(
            NodeStatus(
                id=node.id,
                is_for_profiling=node.is_for_profiling,
                cost=node.cost,
                resources=node.resources,
                status=NodeStatusEnum.BUSY if job_ids else NodeStatusEnum.IDLE,
                current_job_ids=job_ids,
            )
        )
    return result


@router.get("/gpu-costs")
async def get_gpu_costs() -> dict[str, dict[str, float]]:
    """Return hourly GPU energy costs used by the scheduler for optimization."""
    return cluster.gpu_energy_costs


@router.get("/configurations")
async def list_configurations() -> list[dict[str, Any]]:
    """List all valid hardware configurations in the cluster."""
    configs = scheduler.get_valid_configurations()
    return [{"gpu_config": c} for c in sorted(configs, key=config_key)]

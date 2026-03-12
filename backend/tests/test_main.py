"""Tests for main application endpoints."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.state as state_module
from src.app import app
from src.cluster import ClusterManager, cluster
from src.models import NodeConfig, NodeResources, _row_to_job
from src.profiling import ProfilingScheduler


@asynccontextmanager
async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Test lifespan that skips database and NATS connections."""
    yield


@pytest.fixture()
def client() -> TestClient:
    """Create a test client with mocked lifespan."""
    app.router.lifespan_context = _noop_lifespan
    # Reset global state so require_db / require_js raise 503
    state_module.db_pool = None
    state_module.js = None
    return TestClient(app)


def test_root(client: TestClient) -> None:
    """Test root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_health_check(client: TestClient) -> None:
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_list_jobs_returns_503_when_db_not_initialized(client: TestClient) -> None:
    """Test that endpoints return 503 when database is not initialized."""
    response = client.get("/jobs")
    assert response.status_code == 503


def test_list_nodes_returns_503_when_db_not_initialized(client: TestClient) -> None:
    """Test that /nodes returns 503 when database is not initialized."""
    response = client.get("/nodes")
    assert response.status_code == 503


def _make_row(
    *,
    job_id: str = "id-123",
    image: str = "image:latest",
    command: list[str] | None = None,
    status: str = "QUEUED",
    progress: str | None = None,
    priority: int = 3,
    deadline: datetime | None = None,
    batch_size: int | None = None,
    epochs_total: int | None = None,
    profiling_epochs_no: int | None = None,
    assigned_node: str | None = None,
    required_memory_gb: int | None = None,
    assigned_gpu_config: dict[str, int] | None = None,
    estimated_duration: float | None = None,
    is_profiling_run: bool = False,
) -> tuple:
    """Helper to build a 19-column row tuple."""
    now = datetime.now(UTC)
    return (
        job_id,
        image,
        command or ["python", "train.py"],
        status,
        now,
        now,
        None,
        None,
        progress,
        priority,
        deadline,
        batch_size,
        epochs_total,
        profiling_epochs_no,
        assigned_node,
        required_memory_gb,
        assigned_gpu_config,
        estimated_duration,
        is_profiling_run,
    )


def test_row_to_job() -> None:
    """Test the row-to-Job conversion helper with full 21-column row."""
    row = _make_row()
    job = _row_to_job(row)
    assert job.id == "id-123"
    assert job.image == "image:latest"
    assert job.command == ["python", "train.py"]
    assert job.status == "QUEUED"
    assert job.container_name is None
    assert job.exit_code is None
    assert job.progress is None
    assert job.priority == 3
    assert job.deadline is None
    assert job.assigned_node is None
    assert job.assigned_gpu_config is None
    assert job.estimated_duration is None
    assert job.is_profiling_run is False


def test_row_to_job_with_progress() -> None:
    """Test the row-to-Job conversion with progress field."""
    row = _make_row(job_id="id-456", image="img:v1", status="RUNNING", progress="150/10000")
    job = _row_to_job(row)
    assert job.status == "RUNNING"
    assert job.progress == "150/10000"


def test_row_to_job_with_extended_fields() -> None:
    """Test row-to-Job with ANDREAS extended fields."""
    dl = datetime.now(UTC)
    row = _make_row(
        priority=5,
        deadline=dl,
        batch_size=2048,
        epochs_total=50,
        profiling_epochs_no=2,
        assigned_node="node-01",
    )
    job = _row_to_job(row)
    assert job.priority == 5
    assert job.deadline == dl
    assert job.batch_size == 2048
    assert job.epochs_total == 50
    assert job.profiling_epochs_no == 2
    assert job.assigned_node == "node-01"


def test_row_to_job_legacy_9_columns() -> None:
    """Test backward compat: old 9-column rows still work."""
    now = datetime.now(UTC)
    row = ("id-old", "img:v0", ["python", "run.py"], "QUEUED", now, now, None, None, None)
    job = _row_to_job(row)
    assert job.id == "id-old"
    assert job.priority == 3  # default
    assert job.deadline is None


def test_cluster_manager_load_nodes(tmp_path: Path) -> None:
    """Test ClusterManager.load_nodes from JSON config."""
    import json

    config_file = tmp_path / "nodes.json"
    config_file.write_text(
        json.dumps(
            [
                {"id": "n1", "isForProfiling": False, "cost": 0.08},
                {"id": "n2", "isForProfiling": True, "cost": 0.12},
            ]
        )
    )

    cm = ClusterManager()
    cm.load_nodes(config_file)
    assert len(cm.nodes) == 2
    assert cm.nodes[0]["id"] == "n1"
    assert cm.nodes[1]["isForProfiling"] is True


def test_cluster_manager_load_gpu_energy_costs(tmp_path: Path) -> None:
    """Test ClusterManager.load_gpu_energy_costs from JSON config."""
    import json

    config_file = tmp_path / "costs.json"
    config_file.write_text(
        json.dumps(
            {
                "A40": {"1": 0.15, "2": 0.28},
                "L40S": {"1": 0.18},
            }
        )
    )

    cm = ClusterManager()
    cm.load_gpu_energy_costs(config_file)
    assert len(cm.gpu_energy_costs) == 2
    assert cm.gpu_energy_costs["A40"]["1"] == 0.15
    assert cm.gpu_energy_costs["L40S"]["1"] == 0.18


def test_cluster_manager_get_profiling_node() -> None:
    """Test ClusterManager.get_profiling_node."""
    cm = ClusterManager()
    cm.nodes = [
        {"id": "node-a40-01", "isForProfiling": False, "cost": 0.15},
        {"id": "node-l40s-01", "isForProfiling": True, "cost": 0.18},
    ]
    prof = cm.get_profiling_node()
    assert prof is not None
    assert prof.id == "node-l40s-01"
    assert prof.is_for_profiling is True


def test_cluster_manager_get_energy_cost() -> None:
    """Test ClusterManager.get_energy_cost lookups."""
    cm = ClusterManager()
    cm.gpu_energy_costs = {
        "A40": {"1": 0.15, "2": 0.28, "4": 0.54},
        "Blackwell": {"1": 0.45, "2": 0.85, "4": 1.60},
    }
    assert cm.get_energy_cost("A40", 1) == 0.15
    assert cm.get_energy_cost("A40", 4) == 0.54
    assert cm.get_energy_cost("Blackwell", 2) == 0.85
    # Unknown GPU type
    assert cm.get_energy_cost("H100", 1) is None
    # Unknown GPU count
    assert cm.get_energy_cost("A40", 8) is None


def test_node_resources_get_available_memory() -> None:
    """Test NodeResources.get_available_memory returns total VRAM."""
    res = NodeResources(gpu_type="A40", gpu_count=4, memory_per_gpu_gb=48)
    assert res.get_available_memory() == 192

    res_single = NodeResources(gpu_type="Blackwell", gpu_count=2, memory_per_gpu_gb=192)
    assert res_single.get_available_memory() == 384


def test_node_config_get_available_memory() -> None:
    """Test NodeConfig.get_available_memory delegates to resources."""
    node_with = NodeConfig.model_validate(
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 4, "memory_per_gpu_gb": 48}],
        }
    )
    assert node_with.get_available_memory() == 192

    node_without = NodeConfig.model_validate({"id": "n2", "isForProfiling": False})
    assert node_without.get_available_memory() == 0


def test_cluster_manager_find_suitable_nodes() -> None:
    """Test find_suitable_nodes filters by memory and excludes profiling nodes."""
    cm = ClusterManager()
    cm.nodes = [
        {
            "id": "node-a40-01",
            "isForProfiling": False,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 4, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "node-l40s-01",
            "isForProfiling": True,
            "cost": 0.18,
            "resources": [{"gpu_type": "L40S", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "node-blackwell-01",
            "isForProfiling": False,
            "cost": 0.45,
            "resources": [{"gpu_type": "Blackwell", "gpu_count": 2, "memory_per_gpu_gb": 192}],
        },
    ]

    # 48 GB: A40 (192 total) and Blackwell (384 total) qualify; L40S is profiling
    suitable = cm.find_suitable_nodes(48)
    ids = [n.id for n in suitable]
    assert "node-a40-01" in ids
    assert "node-blackwell-01" in ids
    assert "node-l40s-01" not in ids

    # 200 GB: only Blackwell qualifies
    suitable = cm.find_suitable_nodes(200)
    assert len(suitable) == 1
    assert suitable[0].id == "node-blackwell-01"

    # 500 GB: nothing qualifies
    assert cm.find_suitable_nodes(500) == []


def test_row_to_job_with_required_memory_gb() -> None:
    """Test row-to-Job includes required_memory_gb field."""
    row = _make_row(required_memory_gb=96)
    job = _row_to_job(row)
    assert job.required_memory_gb == 96


def test_row_to_job_with_profiling_fields() -> None:
    """Test row-to-Job includes profiling scheduler fields."""
    row = _make_row(
        assigned_gpu_config={"A40": 2},
        estimated_duration=123.4,
        is_profiling_run=True,
    )
    job = _row_to_job(row)
    assert job.assigned_gpu_config == {"A40": 2}
    assert job.estimated_duration == 123.4
    assert job.is_profiling_run is True


# ---------------------------------------------------------------------------
# ProfilingScheduler tests
# ---------------------------------------------------------------------------


def test_get_valid_configurations() -> None:
    """Test that all valid configs are generated from cluster nodes."""
    # Set up cluster with single-GPU-type nodes only
    cluster.nodes = [
        {
            "id": "node-a40-01",
            "isForProfiling": False,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 4, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "node-a40-prof",
            "isForProfiling": True,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "node-l40s-01",
            "isForProfiling": False,
            "cost": 0.18,
            "resources": [{"gpu_type": "L40S", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "node-l40s-prof",
            "isForProfiling": True,
            "cost": 0.18,
            "resources": [{"gpu_type": "L40S", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "node-blackwell-01",
            "isForProfiling": False,
            "cost": 0.45,
            "resources": [{"gpu_type": "Blackwell", "gpu_count": 2, "memory_per_gpu_gb": 192}],
        },
        {
            "id": "node-blackwell-prof",
            "isForProfiling": True,
            "cost": 0.45,
            "resources": [{"gpu_type": "Blackwell", "gpu_count": 1, "memory_per_gpu_gb": 192}],
        },
    ]

    sched = ProfilingScheduler()
    configs = sched.get_valid_configurations()

    # A40: 1,2,3,4  L40S: 1,2  Blackwell: 1,2  = 8 configs
    assert len(configs) == 8
    assert {"A40": 1} in configs
    assert {"A40": 4} in configs
    assert {"L40S": 1} in configs
    assert {"L40S": 2} in configs
    assert {"Blackwell": 1} in configs
    assert {"Blackwell": 2} in configs
    # Should NOT contain counts beyond what nodes have
    assert {"L40S": 3} not in configs
    assert {"Blackwell": 4} not in configs


def test_get_valid_configurations_deduplicates() -> None:
    """Test that overlapping nodes produce unique config set."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "n2",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]

    sched = ProfilingScheduler()
    configs = sched.get_valid_configurations()
    # n1 -> A40:1, A40:2;  n2 -> A40:1 (already covered)
    assert len(configs) == 2
    assert {"A40": 1} in configs
    assert {"A40": 2} in configs


def test_find_node_for_config_exact_match() -> None:
    """Prefer node with exact gpu_count match."""
    cluster.nodes = [
        {
            "id": "big",
            "isForProfiling": False,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 4, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "small",
            "isForProfiling": True,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]

    sched = ProfilingScheduler()
    node = sched._find_node_for_config({"A40": 1}, is_for_profiling=True)
    assert node is not None
    assert node.id == "small"  # exact match preferred


def test_find_node_for_config_larger_node() -> None:
    """Fall back to node with more GPUs than needed."""
    cluster.nodes = [
        {
            "id": "big",
            "isForProfiling": False,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 4, "memory_per_gpu_gb": 48}],
        },
    ]

    sched = ProfilingScheduler()
    node = sched._find_node_for_config({"A40": 2}, is_for_profiling=False)
    assert node is not None
    assert node.id == "big"


def test_find_node_for_config_no_match() -> None:
    """Return None when no node can provide the config."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.15,
            "resources": [{"gpu_type": "L40S", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
    ]

    sched = ProfilingScheduler()
    assert sched._find_node_for_config({"A40": 1}, is_for_profiling=False) is None
    assert sched._find_node_for_config({"L40S": 4}, is_for_profiling=False) is None


def test_configurations_endpoint(client: TestClient) -> None:
    """Test GET /configurations returns valid configs."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "n2",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "L40S", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]

    response = client.get("/configurations")
    assert response.status_code == 200
    data = response.json()
    # A40: 1,2  L40S: 1  = 3 configs
    assert len(data) == 3
    configs = [c["gpu_config"] for c in data]
    assert {"A40": 1} in configs
    assert {"A40": 2} in configs
    assert {"L40S": 1} in configs


# ---------------------------------------------------------------------------
# ClusterManager edge cases
# ---------------------------------------------------------------------------


def test_get_profiling_node_returns_none_when_no_profiling_node() -> None:
    """get_profiling_node returns None when no node has isForProfiling=True."""
    cm = ClusterManager()
    cm.nodes = [
        {"id": "n1", "isForProfiling": False, "cost": 0.1},
        {"id": "n2", "isForProfiling": False, "cost": 0.2},
    ]
    assert cm.get_profiling_node() is None


def test_load_nodes_missing_file(tmp_path: Path) -> None:
    """load_nodes logs warning and doesn't crash if config file is missing."""
    cm = ClusterManager()
    cm.load_nodes(tmp_path / "does_not_exist.json")
    assert cm.nodes == []


def test_load_gpu_energy_costs_missing_file(tmp_path: Path) -> None:
    """load_gpu_energy_costs logs warning and doesn't crash if config file is missing."""
    cm = ClusterManager()
    cm.load_gpu_energy_costs(tmp_path / "does_not_exist.json")
    assert cm.gpu_energy_costs == {}


# ---------------------------------------------------------------------------
# ProfilingScheduler — _find_any_available_node
# ---------------------------------------------------------------------------


def test_find_any_available_node_returns_non_profiling() -> None:
    """Should return a non-profiling node with resources."""
    cluster.nodes = [
        {
            "id": "prof",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "compute",
            "isForProfiling": False,
            "cost": 0.2,
            "resources": [{"gpu_type": "L40S", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
    ]
    sched = ProfilingScheduler()
    node = sched._find_any_available_node()
    assert node is not None
    assert node.id == "compute"


def test_find_any_available_node_all_profiling_returns_none() -> None:
    """Returns None when all nodes are for profiling."""
    cluster.nodes = [
        {
            "id": "p1",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "p2",
            "isForProfiling": True,
            "cost": 0.2,
            "resources": [{"gpu_type": "L40S", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]
    sched = ProfilingScheduler()
    assert sched._find_any_available_node() is None


def test_find_any_available_node_skips_nodes_without_resources() -> None:
    """Nodes without resources are skipped."""
    cluster.nodes = [
        {"id": "no-res", "isForProfiling": False, "cost": 0.1},
        {
            "id": "has-res",
            "isForProfiling": False,
            "cost": 0.2,
            "resources": [{"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
    ]
    sched = ProfilingScheduler()
    node = sched._find_any_available_node()
    assert node is not None
    assert node.id == "has-res"


# ---------------------------------------------------------------------------
# ProfilingScheduler — find_node_for_config with multiple candidates
# ---------------------------------------------------------------------------


def test_find_node_for_config_prefers_smallest_surplus() -> None:
    """Among multiple matching nodes, prefer the one with smallest GPU surplus."""
    cluster.nodes = [
        {
            "id": "big",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 8, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "medium",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 4, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "small",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
    ]
    sched = ProfilingScheduler()
    # Requesting 2 GPUs — "small" has exactly 2 (surplus 0)
    node = sched._find_node_for_config({"A40": 2}, is_for_profiling=False)
    assert node is not None
    assert node.id == "small"

    # Requesting 3 GPUs — "medium" has 4 (surplus 1), "big" has 8 (surplus 5)
    node = sched._find_node_for_config({"A40": 3}, is_for_profiling=False)
    assert node is not None
    assert node.id == "medium"


def test_get_valid_configurations_empty_cluster() -> None:
    """Empty cluster produces no configurations."""
    cluster.nodes = []
    sched = ProfilingScheduler()
    assert sched.get_valid_configurations() == []


def test_get_valid_configurations_nodes_without_resources() -> None:
    """Nodes without resources are skipped."""
    cluster.nodes = [
        {"id": "n1", "isForProfiling": False, "cost": 0.1},
    ]
    sched = ProfilingScheduler()
    assert sched.get_valid_configurations() == []


# ---------------------------------------------------------------------------
# Mixed-GPU node tests
# ---------------------------------------------------------------------------


def test_mixed_gpu_node_get_available_memory() -> None:
    """NodeConfig with multiple GPU groups sums total VRAM."""
    node = NodeConfig.model_validate(
        {
            "id": "mixed",
            "isForProfiling": False,
            "cost": 0.2,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48},
                {"gpu_type": "L40S", "gpu_count": 2, "memory_per_gpu_gb": 48},
            ],
        }
    )
    # 2*48 + 2*48 = 192
    assert node.get_available_memory() == 192


def test_mixed_gpu_node_valid_configurations() -> None:
    """Mixed-GPU node generates cartesian product of all GPU group combos."""
    cluster.nodes = [
        {
            "id": "mixed-01",
            "isForProfiling": False,
            "cost": 0.2,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48},
                {"gpu_type": "L40S", "gpu_count": 3, "memory_per_gpu_gb": 48},
            ],
        },
    ]
    sched = ProfilingScheduler()
    configs = sched.get_valid_configurations()
    # Cartesian product of [0,1,2] x [0,1,2,3] minus (0,0) = 11 configs
    assert len(configs) == 11
    assert {"A40": 1} in configs
    assert {"A40": 2} in configs
    assert {"L40S": 3} in configs
    assert {"A40": 1, "L40S": 1} in configs
    assert {"A40": 2, "L40S": 3} in configs


def test_find_node_for_config_mixed_gpu() -> None:
    """_find_node_for_config finds mixed node for both pure and mixed configs."""
    cluster.nodes = [
        {
            "id": "mixed",
            "isForProfiling": False,
            "cost": 0.2,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48},
                {"gpu_type": "Blackwell", "gpu_count": 1, "memory_per_gpu_gb": 192},
            ],
        },
    ]
    sched = ProfilingScheduler()
    node = sched._find_node_for_config({"A40": 1}, is_for_profiling=False)
    assert node is not None
    assert node.id == "mixed"
    node = sched._find_node_for_config({"Blackwell": 1}, is_for_profiling=False)
    assert node is not None
    assert node.id == "mixed"
    node = sched._find_node_for_config({"A40": 1, "Blackwell": 1}, is_for_profiling=False)
    assert node is not None
    assert node.id == "mixed"
    assert sched._find_node_for_config({"Blackwell": 2}, is_for_profiling=False) is None
    assert sched._find_node_for_config({"A40": 3}, is_for_profiling=False) is None


def test_find_suitable_nodes_mixed_gpu() -> None:
    """find_suitable_nodes uses total memory across all GPU groups."""
    cm = ClusterManager()
    cm.nodes = [
        {
            "id": "mixed",
            "isForProfiling": False,
            "cost": 0.2,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48},
                {"gpu_type": "L40S", "gpu_count": 1, "memory_per_gpu_gb": 48},
            ],
        },
    ]
    assert len(cm.find_suitable_nodes(100)) == 1
    assert len(cm.find_suitable_nodes(144)) == 1
    assert len(cm.find_suitable_nodes(145)) == 0


def test_node_status_resources_is_list(client: TestClient) -> None:
    """GET /nodes returns resources as a list of GPU groups."""
    cluster.nodes = [
        {
            "id": "mixed-01",
            "isForProfiling": False,
            "cost": 0.2,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48},
                {"gpu_type": "Blackwell", "gpu_count": 1, "memory_per_gpu_gb": 192},
            ],
        },
    ]

    fake_cursor = AsyncMock()
    fake_cursor.fetchall.return_value = []
    fake_cursor.__aenter__ = AsyncMock(return_value=fake_cursor)
    fake_cursor.__aexit__ = AsyncMock(return_value=None)
    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    state_module.db_pool = fake_conn

    response = client.get("/nodes")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    node = data[0]
    assert len(node["resources"]) == 2
    assert node["resources"][0]["gpu_type"] == "A40"
    assert node["resources"][1]["gpu_type"] == "Blackwell"

    state_module.db_pool = None


# ---------------------------------------------------------------------------
# ProfilingScheduler — async methods (DB-dependent)
# ---------------------------------------------------------------------------


class FakeAsyncCursor:
    """Async cursor that returns query-pattern-based responses."""

    def __init__(self, responses: dict[str, list[tuple[Any, ...]]] | None = None) -> None:
        self._responses = responses or {}
        self._last_query = ""
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        self.queries.append((query, params))
        self._last_query = query

    async def fetchone(self) -> tuple[Any, ...] | None:
        rows = self._get_rows()
        return rows[0] if rows else None

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._get_rows()

    def _get_rows(self) -> list[tuple[Any, ...]]:
        for pattern, rows in self._responses.items():
            if pattern in self._last_query:
                return rows
        return []

    async def __aenter__(self) -> "FakeAsyncCursor":
        return self

    async def __aexit__(self, *_args: object) -> None:
        pass


class FakeAsyncConn:
    """Async connection that uses FakeAsyncCursor."""

    def __init__(self, responses: dict[str, list[tuple[Any, ...]]] | None = None) -> None:
        self._cursor = FakeAsyncCursor(responses)

    def cursor(self) -> FakeAsyncCursor:
        return self._cursor

    async def commit(self) -> None:
        pass


async def test_find_best_available_config_returns_fastest() -> None:
    """_find_best_available_config returns the fastest config that has an available node."""
    cluster.nodes = [
        {
            "id": "compute",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        }
    ]

    class BestCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            return [({"A40": 2}, 30.0)]

    class BestConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = BestCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    sched = ProfilingScheduler()
    result = await sched._find_best_available_config(BestConn(), "job-123")
    assert result is not None
    config, node, eta = result
    assert config == {"A40": 2}
    assert node.id == "compute"
    assert eta == 30.0


async def test_find_best_available_config_no_results() -> None:
    """_find_best_available_config returns None when no profiling results exist."""
    cluster.nodes = []
    sched = ProfilingScheduler()
    result = await sched._find_best_available_config(FakeAsyncConn(), "job-missing")
    assert result is None


async def test_estimate_duration_exact_match() -> None:
    """_estimate_duration returns exact match when available."""
    conn = FakeAsyncConn(
        responses={
            "profiling_results": [(45.3,)],
        }
    )
    sched = ProfilingScheduler()
    dur = await sched._estimate_duration(conn, "job-1", {"A40": 2})
    assert dur == 45.3


async def test_estimate_duration_average_fallback() -> None:
    """_estimate_duration falls back to average when no exact match."""
    call_count = 0

    class AvgFakeCursor(FakeAsyncCursor):
        async def fetchone(self) -> tuple[Any, ...] | None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None
            return (60.0,)

    class AvgFakeConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = AvgFakeCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = AvgFakeConn()
    sched = ProfilingScheduler()
    dur = await sched._estimate_duration(conn, "job-1", {"L40S": 1})
    assert dur == 60.0


async def test_estimate_duration_no_data() -> None:
    """_estimate_duration returns None when no profiling data at all."""
    call_count = 0

    class EmptyAvgCursor(FakeAsyncCursor):
        async def fetchone(self) -> tuple[Any, ...] | None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None
            return (None,)

    class EmptyAvgConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = EmptyAvgCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = EmptyAvgConn()
    sched = ProfilingScheduler()
    dur = await sched._estimate_duration(conn, "job-1", {"A40": 4})
    assert dur is None


async def test_schedule_job_exploration_mode() -> None:
    """schedule_job picks a random un-profiled config in exploration mode."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
    ]

    conn = FakeAsyncConn()
    sched = ProfilingScheduler()
    result = await sched.schedule_job(conn, "job-new")
    assert result.mode == "profiling"
    assert result.is_profiling_run is True
    assert result.gpu_config in ({"A40": 1}, {"A40": 2})
    assert result.node_id == "n1"


async def test_schedule_job_standard_mode_after_all_profiled() -> None:
    """schedule_job switches to standard mode when all configs are profiled."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]
    call_count = 0

    class StandardCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            if "SELECT DISTINCT" in self._last_query:
                return [({"A40": 1},)]
            if "ORDER BY duration_seconds" in self._last_query:
                return [({"A40": 1}, 42.5)]
            return []

        async def fetchone(self) -> tuple[Any, ...] | None:
            nonlocal call_count
            call_count += 1
            return None

    class StandardConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = StandardCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = StandardConn()
    sched = ProfilingScheduler()
    result = await sched.schedule_job(conn, "job-done")
    assert result.mode == "standard"
    assert result.is_profiling_run is False
    assert result.gpu_config == {"A40": 1}
    assert result.node_id == "n1"
    assert result.estimated_duration == 42.5


async def test_schedule_job_standard_mode_no_available_node() -> None:
    """Standard mode yields no assignment when all profiled configs have no production node.

    Cluster has only a profiling-designated node, so valid configs exist but
    _find_node_for_config(is_for_profiling=False) always returns None.
    """
    cluster.nodes = [
        {
            "id": "prof-only",
            "isForProfiling": True,  # no production nodes in this cluster
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]

    class NoProductionNodeCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            if "SELECT DISTINCT" in self._last_query:
                # Mark the only valid config as already profiled
                return [({"A40": 1},)]
            if "ORDER BY duration_seconds" in self._last_query:
                return [({"A40": 1}, 30.0)]
            return []

        async def fetchone(self) -> tuple[Any, ...] | None:
            return None

    class NoProductionNodeConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = NoProductionNodeCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = NoProductionNodeConn()
    sched = ProfilingScheduler()
    result = await sched.schedule_job(conn, "job-fb")
    assert result.mode == "standard"
    assert result.is_profiling_run is False
    assert result.node_id is None
    assert result.gpu_config is None


# ---------------------------------------------------------------------------
# Profiling-first enforcement tests
# ---------------------------------------------------------------------------


async def test_one_config_per_submit_then_standard() -> None:
    """Each schedule_job profiles ONE config, then schedule_standard_run goes to real run."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48}],
        },
        {
            "id": "n2",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "L40S", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]

    profiled: list[tuple[dict[str, int]]] = []

    class IncrementalCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            if "SELECT DISTINCT" in self._last_query:
                return list(profiled)
            if "ORDER BY duration_seconds" in self._last_query:
                return [(config, 30.0) for (config,) in profiled]
            return []

        async def fetchone(self) -> tuple[Any, ...] | None:
            # Used only by _estimate_duration in the profiling exploration branch
            if "duration_seconds" in self._last_query:
                return (30.0,) if profiled else None
            return None

    class IncrementalConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = IncrementalCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = IncrementalConn()
    sched = ProfilingScheduler()

    all_configs = sched.get_valid_configurations()
    assert len(all_configs) == 3

    for i in range(3):
        result = await sched.schedule_job(conn, f"job-{i}")
        assert result.is_profiling_run is True
        assert result.mode == "profiling"
        assert result.gpu_config is not None
        profiled.append((result.gpu_config,))

        std = await sched.schedule_standard_run(conn, f"job-{i}")
        assert std.is_profiling_run is False
        assert std.mode == "standard"

    result = await sched.schedule_job(conn, "job-final")
    assert result.is_profiling_run is False
    assert result.mode == "standard"


async def test_profiling_skipped_when_all_configs_already_tested() -> None:
    """If all configs were profiled, schedule_job returns standard mode."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]

    class AlreadyProfiledCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            if "SELECT DISTINCT" in self._last_query:
                return [({"A40": 1},)]
            if "ORDER BY duration_seconds" in self._last_query:
                return [({"A40": 1}, 25.0)]
            return []

        async def fetchone(self) -> tuple[Any, ...] | None:
            return None

    class AlreadyProfiledConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = AlreadyProfiledCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = AlreadyProfiledConn()
    sched = ProfilingScheduler()
    result = await sched.schedule_job(conn, "job-resubmit")
    assert result.is_profiling_run is False
    assert result.mode == "standard"
    assert result.gpu_config == {"A40": 1}


async def test_profiling_explores_all_configs_exhaustively() -> None:
    """Verify that profiling visits every configuration exactly once."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48},
                {"gpu_type": "L40S", "gpu_count": 1, "memory_per_gpu_gb": 48},
            ],
        },
    ]

    profiled: list[dict[str, int]] = []

    class ExhaustiveCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            if "SELECT DISTINCT" in self._last_query:
                return [(c,) for c in profiled]
            return []

        async def fetchone(self) -> tuple[Any, ...] | None:
            return None

    class ExhaustiveConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = ExhaustiveCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = ExhaustiveConn()
    sched = ProfilingScheduler()
    all_configs = sched.get_valid_configurations()

    for _ in range(len(all_configs)):
        result = await sched.schedule_job(conn, "job-exh")
        assert result.is_profiling_run is True
        config = result.gpu_config
        assert config is not None
        assert config not in profiled, f"Config {config} was already profiled — duplicate!"
        profiled.append(config)
    assert len(profiled) == len(all_configs)


def test_require_js_returns_503_when_not_initialized(client: TestClient) -> None:
    """POST /jobs returns 503 when NATS is not initialized."""
    response = client.post("/jobs", json={"image": "test", "command": ["python"]})
    assert response.status_code == 503

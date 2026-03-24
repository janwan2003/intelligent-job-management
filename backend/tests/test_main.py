"""Tests for main application endpoints."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.state as state_module
from src.app import app
from src.cluster import ClusterManager, cluster
from src.models import Job
from src.profiling import ProfilingScheduler

# Save original get_conn before any tests can override it
_original_get_conn = state_module.get_conn


@asynccontextmanager
async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Test lifespan that skips database connections."""
    yield


@pytest.fixture()
def client() -> TestClient:
    """Create a test client with mocked lifespan."""
    app.router.lifespan_context = _noop_lifespan
    # Reset global state so get_conn / require_runner raise 503
    state_module.pool = None
    state_module.job_runner = None
    state_module.get_conn = _original_get_conn
    return TestClient(app)


def test_root(client: TestClient) -> None:
    """Test root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_health_check_degraded_when_not_connected(client: TestClient) -> None:
    """Test health check returns degraded when DB and job runner are not connected."""
    response = client.get("/health")
    assert response.status_code == 503
    data = response.json()
    assert data["status"] == "degraded"
    assert data["database"] == "unavailable"
    assert data["job_runner"] == "unavailable"


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
    id: str = "id-123",
    job_id: str = "test-job",
    image: str = "image:latest",
    command: list[str] | None = None,
    script_path: str | None = None,
    directory_to_mount: str | None = None,
    status: str = "QUEUED",
    progress: str | None = None,
    priority: int = 3,
    deadline: datetime | None = None,
    batch_size: int | None = None,
    epochs_total: int | None = None,
    profiling_epochs_no: int | None = None,
    assigned_node: str | None = None,
    assigned_gpu_config: dict[str, int] | None = None,
    is_profiling_run: bool = False,
) -> dict[str, Any]:
    """Helper to build a job row dict (matching dict_row format)."""
    now = datetime.now(UTC)
    return {
        "id": id,
        "job_id": job_id,
        "image": image,
        "command": command or ["python", "train.py"],
        "script_path": script_path,
        "directory_to_mount": directory_to_mount,
        "status": status,
        "created_at": now,
        "updated_at": now,
        "container_name": None,
        "exit_code": None,
        "progress": progress,
        "priority": priority,
        "deadline": deadline,
        "batch_size": batch_size,
        "epochs_total": epochs_total,
        "profiling_epochs_no": profiling_epochs_no,
        "assigned_node": assigned_node,
        "assigned_gpu_config": assigned_gpu_config,
        "is_profiling_run": is_profiling_run,
    }


def test_job_model_validate() -> None:
    """Test the row-to-Job conversion helper with full 21-column row."""
    row = _make_row()
    job = Job.model_validate(row)
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
    assert job.is_profiling_run is False


def test_job_model_validate_with_progress() -> None:
    """Test the row-to-Job conversion with progress field."""
    row = _make_row(id="id-456", image="img:v1", status="RUNNING", progress="150/10000")
    job = Job.model_validate(row)
    assert job.status == "RUNNING"
    assert job.progress == "150/10000"


def test_job_model_validate_with_extended_fields() -> None:
    """Test row-to-Job with extended fields."""
    dl = datetime.now(UTC)
    row = _make_row(
        priority=5,
        deadline=dl,
        batch_size=2048,
        epochs_total=50,
        profiling_epochs_no=2,
        assigned_node="node-01",
    )
    job = Job.model_validate(row)
    assert job.priority == 5
    assert job.deadline == dl
    assert job.batch_size == 2048
    assert job.epochs_total == 50
    assert job.profiling_epochs_no == 2
    assert job.assigned_node == "node-01"


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


def test_job_model_validate_with_profiling_fields() -> None:
    """Test row-to-Job includes profiling scheduler fields."""
    row = _make_row(
        assigned_gpu_config={"A40": 2},
        is_profiling_run=True,
    )
    job = Job.model_validate(row)
    assert job.assigned_gpu_config == {"A40": 2}
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
            "resources": [{"gpu_type": "A40", "gpu_count": 4}],
        },
        {
            "id": "node-a40-prof",
            "isForProfiling": True,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
        },
        {
            "id": "node-l40s-01",
            "isForProfiling": False,
            "cost": 0.18,
            "resources": [{"gpu_type": "L40S", "gpu_count": 2}],
        },
        {
            "id": "node-l40s-prof",
            "isForProfiling": True,
            "cost": 0.18,
            "resources": [{"gpu_type": "L40S", "gpu_count": 1}],
        },
        {
            "id": "node-blackwell-01",
            "isForProfiling": False,
            "cost": 0.45,
            "resources": [{"gpu_type": "Blackwell", "gpu_count": 2}],
        },
        {
            "id": "node-blackwell-prof",
            "isForProfiling": True,
            "cost": 0.45,
            "resources": [{"gpu_type": "Blackwell", "gpu_count": 1}],
        },
    ]

    sched = ProfilingScheduler()
    configs = sched.get_valid_configurations()

    # Only profiling nodes contribute: A40:1, L40S:1, Blackwell:1 = 3 configs
    assert len(configs) == 3
    assert {"A40": 1} in configs
    assert {"L40S": 1} in configs
    assert {"Blackwell": 1} in configs
    # Production-node-only configs must NOT appear
    assert {"A40": 4} not in configs
    assert {"L40S": 2} not in configs
    assert {"Blackwell": 2} not in configs


def test_get_valid_configurations_deduplicates() -> None:
    """Test that overlapping nodes produce unique config set."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2}],
        },
        {
            "id": "n2",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
        },
    ]

    sched = ProfilingScheduler()
    configs = sched.get_valid_configurations()
    # only profiling node n2 contributes: A40:1
    assert len(configs) == 1
    assert {"A40": 1} in configs


def test_find_node_for_config_exact_match() -> None:
    """Prefer node with exact gpu_count match."""
    cluster.nodes = [
        {
            "id": "big",
            "isForProfiling": False,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 4}],
        },
        {
            "id": "small",
            "isForProfiling": True,
            "cost": 0.15,
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
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
            "resources": [{"gpu_type": "A40", "gpu_count": 4}],
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
            "resources": [{"gpu_type": "L40S", "gpu_count": 2}],
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
            "resources": [{"gpu_type": "A40", "gpu_count": 2}],
        },
        {
            "id": "n2",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
        },
    ]

    response = client.get("/configurations")
    assert response.status_code == 200
    data = response.json()
    # profiling node n2 contributes A40:1; intersects with production node n1 (A40:1,2)
    assert len(data) == 1
    configs = [c["gpu_config"] for c in data]
    assert {"A40": 1} in configs


# ---------------------------------------------------------------------------
# ClusterManager edge cases
# ---------------------------------------------------------------------------


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
# ProfilingScheduler — find_node_for_config with multiple candidates
# ---------------------------------------------------------------------------


def test_find_node_for_config_prefers_smallest_surplus() -> None:
    """Among multiple matching nodes, prefer the one with smallest GPU surplus."""
    cluster.nodes = [
        {
            "id": "big",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 8}],
        },
        {
            "id": "medium",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 4}],
        },
        {
            "id": "small",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2}],
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


def test_mixed_gpu_node_valid_configurations() -> None:
    """Mixed-GPU node generates cartesian product of all GPU group combos."""
    cluster.nodes = [
        {
            "id": "mixed-01",
            "isForProfiling": True,
            "cost": 0.2,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2},
                {"gpu_type": "L40S", "gpu_count": 3},
            ],
        },
        {
            "id": "prod-01",
            "isForProfiling": False,
            "cost": 0.3,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2},
                {"gpu_type": "L40S", "gpu_count": 3},
            ],
        },
    ]
    sched = ProfilingScheduler()
    configs = sched.get_valid_configurations()
    # Cartesian product of [0,1,2] x [0,1,2,3] minus (0,0) = 11 configs; all intersect with prod node
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
                {"gpu_type": "A40", "gpu_count": 2},
                {"gpu_type": "Blackwell", "gpu_count": 1},
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


def test_node_status_resources_is_list(client: TestClient) -> None:
    """GET /nodes returns resources as a list of GPU groups."""
    cluster.nodes = [
        {
            "id": "mixed-01",
            "isForProfiling": False,
            "cost": 0.2,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2},
                {"gpu_type": "Blackwell", "gpu_count": 1},
            ],
        },
    ]

    fake_result = AsyncMock()
    fake_result.fetchall = AsyncMock(return_value=[])
    fake_conn = AsyncMock()
    fake_conn.execute = AsyncMock(return_value=fake_result)
    original_get_conn = state_module.get_conn

    @asynccontextmanager
    async def mock_get_conn() -> AsyncGenerator[Any]:
        yield fake_conn

    state_module.get_conn = mock_get_conn

    response = client.get("/nodes")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    node = data[0]
    assert len(node["resources"]) == 2
    assert node["resources"][0]["gpu_type"] == "A40"
    assert node["resources"][1]["gpu_type"] == "Blackwell"

    state_module.get_conn = original_get_conn


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


async def test_schedule_job_exploration_mode() -> None:
    """schedule_job picks a random un-profiled config in exploration mode."""
    cluster.nodes = [
        {
            "id": "prod",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2}],
        },
        {
            "id": "prof",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
        },
    ]

    conn = FakeAsyncConn()
    sched = ProfilingScheduler()
    result = await sched.schedule_job(conn, "job-new")
    assert result.mode == "profiling"
    assert result.is_profiling_run is True
    assert result.gpu_config == {"A40": 1}
    assert result.node_id == "prof"


async def test_schedule_job_standard_mode_after_all_profiled() -> None:
    """schedule_job switches to standard mode when all configs are profiled."""
    cluster.nodes = [
        {
            "id": "n1",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
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


async def test_schedule_job_standard_mode_profiling_node_can_run_standard() -> None:
    """With only a profiling-designated node, standard runs still land on it.

    All nodes can run standard jobs (isForProfiling only gates profiling runs).
    """
    cluster.nodes = [
        {
            "id": "prof-only",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
        },
    ]

    class ProfiledCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            if "SELECT DISTINCT" in self._last_query:
                return [({"A40": 1},)]
            if "ORDER BY duration_seconds" in self._last_query:
                return [({"A40": 1}, 30.0)]
            return []

        async def fetchone(self) -> tuple[Any, ...] | None:
            return None

    class ProfiledConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = ProfiledCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = ProfiledConn()
    sched = ProfilingScheduler()
    result = await sched.schedule_job(conn, "job-fb")
    assert result.mode == "standard"
    assert result.is_profiling_run is False
    assert result.node_id == "prof-only"
    assert result.gpu_config == {"A40": 1}


async def test_schedule_job_skips_fully_allocated_nodes() -> None:
    """schedule_job must prefer a node with remaining GPU capacity over a fully allocated one."""
    cluster.nodes = [
        {
            "id": "full-node",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2}],
        },
        {
            "id": "free-node",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 2}],
        },
    ]

    class BusyNodeCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            # GPU usage query — full-node has all 2 A40s allocated
            if "assigned_node" in self._last_query and "assigned_gpu_config" in self._last_query:
                return [("full-node", {"A40": 2})]
            # profiling results query — all profiled
            if "SELECT DISTINCT" in self._last_query:
                return [({"A40": 1},), ({"A40": 2},)]
            return []

        async def fetchone(self) -> tuple[Any, ...] | None:
            return None

    class BusyNodeConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = BusyNodeCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = BusyNodeConn()
    sched = ProfilingScheduler()
    result = await sched.schedule_job(conn, "job-x")
    assert result.mode == "standard"
    assert result.node_id == "free-node"


async def test_schedule_job_packs_onto_partially_used_node() -> None:
    """schedule_job should assign to a node that still has remaining GPUs."""
    cluster.nodes = [
        {
            "id": "partial-node",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 4}],
        },
    ]

    class PartialCursor(FakeAsyncCursor):
        async def fetchall(self) -> list[tuple[Any, ...]]:
            # GPU usage query — 2 of 4 A40s already allocated
            if "assigned_node" in self._last_query and "assigned_gpu_config" in self._last_query:
                return [("partial-node", {"A40": 2})]
            # profiling results — config {"A40": 1} profiled
            if "SELECT DISTINCT" in self._last_query:
                return [({"A40": 1},)]
            return []

        async def fetchone(self) -> tuple[Any, ...] | None:
            return None

    class PartialConn(FakeAsyncConn):
        def __init__(self) -> None:
            self._cursor = PartialCursor()

        def cursor(self) -> FakeAsyncCursor:
            return self._cursor

    conn = PartialConn()
    sched = ProfilingScheduler()
    result = await sched.schedule_job(conn, "job-y")
    assert result.mode == "standard"
    assert result.node_id == "partial-node"
    assert result.gpu_config == {"A40": 1}


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
            "resources": [
                {"gpu_type": "A40", "gpu_count": 2},
                {"gpu_type": "L40S", "gpu_count": 2},
            ],
        },
        {
            "id": "n2",
            "isForProfiling": True,
            "cost": 0.1,
            "resources": [
                {"gpu_type": "A40", "gpu_count": 1},
                {"gpu_type": "L40S", "gpu_count": 1},
            ],
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
    sched.configs_per_job = 999  # profile all configs in this test

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
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
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
                {"gpu_type": "A40", "gpu_count": 2},
                {"gpu_type": "L40S", "gpu_count": 1},
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


def test_require_runner_returns_503_when_not_initialized(client: TestClient) -> None:
    """POST /jobs returns 503 when job runner is not initialized."""
    response = client.post("/jobs", json={"job_id": "test", "dockerImage": "test", "command": ["python"]})
    assert response.status_code == 503

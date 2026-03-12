"""Comprehensive tests for job lifecycle: create, stop, resume, delete.

Tests the state machine transitions and validates that invalid transitions
are rejected with 409.  Uses mocked DB (via get_conn) and NATS (js).

Also includes regression tests for multi-job concurrent execution and
profiling-before-running enforcement.
"""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.state as state_module
from src.app import app
from src.cluster import cluster
from src.profiling import ProfilingScheduler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeResult:
    """Cursor-like result returned by FakeConn.execute()."""

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    async def fetchone(self) -> Any | None:
        return self._rows[0] if self._rows else None

    async def fetchall(self) -> list[Any]:
        return self._rows


class FakeCursor:
    """Async cursor with query-pattern-based responses."""

    def __init__(
        self,
        rows: list[Any] | None = None,
        responses: dict[str, list[Any]] | None = None,
    ) -> None:
        self.rows = rows or []
        self._responses = responses or {}
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._last_query = ""

    async def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        self.queries.append((query, params))
        self._last_query = query

    async def fetchone(self) -> Any | None:
        rows = self._resolve()
        return rows[0] if rows else None

    async def fetchall(self) -> list[Any]:
        return self._resolve()

    def _resolve(self) -> list[Any]:
        for pattern, resp_rows in self._responses.items():
            if pattern in self._last_query:
                return resp_rows
        if "profiling_results" in self._last_query:
            return []
        return self.rows

    async def __aenter__(self) -> "FakeCursor":
        return self

    async def __aexit__(self, *_args: object) -> None:
        pass


class FakeConn:
    """Minimal async DB connection supporting both execute() and cursor() patterns."""

    def __init__(
        self,
        rows: list[Any] | None = None,
        responses: dict[str, list[Any]] | None = None,
    ) -> None:
        self._rows = rows or []
        self._responses = responses or {}
        self._cursor = FakeCursor(self._rows, self._responses)
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, params: tuple[Any, ...] = ()) -> FakeResult:
        self.queries.append((query, params))
        return FakeResult(self._resolve(query))

    def cursor(self, row_factory: Any = None) -> FakeCursor:
        return self._cursor

    def _resolve(self, query: str) -> list[Any]:
        for pattern, resp_rows in self._responses.items():
            if pattern in query:
                return resp_rows
        if "profiling_results" in query:
            return []
        return self._rows

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        yield

    async def commit(self) -> None:
        pass


def _mock_get_conn(conn: FakeConn) -> Any:
    """Create a get_conn replacement that yields the given FakeConn."""

    @asynccontextmanager
    async def get_conn() -> AsyncGenerator[FakeConn]:
        yield conn

    return get_conn


@asynccontextmanager
async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    yield


def _make_client(
    responses: dict[str, list[Any]] | None = None,
    default_rows: list[Any] | None = None,
) -> tuple[TestClient, FakeConn, AsyncMock]:
    """Create a test client with mocked DB and NATS."""
    app.router.lifespan_context = _noop_lifespan

    fake_conn = FakeConn(default_rows, responses)
    fake_js = AsyncMock()
    fake_js.publish.return_value = MagicMock(seq=1, duplicate=False)

    state_module.get_conn = _mock_get_conn(fake_conn)
    state_module.js = fake_js

    cluster.nodes = [
        {
            "id": "test-node",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
        },
    ]

    client = TestClient(app)
    return client, fake_conn, fake_js


# ---------------------------------------------------------------------------
# Stop endpoint
# ---------------------------------------------------------------------------


class TestStopJob:
    """Tests for POST /jobs/{job_id}/stop."""

    def test_stop_queued_job_sets_preempted_directly(self) -> None:
        """Stopping a QUEUED job should set it to PREEMPTED without NATS."""
        client, _conn, fake_js = _make_client(responses={"RETURNING": [("test-id-1",)]})

        response = client.post("/jobs/test-id-1/stop")
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "stopped"
        fake_js.publish.assert_not_called()

    def test_stop_running_job_publishes_nats(self) -> None:
        """Stopping a RUNNING job should publish stop_requested to NATS."""
        client, _conn, fake_js = _make_client(responses={"SELECT status": [("RUNNING",)]})

        response = client.post("/jobs/test-id-2/stop")
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "stop_requested"
        fake_js.publish.assert_called_once()
        call_args = fake_js.publish.call_args
        assert call_args[0][0] == "jobs.stop_requested"

    def test_stop_nonexistent_job_returns_404(self) -> None:
        """Stopping a job that doesn't exist should return 404."""
        client, _conn, _ = _make_client()

        response = client.post("/jobs/missing-id/stop")
        assert response.status_code == 404

    def test_stop_succeeded_job_returns_409(self) -> None:
        """Cannot stop an already completed job."""
        client, _conn, _ = _make_client(responses={"SELECT status": [("SUCCEEDED",)]})

        response = client.post("/jobs/done-id/stop")
        assert response.status_code == 409
        assert "SUCCEEDED" in response.json()["detail"]

    def test_stop_failed_job_returns_409(self) -> None:
        """Cannot stop a failed job."""
        client, _conn, _ = _make_client(responses={"SELECT status": [("FAILED",)]})

        response = client.post("/jobs/fail-id/stop")
        assert response.status_code == 409

    def test_stop_profiling_job_publishes_nats(self) -> None:
        """Stopping a PROFILING job should publish stop_requested to NATS."""
        client, _conn, fake_js = _make_client(responses={"SELECT status": [("PROFILING",)]})

        response = client.post("/jobs/prof-id/stop")
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "stop_requested"
        fake_js.publish.assert_called_once()
        call_args = fake_js.publish.call_args
        assert call_args[0][0] == "jobs.stop_requested"

    def test_stop_preempted_job_returns_409(self) -> None:
        """Cannot stop an already preempted job."""
        client, _conn, _ = _make_client(responses={"SELECT status": [("PREEMPTED",)]})

        response = client.post("/jobs/preempt-id/stop")
        assert response.status_code == 409


# ---------------------------------------------------------------------------
# Resume endpoint
# ---------------------------------------------------------------------------


class TestResumeJob:
    """Tests for POST /jobs/{job_id}/resume."""

    def test_resume_preempted_job_sets_queued(self) -> None:
        """Resuming a PREEMPTED job should set it to QUEUED and publish."""
        client, _conn, fake_js = _make_client(responses={"RETURNING": [("preempt-id",)]})

        response = client.post("/jobs/preempt-id/resume")
        assert response.status_code == 202
        fake_js.publish.assert_called_once()
        call_args = fake_js.publish.call_args
        assert call_args[0][0] == "jobs.submitted"

    def test_resume_failed_job_sets_queued(self) -> None:
        """Resuming a FAILED job should set it to QUEUED and publish."""
        client, _conn, fake_js = _make_client(responses={"RETURNING": [("fail-id",)]})

        response = client.post("/jobs/fail-id/resume")
        assert response.status_code == 202
        fake_js.publish.assert_called_once()

    def test_resume_nonexistent_job_returns_404(self) -> None:
        client, _conn, _ = _make_client()

        response = client.post("/jobs/missing/resume")
        assert response.status_code == 404

    def test_resume_queued_job_returns_409(self) -> None:
        """Cannot resume a job that is already queued."""
        client, _conn, _ = _make_client(responses={"SELECT status": [("QUEUED",)]})

        response = client.post("/jobs/q-id/resume")
        assert response.status_code == 409
        assert "QUEUED" in response.json()["detail"]

    def test_resume_running_job_returns_409(self) -> None:
        """Cannot resume a job that is currently running."""
        client, _conn, _ = _make_client(responses={"SELECT status": [("RUNNING",)]})

        response = client.post("/jobs/run-id/resume")
        assert response.status_code == 409

    def test_resume_succeeded_job_returns_409(self) -> None:
        """Cannot resume a completed job."""
        client, _conn, _ = _make_client(responses={"SELECT status": [("SUCCEEDED",)]})

        response = client.post("/jobs/done-id/resume")
        assert response.status_code == 409


# ---------------------------------------------------------------------------
# Rapid stop/resume cycling
# ---------------------------------------------------------------------------


class TestRapidStopResume:
    """Test that rapid stop/resume sequences are handled correctly."""

    def test_stop_then_resume_queued(self) -> None:
        """Stop a QUEUED job, then resume it."""
        client, _conn, _ = _make_client(responses={"RETURNING": [("test",)]})
        resp = client.post("/jobs/test/stop")
        assert resp.status_code == 202

        resp = client.post("/jobs/test/resume")
        assert resp.status_code == 202

    def test_stop_resume_stop_resume(self) -> None:
        """Multiple stop/resume cycles should all succeed."""
        client, _conn, _ = _make_client(responses={"RETURNING": [("test",)]})

        resp = client.post("/jobs/test/stop")
        assert resp.status_code == 202

        resp = client.post("/jobs/test/resume")
        assert resp.status_code == 202

        resp = client.post("/jobs/test/stop")
        assert resp.status_code == 202

        resp = client.post("/jobs/test/resume")
        assert resp.status_code == 202

    def test_double_stop_returns_409(self) -> None:
        """Stopping an already stopped job returns 409."""
        conn = FakeConn(responses={"RETURNING": [("test",)]})
        app.router.lifespan_context = _noop_lifespan
        fake_js = AsyncMock()
        fake_js.publish.return_value = MagicMock(seq=1, duplicate=False)
        state_module.get_conn = _mock_get_conn(conn)
        state_module.js = fake_js
        cluster.nodes = [
            {
                "id": "n",
                "isForProfiling": False,
                "cost": 0.1,
                "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
            }
        ]
        client = TestClient(app)

        resp = client.post("/jobs/test/stop")
        assert resp.status_code == 202

        conn._responses = {"SELECT status": [("PREEMPTED",)]}
        resp = client.post("/jobs/test/stop")
        assert resp.status_code == 409

    def test_double_resume_returns_409(self) -> None:
        """Resuming a QUEUED job (already resumed) returns 409."""
        conn = FakeConn(responses={"RETURNING": [("test",)]})
        app.router.lifespan_context = _noop_lifespan
        fake_js = AsyncMock()
        fake_js.publish.return_value = MagicMock(seq=1, duplicate=False)
        state_module.get_conn = _mock_get_conn(conn)
        state_module.js = fake_js
        cluster.nodes = [
            {
                "id": "n",
                "isForProfiling": False,
                "cost": 0.1,
                "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
            }
        ]
        client = TestClient(app)

        resp = client.post("/jobs/test/resume")
        assert resp.status_code == 202

        conn._responses = {"SELECT status": [("QUEUED",)]}
        resp = client.post("/jobs/test/resume")
        assert resp.status_code == 409

    def test_ten_stop_resume_cycles(self) -> None:
        """Ten rapid stop/resume cycles should all work."""
        client, _conn, _ = _make_client(responses={"RETURNING": [("test",)]})

        for i in range(10):
            resp = client.post("/jobs/test/stop")
            assert resp.status_code == 202, f"Stop failed on cycle {i}"

            resp = client.post("/jobs/test/resume")
            assert resp.status_code == 202, f"Resume failed on cycle {i}"


# ---------------------------------------------------------------------------
# Delete endpoint
# ---------------------------------------------------------------------------


class TestDeleteJob:
    """Tests for DELETE /jobs/{job_id}."""

    def test_delete_existing_job(self) -> None:
        client, _conn, _ = _make_client(responses={"SELECT status": [("SUCCEEDED",)]})
        response = client.delete("/jobs/some-id")
        assert response.status_code == 204

    def test_delete_nonexistent_returns_404(self) -> None:
        client, _conn, _ = _make_client()
        response = client.delete("/jobs/missing")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Logs endpoint
# ---------------------------------------------------------------------------


class TestGetJobLogs:
    """Tests for GET /jobs/{job_id}/logs."""

    def test_logs_nonexistent_job_returns_404(self) -> None:
        client, _conn, _ = _make_client()
        response = client.get("/jobs/00000000-0000-0000-0000-000000000000/logs")
        assert response.status_code == 404

    def test_logs_no_file_returns_message(self) -> None:
        client, _conn, _ = _make_client(default_rows=[("some-id",)])
        response = client.get("/jobs/00000000-0000-0000-0000-000000000001/logs")
        assert response.status_code == 200
        assert "No logs available" in response.text

    def test_logs_with_file(self, tmp_path: Any) -> None:
        """When a log file exists, its contents are returned."""
        job_uuid = "12345678-1234-1234-1234-123456789abc"
        client, _conn, _ = _make_client(default_rows=[(job_uuid,)])

        log_dir = tmp_path / "runs" / job_uuid
        log_dir.mkdir(parents=True)
        log_file = log_dir / "output.log"
        log_file.write_text("line 1\nline 2\n")

        with patch("src.routers.jobs.DATA_DIR", tmp_path):
            response = client.get(f"/jobs/{job_uuid}/logs")

        assert response.status_code == 200
        assert "line 1" in response.text
        assert "line 2" in response.text

    def test_logs_invalid_job_id_format_returns_400(self) -> None:
        """Non-UUID job IDs are rejected for path safety."""
        client, _conn, _ = _make_client(default_rows=[("bad-id",)])
        response = client.get("/jobs/not-a-valid-uuid/logs")
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Helper: job row dict
# ---------------------------------------------------------------------------

_CLUSTER_NODES = [
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
]


def _job_row(
    job_id: str = "job-001",
    image: str = "ijm-runtime:dev",
    command: list[str] | None = None,
    status: str = "QUEUED",
    priority: int = 3,
    assigned_node: str | None = None,
    assigned_gpu_config: dict[str, int] | None = None,
    estimated_duration: float | None = None,
    is_profiling_run: bool = False,
) -> dict[str, Any]:
    """Build a job row dict (matching dict_row format)."""
    now = datetime.now(UTC)
    return {
        "id": job_id,
        "image": image,
        "command": command or ["python", "-u", "train.py"],
        "status": status,
        "created_at": now,
        "updated_at": now,
        "container_name": None,
        "exit_code": None,
        "progress": None,
        "priority": priority,
        "deadline": None,
        "batch_size": None,
        "epochs_total": None,
        "profiling_epochs_no": None,
        "assigned_node": assigned_node,
        "required_memory_gb": None,
        "assigned_gpu_config": assigned_gpu_config,
        "estimated_duration": estimated_duration,
        "is_profiling_run": is_profiling_run,
    }


def _make_rich_client(
    db_rows: list[Any] | None = None,
    responses: dict[str, list[Any]] | None = None,
    cluster_nodes: list[dict[str, Any]] | None = None,
) -> tuple[TestClient, FakeConn, AsyncMock]:
    """Create a test client with richer DB mock (pattern-based responses)."""
    app.router.lifespan_context = _noop_lifespan

    fake_conn = FakeConn(db_rows, responses)
    fake_js = AsyncMock()
    fake_js.publish.return_value = MagicMock(seq=1, duplicate=False)

    state_module.get_conn = _mock_get_conn(fake_conn)
    state_module.js = fake_js
    cluster.nodes = cluster_nodes or list(_CLUSTER_NODES)

    client = TestClient(app)
    return client, fake_conn, fake_js


# ---------------------------------------------------------------------------
# Create job endpoint
# ---------------------------------------------------------------------------


class TestCreateJob:
    """Tests for POST /jobs."""

    def test_create_job_minimal(self) -> None:
        """Create a job with just image and command."""
        client, conn, fake_js = _make_rich_client()

        response = client.post("/jobs", json={"image": "my-image:v1", "command": ["python", "run.py"]})
        assert response.status_code == 201
        body = response.json()
        assert body["image"] == "my-image:v1"
        assert body["command"] == ["python", "run.py"]
        assert body["status"] == "QUEUED"
        assert body["priority"] == 3
        assert body["is_profiling_run"] is True
        assert body["assigned_gpu_config"] is not None
        fake_js.publish.assert_called_once()

    def test_create_job_with_extended_fields(self) -> None:
        """Create a job with extended fields."""
        client, _conn, _ = _make_rich_client()

        response = client.post(
            "/jobs",
            json={
                "image": "train:latest",
                "command": ["python", "-u", "train.py"],
                "Priority": 5,
                "batchSize": 2048,
                "epochsTotal": 100,
                "profilingEpochsNo": 3,
            },
        )
        assert response.status_code == 201
        body = response.json()
        assert body["priority"] == 5
        assert body["batch_size"] == 2048
        assert body["epochs_total"] == 100
        assert body["profiling_epochs_no"] == 3

    def test_create_job_missing_image_returns_422(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.post("/jobs", json={"command": ["python", "run.py"]})
        assert response.status_code == 422

    def test_create_job_missing_command_returns_422(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.post("/jobs", json={"image": "my-image:v1"})
        assert response.status_code == 422

    def test_create_job_priority_out_of_range_returns_422(self) -> None:
        client, _conn, _ = _make_rich_client()

        resp = client.post("/jobs", json={"image": "img", "command": ["cmd"], "Priority": 0})
        assert resp.status_code == 422

        resp = client.post("/jobs", json={"image": "img", "command": ["cmd"], "Priority": 6})
        assert resp.status_code == 422

    def test_create_job_past_deadline_returns_422(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.post(
            "/jobs",
            json={"image": "img", "command": ["cmd"], "deadline": "2020-01-01T00:00:00Z"},
        )
        assert response.status_code == 422
        assert "Deadline" in response.json()["detail"]

    def test_create_job_assigns_profiling_config(self) -> None:
        client, conn, _ = _make_rich_client()
        response = client.post("/jobs", json={"image": "img:v1", "command": ["python", "train.py"]})
        assert response.status_code == 201
        body = response.json()
        assert body["is_profiling_run"] is True
        assert body["assigned_gpu_config"] is not None

    def test_create_job_invalid_image_returns_422(self) -> None:
        """Invalid Docker image name returns 422."""
        client, _conn, _ = _make_rich_client()
        response = client.post("/jobs", json={"image": "../evil", "command": ["cmd"]})
        assert response.status_code == 422
        assert "Invalid Docker image" in response.json()["detail"]


# ---------------------------------------------------------------------------
# List / Get job endpoints
# ---------------------------------------------------------------------------


class TestListJobs:
    """Tests for GET /jobs."""

    def test_list_jobs_returns_all(self) -> None:
        rows = [_job_row(job_id="j1", status="QUEUED"), _job_row(job_id="j2", status="RUNNING")]
        client, _conn, _ = _make_rich_client(db_rows=rows)

        response = client.get("/jobs")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        ids = {j["id"] for j in data}
        assert "j1" in ids
        assert "j2" in ids

    def test_list_jobs_empty(self) -> None:
        client, _conn, _ = _make_rich_client(db_rows=[])
        response = client.get("/jobs")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_jobs_pagination(self) -> None:
        """Pagination params are accepted."""
        client, _conn, _ = _make_rich_client(db_rows=[])
        response = client.get("/jobs?limit=10&offset=5")
        assert response.status_code == 200


class TestGetJob:
    """Tests for GET /jobs/{job_id}."""

    def test_get_job_found(self) -> None:
        row = _job_row(job_id="j-abc", status="RUNNING", priority=4)
        client, _conn, _ = _make_rich_client(db_rows=[row])

        response = client.get("/jobs/j-abc")
        assert response.status_code == 200
        body = response.json()
        assert body["id"] == "j-abc"
        assert body["status"] == "RUNNING"
        assert body["priority"] == 4

    def test_get_job_not_found(self) -> None:
        client, _conn, _ = _make_rich_client(db_rows=[])
        response = client.get("/jobs/nonexistent")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# List nodes endpoint
# ---------------------------------------------------------------------------


class TestListNodes:
    """Tests for GET /nodes."""

    def test_list_nodes_shows_all_cluster_nodes(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.get("/nodes")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        ids = {n["id"] for n in data}
        assert "node-a40-01" in ids
        assert "node-l40s-01" in ids

    def test_list_nodes_marks_busy_when_running_job(self) -> None:
        client, conn, _ = _make_rich_client(
            responses={"assigned_node": [("node-a40-01", "job-xyz")]},
        )
        response = client.get("/nodes")
        assert response.status_code == 200
        data = response.json()

        a40 = next(n for n in data if n["id"] == "node-a40-01")
        l40s = next(n for n in data if n["id"] == "node-l40s-01")
        assert a40["status"] == "busy"
        assert a40["current_job_ids"] == ["job-xyz"]
        assert l40s["status"] == "idle"
        assert l40s["current_job_ids"] == []

    def test_list_nodes_includes_resources(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.get("/nodes")
        data = response.json()
        a40 = next(n for n in data if n["id"] == "node-a40-01")
        assert len(a40["resources"]) == 1
        assert a40["resources"][0]["gpu_type"] == "A40"
        assert a40["resources"][0]["gpu_count"] == 4
        assert a40["resources"][0]["memory_per_gpu_gb"] == 48

    def test_list_nodes_includes_profiling_flag(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.get("/nodes")
        data = response.json()
        a40 = next(n for n in data if n["id"] == "node-a40-01")
        l40s = next(n for n in data if n["id"] == "node-l40s-01")
        assert a40["is_for_profiling"] is False
        assert l40s["is_for_profiling"] is True


# ---------------------------------------------------------------------------
# GPU costs endpoint
# ---------------------------------------------------------------------------


class TestGetGpuCosts:
    """Tests for GET /gpu-costs."""

    def test_get_gpu_costs(self) -> None:
        client, _conn, _ = _make_rich_client()
        cluster.gpu_energy_costs = {"A40": {"1": 0.15, "4": 0.54}, "L40S": {"1": 0.18, "2": 0.34}}
        response = client.get("/gpu-costs")
        assert response.status_code == 200
        data = response.json()
        assert data["A40"]["1"] == 0.15
        assert data["L40S"]["2"] == 0.34

    def test_get_gpu_costs_empty(self) -> None:
        client, _conn, _ = _make_rich_client()
        cluster.gpu_energy_costs = {}
        response = client.get("/gpu-costs")
        assert response.status_code == 200
        assert response.json() == {}


# ---------------------------------------------------------------------------
# Profiling results endpoint
# ---------------------------------------------------------------------------


class TestGetProfilingResults:
    """Tests for GET /profiling-results/{job_id}."""

    def test_profiling_results_empty(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.get("/profiling-results/job-123")
        assert response.status_code == 200
        assert response.json() == []

    def test_profiling_results_returns_data(self) -> None:
        now = datetime.now(UTC)
        client, _conn, _ = _make_rich_client(
            responses={
                "profiling_results": [
                    ("r1", {"A40": 1}, "node-a40-01", 30.5, now),
                    ("r2", {"L40S": 2}, "node-l40s-01", 45.2, now),
                ],
            }
        )
        response = client.get("/profiling-results/job-123")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["gpu_config"] == {"A40": 1}
        assert data[0]["duration_seconds"] == 30.5
        assert data[1]["gpu_config"] == {"L40S": 2}


# ---------------------------------------------------------------------------
# Complex lifecycle scenarios
# ---------------------------------------------------------------------------


class TestComplexLifecycle:
    """End-to-end scenarios exercising multiple endpoints in sequence."""

    def test_create_then_stop_queued(self) -> None:
        """Create a job, then immediately stop it while still QUEUED."""
        conn = FakeConn(responses={"RETURNING": [("some-id",)]})
        fake_js = AsyncMock()
        fake_js.publish.return_value = MagicMock(seq=1, duplicate=False)
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(conn)
        state_module.js = fake_js
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs", json={"image": "img", "command": ["cmd"]})
        assert resp.status_code == 201
        job_id = resp.json()["id"]

        resp = client.post(f"/jobs/{job_id}/stop")
        assert resp.status_code == 202
        assert resp.json()["status"] == "stopped"

    def test_create_stop_resume_full_cycle(self) -> None:
        """Create → stop → resume → verify NATS calls."""
        conn = FakeConn(responses={"RETURNING": [("some-id",)]})
        fake_js = AsyncMock()
        fake_js.publish.return_value = MagicMock(seq=1, duplicate=False)
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(conn)
        state_module.js = fake_js
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs", json={"image": "img", "command": ["cmd"]})
        assert resp.status_code == 201
        job_id = resp.json()["id"]
        assert fake_js.publish.call_count == 1

        resp = client.post(f"/jobs/{job_id}/stop")
        assert resp.status_code == 202
        assert fake_js.publish.call_count == 1  # QUEUED→PREEMPTED, no NATS

        resp = client.post(f"/jobs/{job_id}/resume")
        assert resp.status_code == 202
        assert fake_js.publish.call_count == 2

    def test_resume_preserves_profiling_results(self) -> None:
        """Resume should NOT delete profiling results."""
        client, conn, fake_js = _make_client(responses={"RETURNING": [("job-xyz",)]})

        resp = client.post("/jobs/job-xyz/resume")
        assert resp.status_code == 202

        delete_queries = [q for q, _p in conn.queries if "DELETE" in q and "profiling_results" in q]
        assert len(delete_queries) == 0

    def test_delete_cascades_profiling_results(self) -> None:
        """Deleting a job should also delete its profiling results."""
        client, conn, _ = _make_client(responses={"SELECT status": [("SUCCEEDED",)]})

        resp = client.delete("/jobs/job-del")
        assert resp.status_code == 204

        queries_str = [q for q, _p in conn.queries]
        profiling_deletes = [q for q in queries_str if "DELETE" in q and "profiling_results" in q]
        job_deletes = [q for q in queries_str if "DELETE" in q and "profiling_results" not in q and "jobs" in q]
        assert len(profiling_deletes) >= 1
        assert len(job_deletes) >= 1

    def test_stop_all_terminal_statuses_return_409(self) -> None:
        for status in ("SUCCEEDED", "FAILED", "PREEMPTED"):
            client, _conn, _ = _make_client(responses={"SELECT status": [(status,)]})
            resp = client.post("/jobs/test-id/stop")
            assert resp.status_code == 409, f"Expected 409 for status {status}, got {resp.status_code}"

    def test_resume_non_resumable_statuses_return_409(self) -> None:
        for status in ("QUEUED", "RUNNING", "SUCCEEDED"):
            client, _conn, _ = _make_client(responses={"SELECT status": [(status,)]})
            resp = client.post("/jobs/test-id/resume")
            assert resp.status_code == 409, f"Expected 409 for status {status}, got {resp.status_code}"

    def test_create_job_nats_payload_contains_job_id(self) -> None:
        client, _conn, fake_js = _make_rich_client()
        resp = client.post("/jobs", json={"image": "img", "command": ["cmd"]})
        assert resp.status_code == 201
        job_id = resp.json()["id"]

        publish_call = fake_js.publish.call_args
        nats_data = json.loads(publish_call[0][1].decode())
        assert nats_data["job_id"] == job_id

    def test_stop_running_nats_payload_contains_job_id(self) -> None:
        client, _conn, fake_js = _make_client(responses={"SELECT status": [("RUNNING",)]})
        resp = client.post("/jobs/run-job-123/stop")
        assert resp.status_code == 202

        publish_call = fake_js.publish.call_args
        nats_data = json.loads(publish_call[0][1].decode())
        assert nats_data["job_id"] == "run-job-123"


# ---------------------------------------------------------------------------
# _require_js edge case — DB is set but NATS is not
# ---------------------------------------------------------------------------


class TestRequireJs:
    """Test that endpoints needing NATS return 503 when js is None."""

    def test_create_job_returns_503_without_nats(self) -> None:
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(FakeConn())
        state_module.js = None
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs", json={"image": "img", "command": ["cmd"]})
        assert resp.status_code == 503
        assert "NATS" in resp.json()["detail"]

    def test_stop_running_returns_503_without_nats(self) -> None:
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(FakeConn(responses={"SELECT status": [("RUNNING",)]}))
        state_module.js = None
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs/test/stop")
        assert resp.status_code == 503

    def test_resume_returns_503_without_nats(self) -> None:
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(FakeConn(responses={"RETURNING": [("test",)]}))
        state_module.js = None
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs/test/resume")
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Regression: multi-job submission, concurrent running, profiling-before-running
# ---------------------------------------------------------------------------


_REAL_CLUSTER_NODES = [
    {
        "id": "node-a40-01",
        "isForProfiling": False,
        "cost": 0.15,
        "resources": [{"gpu_type": "A40", "gpu_count": 4, "memory_per_gpu_gb": 48}],
    },
    {
        "id": "node-mixed-01",
        "isForProfiling": False,
        "cost": 0.20,
        "resources": [
            {"gpu_type": "A40", "gpu_count": 2, "memory_per_gpu_gb": 48},
            {"gpu_type": "L40S", "gpu_count": 2, "memory_per_gpu_gb": 48},
        ],
    },
    {
        "id": "node-l40s-01",
        "isForProfiling": False,
        "cost": 0.18,
        "resources": [{"gpu_type": "L40S", "gpu_count": 2, "memory_per_gpu_gb": 48}],
    },
    {
        "id": "node-blackwell-01",
        "isForProfiling": False,
        "cost": 0.45,
        "resources": [{"gpu_type": "Blackwell", "gpu_count": 2, "memory_per_gpu_gb": 192}],
    },
    {
        "id": "node-a40-prof",
        "isForProfiling": True,
        "cost": 0.15,
        "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
    },
    {
        "id": "node-mixed-prof",
        "isForProfiling": True,
        "cost": 0.25,
        "resources": [
            {"gpu_type": "L40S", "gpu_count": 1, "memory_per_gpu_gb": 48},
            {"gpu_type": "Blackwell", "gpu_count": 1, "memory_per_gpu_gb": 192},
        ],
    },
]


class TestMultiJobRegression:
    """Regression tests for multi-job submission."""

    def test_submit_many_jobs_all_get_profiling_assignment(self) -> None:
        client, conn, fake_js = _make_rich_client(cluster_nodes=_REAL_CLUSTER_NODES)

        for i in range(6):
            resp = client.post(
                "/jobs",
                json={"image": f"train-img:v{i}", "command": ["python", "-u", "train.py"]},
            )
            assert resp.status_code == 201, f"Job {i} creation failed: {resp.json()}"
            body = resp.json()
            assert body["is_profiling_run"] is True
            assert body["assigned_node"] is not None
            assert body["assigned_gpu_config"] is not None

        assert fake_js.publish.call_count == 6

    def test_all_nats_events_published_for_submitted_jobs(self) -> None:
        client, _conn, fake_js = _make_rich_client(cluster_nodes=_REAL_CLUSTER_NODES)

        job_ids: list[str] = []
        for _i in range(4):
            resp = client.post("/jobs", json={"image": "img:latest", "command": ["python", "train.py"]})
            assert resp.status_code == 201
            job_ids.append(resp.json()["id"])

        published_ids: set[str] = set()
        for call in fake_js.publish.call_args_list:
            payload = json.loads(call[0][1].decode())
            published_ids.add(payload["job_id"])

        for jid in job_ids:
            assert jid in published_ids

    def test_profiling_before_running_invariant(self) -> None:
        sched = ProfilingScheduler()
        cluster.nodes = _REAL_CLUSTER_NODES
        all_configs = sched.get_valid_configurations()
        assert len(all_configs) >= 8

    async def test_one_config_per_submission_then_standard(self) -> None:
        """Simulate the one-at-a-time profiling cycle."""
        cluster.nodes = _REAL_CLUSTER_NODES
        sched = ProfilingScheduler()
        all_configs = sched.get_valid_configurations()

        profiled: list[tuple[dict[str, int]]] = []

        class CycleCursor(FakeCursor):
            def _resolve(self) -> list[Any]:
                if "SELECT DISTINCT" in self._last_query:
                    return list(profiled)
                if "ORDER BY duration_seconds" in self._last_query:
                    return [(config, 30.0) for (config,) in profiled]
                if "duration_seconds" in self._last_query:
                    return [(30.0,)] if profiled else []
                return []

        class CycleConn(FakeConn):
            def __init__(self) -> None:
                super().__init__()
                self._cursor = CycleCursor()

        conn = CycleConn()

        for _i in range(len(all_configs)):
            result = await sched.schedule_job(conn, "job-regression")
            assert result.is_profiling_run is True
            config = result.gpu_config
            assert config is not None
            assert (config,) not in profiled
            profiled.append((config,))

            std_result = await sched.schedule_standard_run(conn, "job-regression")
            assert std_result.is_profiling_run is False
            assert std_result.mode == "standard"

        result = await sched.schedule_job(conn, "job-regression")
        assert result.is_profiling_run is False
        assert result.mode == "standard"

    async def test_multiple_jobs_independent_profiling_cycles(self) -> None:
        """Different jobs have independent profiling state."""
        cluster.nodes = [
            {
                "id": "n1",
                "isForProfiling": False,
                "cost": 0.1,
                "resources": [{"gpu_type": "A40", "gpu_count": 1, "memory_per_gpu_gb": 48}],
            },
        ]
        sched = ProfilingScheduler()

        class EmptyCursor(FakeCursor):
            def _resolve(self) -> list[Any]:
                return []

        class EmptyConn(FakeConn):
            def __init__(self) -> None:
                super().__init__()
                self._cursor = EmptyCursor()

        conn_a = EmptyConn()
        result_a = await sched.schedule_job(conn_a, "job-A")
        assert result_a.is_profiling_run is True

        conn_b = EmptyConn()
        result_b = await sched.schedule_job(conn_b, "job-B")
        assert result_b.is_profiling_run is True

        class ProfiledCursor(FakeCursor):
            def _resolve(self) -> list[Any]:
                if "SELECT DISTINCT" in self._last_query:
                    return [({"A40": 1},)]
                if "ORDER BY duration_seconds" in self._last_query:
                    return [({"A40": 1}, 25.0)]
                if "duration_seconds" in self._last_query:
                    return [(25.0,)]
                return []

        class ProfiledConn(FakeConn):
            def __init__(self) -> None:
                super().__init__()
                self._cursor = ProfiledCursor()

        conn_c = ProfiledConn()
        result_c = await sched.schedule_job(conn_c, "job-C")
        assert result_c.is_profiling_run is False

    def test_stop_profiling_job_does_not_block_others(self) -> None:
        from src.constants import STOPPABLE_STATUSES, JobStatus

        assert JobStatus.PROFILING in STOPPABLE_STATUSES

        client, _conn, fake_js = _make_client(responses={"SELECT status": [("PROFILING",)]})
        resp = client.post("/jobs/prof-job/stop")
        assert resp.status_code == 202
        fake_js.publish.assert_called_once()

    def test_resume_preserves_profiling_and_profiles_next(self) -> None:
        client, conn, fake_js = _make_rich_client(
            responses={"RETURNING": [("resume-job",)]},
            cluster_nodes=_REAL_CLUSTER_NODES,
        )

        resp = client.post("/jobs/resume-job/resume")
        assert resp.status_code == 202

        delete_queries = [q for q, _p in conn.queries if "DELETE" in q and "profiling_results" in q]
        assert len(delete_queries) == 0

        nats_calls = [c for c in fake_js.publish.call_args_list if c[0][0] == "jobs.submitted"]
        assert len(nats_calls) >= 1

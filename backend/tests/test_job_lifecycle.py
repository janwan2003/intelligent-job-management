"""Comprehensive tests for job lifecycle: create, stop, resume, delete.

Tests the state machine transitions and validates that invalid transitions
are rejected with 409.  Uses mocked DB (via get_conn) and job_runner.

Also includes regression tests for multi-job concurrent execution and
profiling-before-running enforcement.
"""

import contextlib
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

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


# --- in-memory model of the profiling_results claim flow -------------------
# The scheduler derives ``is_profiling_run`` from whether an instance owns an
# unmeasured ``profiling_results`` claim, so the fake DB has to round-trip the
# claim INSERT and the in-flight / profiled / count SELECTs instead of stubbing
# every profiling_results query to ``[]``.


def _pr_ck(cfg: dict[str, int] | None) -> tuple[tuple[str, int], ...]:
    return tuple(sorted((cfg or {}).items()))


def _pr_unwrap(v: Any) -> Any:
    """Unwrap a psycopg ``Json(...)`` param to its underlying dict."""
    return getattr(v, "obj", v)


def _pr_insert(store: list[dict[str, Any]], params: list[Any]) -> list[Any]:
    """Model ``INSERT INTO profiling_results ... ON CONFLICT (job_id, gpu_config)
    DO NOTHING RETURNING gpu_config, node_id`` (six params per row)."""
    inserted: list[Any] = []
    for i in range(0, len(params), 6):
        chunk = params[i : i + 6]
        if len(chunk) < 6:
            break
        _id, job_id, instance_id, cfg_param, node_id, _created = chunk
        cfg = _pr_unwrap(cfg_param)
        if any(r["job_id"] == job_id and _pr_ck(r["gpu_config"]) == _pr_ck(cfg) for r in store):
            continue  # ON CONFLICT DO NOTHING
        store.append(
            {
                "job_id": job_id,
                "instance_id": instance_id,
                "gpu_config": cfg,
                "node_id": node_id,
                "duration_seconds": None,
            }
        )
        inserted.append((cfg, node_id))
    return inserted


def _pr_select(store: list[dict[str, Any]], query: str, params: tuple[Any, ...]) -> list[Any]:
    """Model the profiling_results SELECTs the scheduler issues."""
    if "JOIN" in query:  # cross-instance usage join — single-instance tests don't need it
        return []
    if "COUNT(*)" in query:  # _count_profiled_this_round
        inst = params[0] if params else None
        return [(sum(1 for r in store if r["instance_id"] == inst),)]
    if "DISTINCT gpu_config" in query:  # get_profiled_configs (claimed or completed)
        jid = params[0] if params else None
        seen: set[Any] = set()
        out: list[Any] = []
        for r in store:
            key = _pr_ck(r["gpu_config"])
            if r["job_id"] == jid and key not in seen:
                seen.add(key)
                out.append((r["gpu_config"],))
        return out
    if "duration_seconds IS NULL" in query and "instance_id" in query:  # _get_in_flight_claims
        inst = params[0] if params else None
        return [
            (r["gpu_config"], r["node_id"]) for r in store if r["instance_id"] == inst and r["duration_seconds"] is None
        ]
    return []


# --- in-memory model of the jobs table -------------------------------------
# create -> stop -> resume -> delete must transition *real* state for the
# TestComplexLifecycle scenarios, so the fake persists a jobs table keyed by id
# and replays the exact query shapes the refactored endpoints issue.

_JOB_COLUMNS = (
    "id",
    "job_id",
    "image",
    "command",
    "script_path",
    "directory_to_mount",
    "status",
    "created_at",
    "updated_at",
    "priority",
    "deadline",
    "batch_size",
    "epochs_total",
    "profiling_epochs_no",
)


def _jobs_is_jobs_query(query: str) -> bool:
    """True iff *query* touches the jobs table (and not profiling_results)."""
    return " jobs" in query and "profiling_results" not in query


def _jobs_is_dml(query: str) -> bool:
    """True for jobs statements that mutate the in-memory table."""
    head = query.lstrip()[:6].upper()
    return head in ("INSERT", "UPDATE", "DELETE")


def _jobs_table_backed(jobs: dict[str, dict[str, Any]], query: str) -> bool:
    """Whether to satisfy *query* from the in-memory jobs table.

    DML always mutates the table.  Reads use the table only once it holds rows
    (created via the create_job path); otherwise the caller falls back to the
    injected ``default_rows`` mechanism the read-only tests rely on.
    """
    return _jobs_is_dml(query) or bool(jobs)


def _jobs_insert(jobs: dict[str, dict[str, Any]], params: tuple[Any, ...]) -> None:
    """Model the 14-param ``INSERT INTO jobs (...)`` from create_job."""
    row = dict(zip(_JOB_COLUMNS, params, strict=True))
    # ``command`` arrives as a json.dumps(...) string — store the decoded list.
    cmd = row["command"]
    if isinstance(cmd, str):
        with contextlib.suppress(TypeError, ValueError):
            row["command"] = json.loads(cmd)
    row.setdefault("container_name", None)
    row.setdefault("exit_code", None)
    row.setdefault("progress", None)
    row["assigned_node"] = None
    row["assigned_gpu_config"] = None
    row["is_profiling_run"] = False
    jobs[row["id"]] = row


def _jobs_execute(jobs: dict[str, dict[str, Any]], query: str, params: tuple[Any, ...]) -> list[Any]:
    """Apply a jobs query against the in-memory table, returning result rows.

    Tuple-row results for the targeted SELECT/UPDATE shapes; dict rows are
    produced separately by the cursor when ``SELECT *`` is read with a
    dict_row factory (see ``_jobs_select_star``).
    """
    if "INSERT INTO jobs" in query:
        _jobs_insert(jobs, params)
        return []

    if "UPDATE jobs" in query and "SET assigned_node" in query:
        # scheduler._persist_assignment: assigned_node, gpu_config, is_profiling_run, updated_at, id
        node_id, gpu_cfg, is_prof, updated, jid = params
        row = jobs.get(jid)
        if row is not None:
            row["assigned_node"] = node_id
            row["assigned_gpu_config"] = _pr_unwrap(gpu_cfg)
            row["is_profiling_run"] = is_prof
            row["updated_at"] = updated
        return []

    if "UPDATE jobs SET status" in query and "RETURNING" in query:
        # resume: status, updated_at, id, status-list ; RETURNING id, job_id
        new_status, _updated, jid = params[0], params[1], params[2]
        allowed = params[3]
        row = jobs.get(jid)
        if row is not None and row["status"] in allowed:
            row["status"] = new_status
            row["assigned_node"] = None
            row["assigned_gpu_config"] = None
            return [(row["id"], row["job_id"])]
        return []

    if "UPDATE jobs SET status" in query:
        # stop: status, updated_at, id
        new_status, _updated, jid = params
        row = jobs.get(jid)
        if row is not None:
            row["status"] = new_status
        return []

    if "SELECT status FROM jobs" in query:
        jid = params[0] if params else None
        row = jobs.get(jid)
        return [(row["status"],)] if row is not None else []

    if "SELECT id, assigned_node, assigned_gpu_config FROM jobs" in query:
        # get_node_gpu_usage.  By established fake contract, GPU usage is empty
        # unless a test injects it via a `responses` override (the GPU-accounting
        # tests do exactly that) — so freshly-created QUEUED+assigned rows are
        # NOT auto-counted as cluster usage here.
        return []

    if "SELECT assigned_node, id FROM jobs" in query:
        # list_nodes busy check — overridden via `responses` when a test cares
        # (see test_list_nodes_marks_busy_when_running_job); empty otherwise.
        return []

    if "SELECT id, assigned_node FROM jobs" in query:
        # logs endpoint
        jid = params[0] if params else None
        row = jobs.get(jid)
        return [(row["id"], row["assigned_node"])] if row is not None else []

    if "SELECT id FROM jobs WHERE status" in query:
        # clear-all active ids
        statuses = set(params[0]) if params else set()
        return [(r["id"],) for r in jobs.values() if r["status"] in statuses]

    if "DELETE FROM jobs WHERE id" in query:
        jid = params[0] if params else None
        jobs.pop(jid, None)
        return []

    if query.strip() == "DELETE FROM jobs":
        jobs.clear()
        return []

    return []


def _jobs_select_star(jobs: dict[str, dict[str, Any]], query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    """Model ``SELECT * FROM jobs ...`` reads (dict rows for Job.model_validate)."""
    if "WHERE id" in query:
        jid = params[0] if params else None
        row = jobs.get(jid)
        return [dict(row)] if row is not None else []
    # list_jobs: ORDER BY created_at DESC LIMIT %s OFFSET %s
    ordered = sorted(jobs.values(), key=lambda r: r["created_at"], reverse=True)
    limit = params[0] if len(params) > 0 else len(ordered)
    offset = params[1] if len(params) > 1 else 0
    return [dict(r) for r in ordered[offset : offset + limit]]


class FakeCursor:
    """Async cursor with query-pattern-based responses.

    Dispatch precedence (shared with ``FakeConn.execute``): injected
    ``responses`` always win, then the in-memory jobs table, then the
    profiling_results store, then the plain ``rows`` fallback.
    """

    def __init__(
        self,
        rows: list[Any] | None = None,
        responses: dict[str, list[Any]] | None = None,
        pr_store: list[dict[str, Any]] | None = None,
        jobs: dict[str, dict[str, Any]] | None = None,
        conn_queries: list[tuple[str, tuple[Any, ...]]] | None = None,
    ) -> None:
        self.rows = rows or []
        self._responses = responses or {}
        self._pr_store = pr_store if pr_store is not None else []
        self._jobs = jobs if jobs is not None else {}
        # Shared with the owning FakeConn so cursor-path queries (e.g. the
        # DELETE statements inside delete_job's transaction) are visible to
        # tests that inspect ``conn.queries``.
        self._conn_queries = conn_queries
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._last_query = ""
        self._last_params: tuple[Any, ...] = ()
        self._row_factory: Any = None
        self._returning: list[Any] | None = None

    async def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        self.queries.append((query, params))
        if self._conn_queries is not None:
            self._conn_queries.append((query, params))
        self._last_query = query
        self._last_params = params
        # Injected responses take precedence; don't mutate any store for them.
        if any(pattern in query for pattern in self._responses):
            self._returning = None
            return
        if "profiling_results" in query and "INSERT" in query.upper():
            self._returning = _pr_insert(self._pr_store, list(params))
        elif _jobs_is_jobs_query(query) and "SELECT *" not in query and _jobs_table_backed(self._jobs, query):
            self._returning = _jobs_execute(self._jobs, query, params)
        else:
            self._returning = None

    async def fetchone(self) -> Any | None:
        rows = self._resolve()
        return rows[0] if rows else None

    async def fetchall(self) -> list[Any]:
        return self._resolve()

    def _resolve(self) -> list[Any]:
        for pattern, resp_rows in self._responses.items():
            if pattern in self._last_query:
                return resp_rows
        if _jobs_is_jobs_query(self._last_query):
            if "SELECT *" in self._last_query:
                if self._jobs:
                    return _jobs_select_star(self._jobs, self._last_query, self._last_params)
                return self.rows
            if self._returning is not None:
                return self._returning
            # Empty jobs table: fall back to injected default rows.
            return self.rows
        if "profiling_results" in self._last_query:
            if self._returning is not None:
                return self._returning
            return _pr_select(self._pr_store, self._last_query, self._last_params)
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
        self.profiling_results: list[dict[str, Any]] = []
        self.jobs: dict[str, dict[str, Any]] = {}
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._cursor = FakeCursor(self._rows, self._responses, self.profiling_results, self.jobs, self.queries)

    async def execute(self, query: str, params: tuple[Any, ...] = ()) -> FakeResult:
        self.queries.append((query, params))
        for pattern, resp_rows in self._responses.items():
            if pattern in query:
                return FakeResult(resp_rows)
        if _jobs_is_jobs_query(query) and "SELECT *" not in query and _jobs_table_backed(self.jobs, query):
            return FakeResult(_jobs_execute(self.jobs, query, params))
        if "profiling_results" in query:
            if "INSERT" in query.upper():
                return FakeResult(_pr_insert(self.profiling_results, list(params)))
            return FakeResult(_pr_select(self.profiling_results, query, params))
        return FakeResult(self._rows)

    def cursor(self, row_factory: Any = None) -> FakeCursor:
        self._cursor._row_factory = row_factory
        return self._cursor

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
    """Create a test client with mocked DB and job runner."""
    app.router.lifespan_context = _noop_lifespan

    fake_conn = FakeConn(default_rows, responses)
    fake_runner = AsyncMock()

    state_module.get_conn = _mock_get_conn(fake_conn)
    state_module.job_runner = fake_runner

    cluster.nodes = [
        {
            "id": "test-node",
            "isForProfiling": False,
            "cost": 0.1,
            "resources": [{"gpu_type": "A40", "gpu_count": 1}],
        },
    ]

    client = TestClient(app)
    return client, fake_conn, fake_runner


# ---------------------------------------------------------------------------
# Stop endpoint
# ---------------------------------------------------------------------------


class TestStopJob:
    """Tests for POST /jobs/{job_id}/stop."""

    def test_stop_queued_job_sets_preempted_directly(self) -> None:
        """Stopping a QUEUED job should set it to PREEMPTED without runner.stop."""
        client, _conn, fake_runner = _make_client(responses={"SELECT status": [("QUEUED",)]})

        response = client.post("/jobs/test-id-1/stop")
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "stopped"
        fake_runner.stop.assert_not_called()

    def test_stop_running_job_calls_runner_stop(self) -> None:
        """Stopping a RUNNING job should call runner.stop."""
        client, _conn, fake_runner = _make_client(responses={"SELECT status": [("RUNNING",)]})

        response = client.post("/jobs/test-id-2/stop")
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "stop_requested"
        fake_runner.stop.assert_called_once()

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

    def test_stop_profiling_job_calls_runner_stop(self) -> None:
        """Stopping a PROFILING job should call runner.stop."""
        client, _conn, fake_runner = _make_client(responses={"SELECT status": [("PROFILING",)]})

        response = client.post("/jobs/prof-id/stop")
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "stop_requested"
        fake_runner.stop.assert_called_once()

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
        """Resuming a PREEMPTED job should set it to QUEUED (no enqueue when no node available)."""
        client, _conn, fake_runner = _make_client(responses={"RETURNING": [("preempt-id", "some-type")]})

        response = client.post("/jobs/preempt-id/resume")
        assert response.status_code == 202
        # No cluster nodes configured → schedule_job returns node_id=None → no enqueue
        fake_runner.dispatch_with_slot.assert_not_called()

    def test_resume_failed_job_sets_queued(self) -> None:
        """Resuming a FAILED job should set it to QUEUED (no enqueue when no node available)."""
        client, _conn, fake_runner = _make_client(responses={"RETURNING": [("fail-id", "some-type")]})

        response = client.post("/jobs/fail-id/resume")
        assert response.status_code == 202
        fake_runner.dispatch_with_slot.assert_not_called()

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
        # stop reads SELECT status (stoppable); resume succeeds via RETURNING.
        client, _conn, _ = _make_client(
            responses={"SELECT status": [("QUEUED",)], "RETURNING": [("test", "test-type")]}
        )
        resp = client.post("/jobs/test/stop")
        assert resp.status_code == 202

        resp = client.post("/jobs/test/resume")
        assert resp.status_code == 202

    def test_stop_resume_stop_resume(self) -> None:
        """Multiple stop/resume cycles should all succeed."""
        client, _conn, _ = _make_client(
            responses={"SELECT status": [("QUEUED",)], "RETURNING": [("test", "test-type")]}
        )

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
        # First stop sees a stoppable status; second sees PREEMPTED (set below).
        conn = FakeConn(responses={"SELECT status": [("QUEUED",)]})
        app.router.lifespan_context = _noop_lifespan
        fake_runner = AsyncMock()
        state_module.get_conn = _mock_get_conn(conn)
        state_module.job_runner = fake_runner
        cluster.nodes = [
            {
                "id": "n",
                "isForProfiling": False,
                "cost": 0.1,
                "resources": [{"gpu_type": "A40", "gpu_count": 1}],
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
        conn = FakeConn(responses={"RETURNING": [("test", "test-type")]})
        app.router.lifespan_context = _noop_lifespan
        fake_runner = AsyncMock()
        state_module.get_conn = _mock_get_conn(conn)
        state_module.job_runner = fake_runner
        cluster.nodes = [
            {
                "id": "n",
                "isForProfiling": False,
                "cost": 0.1,
                "resources": [{"gpu_type": "A40", "gpu_count": 1}],
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
        client, _conn, _ = _make_client(
            responses={"SELECT status": [("QUEUED",)], "RETURNING": [("test", "test-type")]}
        )

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
        client, _conn, _ = _make_client(default_rows=[("some-id", None)])
        response = client.get("/jobs/00000000-0000-0000-0000-000000000001/logs")
        assert response.status_code == 200
        assert "No logs available" in response.text

    def test_logs_with_file(self, tmp_path: Any) -> None:
        """When a log file exists, its contents are returned."""
        job_uuid = "12345678-1234-1234-1234-123456789abc"
        client, _conn, _ = _make_client(default_rows=[(job_uuid, None)])

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
        "resources": [{"gpu_type": "A40", "gpu_count": 4}],
    },
    {
        "id": "node-a40-prof",
        "isForProfiling": True,
        "cost": 0.15,
        "resources": [{"gpu_type": "A40", "gpu_count": 1}],
    },
]


def _job_row(
    id: str = "job-001",
    job_id: str = "test-job",
    image: str = "ijm-runtime:dev",
    command: list[str] | None = None,
    status: str = "QUEUED",
    priority: int = 3,
    assigned_node: str | None = None,
    assigned_gpu_config: dict[str, int] | None = None,
    is_profiling_run: bool = False,
) -> dict[str, Any]:
    """Build a job row dict (matching dict_row format)."""
    now = datetime.now(UTC)
    return {
        "id": id,
        "job_id": job_id,
        "image": image,
        "command": command or ["python", "-u", "train.py"],
        "script_path": None,
        "directory_to_mount": None,
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
        "assigned_gpu_config": assigned_gpu_config,
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
    fake_runner = AsyncMock()

    state_module.get_conn = _mock_get_conn(fake_conn)
    state_module.job_runner = fake_runner
    cluster.nodes = cluster_nodes or list(_CLUSTER_NODES)

    client = TestClient(app)
    return client, fake_conn, fake_runner


# ---------------------------------------------------------------------------
# Create job endpoint
# ---------------------------------------------------------------------------


class TestCreateJob:
    """Tests for POST /jobs."""

    def test_create_job_minimal(self) -> None:
        """Create a job with just image and command."""
        client, conn, fake_runner = _make_rich_client()

        response = client.post(
            "/jobs", json={"job_id": "test-job", "dockerImage": "my-image:v1", "command": ["python", "run.py"]}
        )
        assert response.status_code == 201
        body = response.json()
        assert body["image"] == "my-image:v1"
        assert body["command"] == ["python", "run.py"]
        assert body["status"] == "QUEUED"
        assert body["priority"] == 3
        assert body["is_profiling_run"] is True
        assert body["assigned_gpu_config"] is not None
        fake_runner.dispatch_with_slot.assert_called_once()

    def test_create_job_with_extended_fields(self) -> None:
        """Create a job with extended fields."""
        client, _conn, _ = _make_rich_client()

        response = client.post(
            "/jobs",
            json={
                "job_id": "test-train",
                "dockerImage": "train:latest",
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
        response = client.post("/jobs", json={"job_id": "test", "command": ["python", "run.py"]})
        assert response.status_code == 422

    def test_create_job_priority_out_of_range_returns_422(self) -> None:
        client, _conn, _ = _make_rich_client()

        resp = client.post("/jobs", json={"job_id": "t", "dockerImage": "img", "command": ["cmd"], "Priority": 0})
        assert resp.status_code == 422

        resp = client.post("/jobs", json={"job_id": "t", "dockerImage": "img", "command": ["cmd"], "Priority": 6})
        assert resp.status_code == 422

    def test_create_job_past_deadline_is_accepted(self) -> None:
        # Past deadlines are valid input — the optimizer treats them as urgent
        # ("expected_tardiness > 0") rather than rejecting the job.
        client, _conn, _ = _make_rich_client()
        response = client.post(
            "/jobs",
            json={"job_id": "t", "dockerImage": "img", "command": ["cmd"], "deadline": "2020-01-01T00:00:00Z"},
        )
        assert response.status_code == 201

    def test_create_job_assigns_profiling_config(self) -> None:
        client, conn, _ = _make_rich_client()
        response = client.post(
            "/jobs", json={"job_id": "test", "dockerImage": "img:v1", "command": ["python", "train.py"]}
        )
        assert response.status_code == 201
        body = response.json()
        assert body["is_profiling_run"] is True
        assert body["assigned_gpu_config"] is not None

    def test_create_job_invalid_image_returns_422(self) -> None:
        """Invalid Docker image name returns 422."""
        client, _conn, _ = _make_rich_client()
        response = client.post("/jobs", json={"job_id": "t", "dockerImage": "../evil", "command": ["cmd"]})
        assert response.status_code == 422
        assert "Invalid Docker image" in response.json()["detail"]


# ---------------------------------------------------------------------------
# List / Get job endpoints
# ---------------------------------------------------------------------------


class TestListJobs:
    """Tests for GET /jobs."""

    def test_list_jobs_returns_all(self) -> None:
        rows = [_job_row(id="j1", status="QUEUED"), _job_row(id="j2", status="RUNNING")]
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
        row = _job_row(id="j-abc", status="RUNNING", priority=4)
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
        assert "node-a40-prof" in ids

    def test_list_nodes_marks_busy_when_running_job(self) -> None:
        client, conn, _ = _make_rich_client(
            responses={"assigned_node": [("node-a40-01", "job-xyz")]},
        )
        response = client.get("/nodes")
        assert response.status_code == 200
        data = response.json()

        a40 = next(n for n in data if n["id"] == "node-a40-01")
        prof = next(n for n in data if n["id"] == "node-a40-prof")
        assert a40["status"] == "busy"
        assert a40["current_job_ids"] == ["job-xyz"]
        assert prof["status"] == "idle"
        assert prof["current_job_ids"] == []

    def test_list_nodes_includes_resources(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.get("/nodes")
        data = response.json()
        a40 = next(n for n in data if n["id"] == "node-a40-01")
        assert len(a40["resources"]) == 1
        assert a40["resources"][0]["gpu_type"] == "A40"
        assert a40["resources"][0]["gpu_count"] == 4

    def test_list_nodes_includes_profiling_flag(self) -> None:
        client, _conn, _ = _make_rich_client()
        response = client.get("/nodes")
        data = response.json()
        a40 = next(n for n in data if n["id"] == "node-a40-01")
        prof = next(n for n in data if n["id"] == "node-a40-prof")
        assert a40["is_for_profiling"] is False
        assert prof["is_for_profiling"] is True


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
        # No response mocks — drive real state through the in-memory jobs table.
        conn = FakeConn()
        fake_runner = AsyncMock()
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(conn)
        state_module.job_runner = fake_runner
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs", json={"job_id": "test", "dockerImage": "img", "command": ["cmd"]})
        assert resp.status_code == 201
        job_id = resp.json()["id"]

        resp = client.post(f"/jobs/{job_id}/stop")
        assert resp.status_code == 202
        assert resp.json()["status"] == "stopped"

    def test_create_stop_resume_full_cycle(self) -> None:
        """Create -> stop -> resume -> verify runner calls."""
        # No response mocks — drive real state through the in-memory jobs table.
        conn = FakeConn()
        fake_runner = AsyncMock()
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(conn)
        state_module.job_runner = fake_runner
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs", json={"job_id": "test", "dockerImage": "img", "command": ["cmd"]})
        assert resp.status_code == 201
        job_id = resp.json()["id"]
        assert fake_runner.dispatch_with_slot.call_count == 1

        resp = client.post(f"/jobs/{job_id}/stop")
        assert resp.status_code == 202
        assert fake_runner.dispatch_with_slot.call_count == 1  # QUEUED->PREEMPTED, no enqueue

        resp = client.post(f"/jobs/{job_id}/resume")
        assert resp.status_code == 202
        assert fake_runner.dispatch_with_slot.call_count == 2

    def test_resume_preserves_profiling_results(self) -> None:
        """Resume should NOT delete profiling results."""
        client, conn, _ = _make_client(responses={"RETURNING": [("job-xyz", "lstm-type")]})

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

    def test_create_job_enqueue_contains_job_id(self) -> None:
        client, _conn, fake_runner = _make_rich_client()
        resp = client.post("/jobs", json={"job_id": "test", "dockerImage": "img", "command": ["cmd"]})
        assert resp.status_code == 201
        job_id = resp.json()["id"]

        fake_runner.dispatch_with_slot.assert_called_once()
        enqueued_id = fake_runner.dispatch_with_slot.call_args[0][0]
        assert enqueued_id == job_id

    def test_stop_running_calls_runner_stop_with_job_id(self) -> None:
        client, _conn, fake_runner = _make_client(responses={"SELECT status": [("RUNNING",)]})
        resp = client.post("/jobs/run-job-123/stop")
        assert resp.status_code == 202

        fake_runner.stop.assert_called_once()
        stopped_id = fake_runner.stop.call_args[0][0]
        assert stopped_id == "run-job-123"


# ---------------------------------------------------------------------------
# require_runner edge case — DB is set but runner is not
# ---------------------------------------------------------------------------


class TestRequireRunner:
    """Test that endpoints needing the job runner return 503 when job_runner is None."""

    def test_create_job_returns_503_without_runner(self) -> None:
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(FakeConn())
        state_module.job_runner = None
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs", json={"job_id": "test", "dockerImage": "img", "command": ["cmd"]})
        assert resp.status_code == 503
        assert "runner" in resp.json()["detail"].lower()

    def test_stop_running_returns_503_without_runner(self) -> None:
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(FakeConn(responses={"SELECT status": [("RUNNING",)]}))
        state_module.job_runner = None
        cluster.nodes = list(_CLUSTER_NODES)

        client = TestClient(app)
        resp = client.post("/jobs/test/stop")
        assert resp.status_code == 503

    def test_resume_returns_503_without_runner(self) -> None:
        app.router.lifespan_context = _noop_lifespan
        state_module.get_conn = _mock_get_conn(FakeConn(responses={"RETURNING": [("test", "test-type")]}))
        state_module.job_runner = None
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
        "resources": [{"gpu_type": "A40", "gpu_count": 4}],
    },
    {
        "id": "node-mixed-01",
        "isForProfiling": False,
        "cost": 0.20,
        "resources": [
            {"gpu_type": "A40", "gpu_count": 2},
            {"gpu_type": "L40S", "gpu_count": 2},
        ],
    },
    {
        "id": "node-l40s-01",
        "isForProfiling": False,
        "cost": 0.18,
        "resources": [{"gpu_type": "L40S", "gpu_count": 2}],
    },
    {
        "id": "node-blackwell-01",
        "isForProfiling": False,
        "cost": 0.45,
        "resources": [{"gpu_type": "Blackwell", "gpu_count": 2}],
    },
    {
        "id": "node-a40-prof",
        "isForProfiling": True,
        "cost": 0.15,
        "resources": [{"gpu_type": "A40", "gpu_count": 1}],
    },
    {
        "id": "node-mixed-prof",
        "isForProfiling": True,
        "cost": 0.25,
        "resources": [
            {"gpu_type": "L40S", "gpu_count": 1},
            {"gpu_type": "Blackwell", "gpu_count": 1},
        ],
    },
]


class TestMultiJobRegression:
    """Regression tests for multi-job submission."""

    def test_submit_many_jobs_all_get_profiling_assignment(self) -> None:
        client, conn, fake_runner = _make_rich_client(cluster_nodes=_REAL_CLUSTER_NODES)

        for i in range(6):
            resp = client.post(
                "/jobs",
                json={
                    "job_id": f"train-{i}",
                    "dockerImage": f"train-img:v{i}",
                    "command": ["python", "-u", "train.py"],
                },
            )
            assert resp.status_code == 201, f"Job {i} creation failed: {resp.json()}"
            body = resp.json()
            assert body["is_profiling_run"] is True
            assert body["assigned_node"] is not None
            assert body["assigned_gpu_config"] is not None

        assert fake_runner.dispatch_with_slot.call_count == 6

    def test_all_jobs_enqueued_after_submission(self) -> None:
        # Four independent job types: each starts its own profiling cycle, so
        # each submission is placed and dispatched under its own instance id.
        client, _conn, fake_runner = _make_rich_client(cluster_nodes=_REAL_CLUSTER_NODES)

        job_ids: list[str] = []
        for i in range(4):
            resp = client.post(
                "/jobs",
                json={"job_id": f"test-{i}", "dockerImage": "img:latest", "command": ["python", "train.py"]},
            )
            assert resp.status_code == 201
            job_ids.append(resp.json()["id"])

        enqueued_ids: set[str] = set()
        for call in fake_runner.dispatch_with_slot.call_args_list:
            enqueued_ids.add(call[0][0])

        for jid in job_ids:
            assert jid in enqueued_ids

    def test_profiling_before_running_invariant(self) -> None:
        sched = ProfilingScheduler()
        cluster.nodes = _REAL_CLUSTER_NODES
        all_configs = sched.get_valid_configurations()
        # Profiling nodes: node-a40-prof (A40:1) + node-mixed-prof (L40S:1, Blackwell:1, L40S+Blackwell:1)
        # Intersection: L40S+Blackwell:1 filtered (no production node has both), leaving 3 configs
        assert len(all_configs) >= 3

    async def test_one_config_per_submission_then_standard(self) -> None:
        """Simulate profiling all configs when configs_per_job is high enough."""
        cluster.nodes = _REAL_CLUSTER_NODES
        sched = ProfilingScheduler()
        all_configs = sched.get_valid_configurations()
        # Allow profiling ALL configs before switching to standard
        sched.configs_per_job = len(all_configs)

        conn = FakeConn()

        # Same instance each round; the worker measuring the dispatched cell
        # between rounds advances the scheduler to the next config until all
        # are profiled, after which it drops to a standard run.
        profiled: list[dict[str, int]] = []
        for _i in range(len(all_configs)):
            result = await sched.schedule_job(conn, "job-regression")
            assert result.is_profiling_run is True
            config = result.gpu_config
            assert config is not None
            assert config not in profiled
            profiled.append(config)
            for row in conn.profiling_results:
                if row["duration_seconds"] is None and row["gpu_config"] == config:
                    row["duration_seconds"] = 30.0

        result = await sched.schedule_job(conn, "job-regression")
        assert result.is_profiling_run is False
        assert result.mode == "standard"

    async def test_configs_per_job_limits_per_round(self) -> None:
        """With configs_per_job=1, one profiling run per round then standard run."""
        cluster.nodes = _REAL_CLUSTER_NODES
        sched = ProfilingScheduler()
        all_configs = sched.get_valid_configurations()
        assert len(all_configs) > 1, "Need multiple configs for this test"
        # Exercise the one-config-per-round limit explicitly (the cluster
        # default is now 2 configs per round — see DEFAULT_PROFILING_CONFIGS_PER_JOB).
        sched.configs_per_job = 1

        conn = FakeConn()

        # Round start: this instance profiles its one (budget=1) config.
        result = await sched.schedule_job(conn, "job-limit")
        assert result.is_profiling_run is True
        assert result.gpu_config is not None
        first_config = result.gpu_config
        # Worker completes the profile for this instance's claim.
        for row in conn.profiling_results:
            if row["instance_id"] == "job-limit" and row["duration_seconds"] is None:
                row["duration_seconds"] = 30.0

        # Same instance, post-profiling: round budget (1) is spent → standard.
        result = await sched.schedule_job(conn, "job-limit")
        assert result.is_profiling_run is False
        assert result.mode == "standard"

        # Different instance of the same type: independent round → profiles the
        # next still-unprofiled config.
        result = await sched.schedule_job(conn, "job-limit-2", job_type_id="job-limit")
        assert result.is_profiling_run is True
        assert result.gpu_config is not None
        assert result.gpu_config != first_config

    async def test_multiple_jobs_independent_profiling_cycles(self) -> None:
        """Different jobs have independent profiling state."""
        cluster.nodes = [
            {
                "id": "n1",
                "isForProfiling": False,
                "cost": 0.1,
                "resources": [{"gpu_type": "A40", "gpu_count": 1}],
            },
            {
                "id": "n-prof",
                "isForProfiling": True,
                "cost": 0.1,
                "resources": [{"gpu_type": "A40", "gpu_count": 1}],
            },
        ]
        sched = ProfilingScheduler()

        # Independent connections → independent profiling stores: A and B each
        # start a fresh profiling cycle and claim the single valid config.
        conn_a = FakeConn()
        result_a = await sched.schedule_job(conn_a, "job-A")
        assert result_a.is_profiling_run is True

        conn_b = FakeConn()
        result_b = await sched.schedule_job(conn_b, "job-B")
        assert result_b.is_profiling_run is True

        # C's type already has its only config measured → standard, no profiling.
        conn_c = FakeConn()
        conn_c.profiling_results.append(
            {
                "job_id": "job-C",
                "instance_id": "measured-job-C",
                "gpu_config": {"A40": 1},
                "node_id": None,
                "duration_seconds": 25.0,
            }
        )
        result_c = await sched.schedule_job(conn_c, "job-C")
        assert result_c.is_profiling_run is False

    def test_stop_profiling_job_does_not_block_others(self) -> None:
        from src.constants import STOPPABLE_STATUSES, JobStatus

        assert JobStatus.PROFILING in STOPPABLE_STATUSES

        client, _conn, fake_runner = _make_client(responses={"SELECT status": [("PROFILING",)]})
        resp = client.post("/jobs/prof-job/stop")
        assert resp.status_code == 202
        fake_runner.stop.assert_called_once()

    def test_resume_preserves_profiling_and_profiles_next(self) -> None:
        client, conn, fake_runner = _make_rich_client(cluster_nodes=_REAL_CLUSTER_NODES)
        # Seed a stopped job to resume; resume must NOT wipe profiling results.
        conn.jobs["resume-job"] = {
            "id": "resume-job",
            "job_id": "lstm-type",
            "status": "PREEMPTED",
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
            "assigned_node": None,
            "assigned_gpu_config": None,
            "is_profiling_run": False,
        }

        resp = client.post("/jobs/resume-job/resume")
        assert resp.status_code == 202

        delete_queries = [q for q, _p in conn.queries if "DELETE" in q and "profiling_results" in q]
        assert len(delete_queries) == 0


# ---------------------------------------------------------------------------
# Scheduler GPU accounting — regression tests for over-subscription bug
# ---------------------------------------------------------------------------


_TWO_GPU_NODE = {
    "id": "gpu-node",
    "isForProfiling": True,
    "cost": 0.1,
    "resources": [{"gpu_type": "A40", "gpu_count": 2}],
}


class TestSchedulerGpuAccounting:
    """Regression: QUEUED+assigned jobs must count toward GPU usage.

    Previously get_node_gpu_usage only counted RUNNING/PROFILING, so the
    scheduler could assign the same GPU slot to multiple jobs before any of
    them transitioned to RUNNING.
    """

    async def test_queued_assigned_jobs_counted_in_gpu_usage(self) -> None:
        """QUEUED jobs with assigned_node must appear in the usage totals."""
        sched = ProfilingScheduler()
        conn = FakeConn(
            responses={
                # key matches only the jobs SELECT, not the cross-instance
                # profiling_results JOIN (which must return no claims here)
                "FROM jobs": [
                    ("j", "gpu-node", {"A40": 1}),
                    ("j", "gpu-node", {"A40": 1}),
                ]
            }
        )
        usage = await sched.get_node_gpu_usage(conn)
        assert usage == {"gpu-node": {"A40": 2}}

    async def test_no_oversubscription_when_queued_jobs_fill_node(self) -> None:
        """A 3rd job must not be scheduled when 2 QUEUED jobs already fill the node."""
        cluster.nodes = [_TWO_GPU_NODE]
        sched = ProfilingScheduler()
        # Two QUEUED jobs already assigned, each consuming 1 GPU
        conn = FakeConn(
            responses={
                "assigned_node": [
                    ("j", "gpu-node", {"A40": 1}),
                    ("j", "gpu-node", {"A40": 1}),
                ],
            }
        )
        result = await sched.schedule_job(conn, "new-job", job_type_id="lstm-small")
        assert result.node_id is None

    async def test_one_queued_job_leaves_one_slot_free(self) -> None:
        """With one QUEUED job on a 2-GPU node, a second job should still fit."""
        cluster.nodes = [_TWO_GPU_NODE]
        sched = ProfilingScheduler()
        conn = FakeConn(
            responses={
                "assigned_node": [("j", "gpu-node", {"A40": 1})],
            }
        )
        result = await sched.schedule_job(conn, "new-job", job_type_id="lstm-small")
        assert result.node_id == "gpu-node"

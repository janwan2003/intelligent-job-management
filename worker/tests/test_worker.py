"""Tests for the job worker."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from shared.constants import NATS_SUBJECT_PROFILING_COMPLETE, JobStatus

from constants import (
    CHECKPOINT_MOUNT_PATH,
    RUNS_MOUNT_PATH,
)
from worker import PROGRESS_RE, JobWorker


class TestBuildDockerCmd:
    """Tests for docker command construction."""

    def test_basic_command(self) -> None:
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-abc12345",
            ckpt_host_path="/home/user/project/data/checkpoints/job-1",
            runs_host_path="/home/user/project/data/runs/job-1",
            image="ijm-runtime:dev",
            command=["python", "-u", "train.py"],
        )
        assert cmd == [
            "docker",
            "run",
            "--rm",
            "--name",
            "ijm-abc12345",
            "-v",
            f"/home/user/project/data/checkpoints/job-1:{CHECKPOINT_MOUNT_PATH}",
            "-v",
            f"/home/user/project/data/runs/job-1:{RUNS_MOUNT_PATH}",
            "ijm-runtime:dev",
            "python",
            "-u",
            "train.py",
        ]

    def test_volume_paths_use_host_paths_not_container_paths(self) -> None:
        """Verify volume mounts use host-resolvable paths, not /host/* paths."""
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-test",
            ckpt_host_path="/real/host/path/checkpoints/job-1",
            runs_host_path="/real/host/path/runs/job-1",
            image="test:latest",
            command=[],
        )
        volume_args = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-v"]
        assert len(volume_args) == 2
        for vol in volume_args:
            host_part = vol.split(":")[0]
            assert host_part.startswith("/real/host/path")

    def test_empty_command(self) -> None:
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-test",
            ckpt_host_path="/tmp/ckpt",
            runs_host_path="/tmp/runs",
            image="test:latest",
            command=[],
        )
        assert cmd[-1] == "test:latest"

    def test_env_vars(self) -> None:
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-test",
            ckpt_host_path="/tmp/ckpt",
            runs_host_path="/tmp/runs",
            image="test:latest",
            command=["train.py"],
            env_vars={"LOG_INTERVAL": "50", "MAX_STEPS": "100"},
        )
        env_pairs = []
        for i, arg in enumerate(cmd):
            if arg == "-e":
                env_pairs.append(cmd[i + 1])
        assert "LOG_INTERVAL=50" in env_pairs
        assert "MAX_STEPS=100" in env_pairs

    def test_none_env_vars(self) -> None:
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-test",
            ckpt_host_path="/tmp/ckpt",
            runs_host_path="/tmp/runs",
            image="test:latest",
            command=[],
            env_vars=None,
        )
        assert "-e" not in cmd


# ---------------------------------------------------------------------------
# Progress regex
# ---------------------------------------------------------------------------


class TestProgressRegex:
    def test_matches_step_pattern(self) -> None:
        m = PROGRESS_RE.search("Step 50/10000")
        assert m is not None
        assert m.group(1) == "50"
        assert m.group(2) == "10000"

    def test_matches_with_prefix(self) -> None:
        m = PROGRESS_RE.search("[Job abc123] Step 100/5000")
        assert m is not None
        assert m.group(1) == "100"

    def test_no_match(self) -> None:
        assert PROGRESS_RE.search("Training started") is None


# ---------------------------------------------------------------------------
# Warmup-aware profiling duration
# ---------------------------------------------------------------------------


class TestComputeProfilingDuration:
    def test_warmup_excluded_with_enough_timestamps(self) -> None:
        ts = [(50, 10.0), (100, 15.0), (150, 19.0), (200, 23.0)]
        duration = JobWorker._compute_profiling_duration(ts, datetime.now(UTC))
        assert duration > 0
        # With warmup excluded: mean of [4.0, 4.0] = 4.0, total_steps=200 → 800
        assert abs(duration - 800.0) < 0.1

    def test_fallback_to_wall_clock(self) -> None:
        start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        ts = [(50, 10.0), (100, 15.0)]  # only 2 timestamps — not enough
        duration = JobWorker._compute_profiling_duration(ts, start)
        assert duration > 0

    def test_none_timestamps_uses_wall_clock(self) -> None:
        start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        duration = JobWorker._compute_profiling_duration(None, start)
        assert duration > 0

    def test_empty_timestamps_uses_wall_clock(self) -> None:
        start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        duration = JobWorker._compute_profiling_duration([], start)
        assert duration > 0


# ---------------------------------------------------------------------------
# _resolve_paths
# ---------------------------------------------------------------------------


class TestResolvePaths:
    def test_resolve_paths(self) -> None:
        worker = JobWorker()
        worker.host_root = "/host"
        worker.host_project_root = "/real/path"
        ckpt_local, runs_local, ckpt_host, runs_host = worker._resolve_paths("job-123")
        assert str(ckpt_local) == "/host/data/checkpoints/job-123"
        assert str(runs_local) == "/host/data/runs/job-123"
        assert str(ckpt_host) == "/real/path/data/checkpoints/job-123"
        assert str(runs_host) == "/real/path/data/runs/job-123"


# ---------------------------------------------------------------------------
# _prepare_checkpoint_dir
# ---------------------------------------------------------------------------


class TestPrepareCheckpointDir:
    def test_profiling_uses_subdirectory(self, tmp_path: Path) -> None:
        result = JobWorker._prepare_checkpoint_dir(tmp_path, is_profiling=True, is_first_run=False)
        assert result == tmp_path / ".profiling"
        assert result.exists()

    def test_profiling_clears_old_files(self, tmp_path: Path) -> None:
        prof_dir = tmp_path / ".profiling"
        prof_dir.mkdir()
        (prof_dir / "old_checkpoint.pt").write_text("data")
        JobWorker._prepare_checkpoint_dir(tmp_path, is_profiling=True, is_first_run=False)
        assert not (prof_dir / "old_checkpoint.pt").exists()

    def test_first_run_clears_checkpoints(self, tmp_path: Path) -> None:
        (tmp_path / "stale.pt").write_text("data")
        JobWorker._prepare_checkpoint_dir(tmp_path, is_profiling=False, is_first_run=True)
        assert not (tmp_path / "stale.pt").exists()

    def test_resume_preserves_checkpoints(self, tmp_path: Path) -> None:
        (tmp_path / "latest.pt").write_text("data")
        JobWorker._prepare_checkpoint_dir(tmp_path, is_profiling=False, is_first_run=False)
        assert (tmp_path / "latest.pt").exists()


# ---------------------------------------------------------------------------
# _build_env_vars
# ---------------------------------------------------------------------------


class TestBuildEnvVars:
    def test_profiling_run(self) -> None:
        job = {"is_profiling_run": True, "log_interval": 25, "profiling_epochs_no": 50, "epochs_total": 10000}
        env = JobWorker._build_env_vars(job)
        assert env["LOG_INTERVAL"] == "25"
        assert env["MAX_STEPS"] == "50"

    def test_standard_run(self) -> None:
        job = {"is_profiling_run": False, "log_interval": 50, "profiling_epochs_no": 100, "epochs_total": 5000}
        env = JobWorker._build_env_vars(job)
        assert env["LOG_INTERVAL"] == "50"
        assert env["MAX_STEPS"] == "5000"

    def test_standard_run_no_epochs(self) -> None:
        job = {"is_profiling_run": False, "log_interval": 50, "profiling_epochs_no": 100, "epochs_total": None}
        env = JobWorker._build_env_vars(job)
        assert "MAX_STEPS" not in env


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_running_jobs_tracked_in_dict(self) -> None:
        """running_jobs dict should track all concurrently running jobs."""
        worker = JobWorker()
        worker.database_url = "mock://db"

        barrier = asyncio.Event()

        async def fake_run_job(job_id: str) -> None:
            worker.running_jobs[job_id] = MagicMock()
            await barrier.wait()
            worker.running_jobs.pop(job_id, None)

        with patch.object(worker, "_run_job", side_effect=fake_run_job):
            tasks = [asyncio.create_task(worker._run_job(f"job-{i}")) for i in range(3)]
            await asyncio.sleep(0.01)
            assert len(worker.running_jobs) == 3
            barrier.set()
            await asyncio.gather(*tasks)
            assert len(worker.running_jobs) == 0


# ---------------------------------------------------------------------------
# Fake DB infrastructure
# ---------------------------------------------------------------------------


def _make_job_dict(
    *,
    is_profiling_run: bool,
    status: str = "QUEUED",
    prev_exit_code: int | None = None,
) -> dict[str, object]:
    """Return a dict matching what dict_row would produce for a job SELECT."""
    return {
        "image": "img:latest",
        "command": ["python", "train.py"],
        "status": status,
        "is_profiling_run": is_profiling_run,
        "profiling_epochs_no": 100,
        "exit_code": prev_exit_code,
        "log_interval": 50,
        "epochs_total": 10000,
        "assigned_node": "node-1",
        "assigned_gpu_config": {"A40": 2},
    }


class FlexFakeCursor:
    """Cursor that returns configurable dict rows keyed by query substring."""

    def __init__(self, responses: dict[str, list[dict[str, object] | None]]) -> None:
        self._responses = responses
        self._last_query = ""
        self._executions: list[tuple[str, object]] = []

    async def execute(self, query: str, params: object = ()) -> None:
        self._executions.append((query, params))
        self._last_query = query

    async def fetchone(self) -> dict[str, object] | None:
        for key, rows in self._responses.items():
            if key in self._last_query and rows:
                return rows.pop(0)
        return None

    async def fetchall(self) -> list[dict[str, object]]:
        for key, rows in self._responses.items():
            if key in self._last_query and rows:
                result = [r for r in rows if r is not None]
                rows.clear()
                return result
        return []

    async def __aenter__(self) -> "FlexFakeCursor":
        return self

    async def __aexit__(self, *_args: object) -> None:
        pass


class FlexFakeConn:
    """Connection that tracks all queries."""

    def __init__(self, responses: dict[str, list[dict[str, object] | None]]) -> None:
        self._responses = responses
        self.queries: list[tuple[str, object]] = []
        self._cursor = FlexFakeCursor(responses)

    def cursor(self) -> FlexFakeCursor:
        cur = FlexFakeCursor(self._responses)
        cur._executions = self.queries
        return cur

    async def commit(self) -> None:
        pass

    async def close(self) -> None:
        pass


def _make_fake_worker_db(
    *,
    is_profiling_run: bool,
    status: str = "QUEUED",
    prev_exit_code: int | None = None,
) -> tuple[JobWorker, list[tuple[str, object]]]:
    """Create a JobWorker with mocked DB that returns the given job metadata."""
    job = _make_job_dict(is_profiling_run=is_profiling_run, status=status, prev_exit_code=prev_exit_code)
    profiling_meta = {
        "is_profiling_run": is_profiling_run,
        "assigned_gpu_config": {"A40": 2},
        "assigned_node": "node-1",
    }
    status_row: dict[str, object] = {"status": JobStatus.PROFILING if is_profiling_run else JobStatus.RUNNING}

    responses: dict[str, list[dict[str, object] | None]] = {
        "SELECT image": [job],
        "SELECT status": [status_row],
        "SELECT is_profiling_run": [profiling_meta],
    }

    fake_conn = FlexFakeConn(responses)
    worker = JobWorker()
    worker.database_url = "mock://db"
    worker.host_root = "/tmp/test-host"
    worker.host_project_root = "/tmp/test-host"
    worker.connect_db = AsyncMock(return_value=fake_conn)
    worker.js = AsyncMock()

    return worker, fake_conn.queries


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------


class TestStatusTransitions:
    @pytest.mark.asyncio
    async def test_profiling_run_sets_profiling_status(self, tmp_path: Path) -> None:
        """When is_profiling_run=True, worker must set status to PROFILING."""
        worker, queries = _make_fake_worker_db(is_profiling_run=True)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-job-profiling-1234")

        status_updates = [p for q, p in queries if "SET" in q and "status" in q]
        assert any((isinstance(p, dict) and p.get("status") == JobStatus.PROFILING) for p in status_updates), (
            f"Expected PROFILING status update, got: {status_updates}"
        )

    @pytest.mark.asyncio
    async def test_standard_run_sets_running_status(self, tmp_path: Path) -> None:
        """When is_profiling_run=False, worker must set status to RUNNING."""
        worker, queries = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-job-standard-1234")

        status_updates = [p for q, p in queries if "SET" in q and "status" in q]
        assert any((isinstance(p, dict) and p.get("status") == JobStatus.RUNNING) for p in status_updates), (
            f"Expected RUNNING status update, got: {status_updates}"
        )

    @pytest.mark.asyncio
    async def test_profiling_run_sets_max_steps_env(self, tmp_path: Path) -> None:
        """Profiling runs should pass MAX_STEPS env var to docker."""
        worker, _ = _make_fake_worker_db(is_profiling_run=True)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-job-maxsteps-1234")

        cmd = mock_popen.call_args[0][0]
        env_idx = [i for i, c in enumerate(cmd) if c == "-e"]
        env_vals = [cmd[i + 1] for i in env_idx]
        assert "MAX_STEPS=100" in env_vals

    @pytest.mark.asyncio
    async def test_standard_run_passes_env_vars(self, tmp_path: Path) -> None:
        """Standard runs pass LOG_INTERVAL and MAX_STEPS (from epochs_total)."""
        worker, _ = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-job-envvars-1234")

        cmd = mock_popen.call_args[0][0]
        env_idx = [i for i, c in enumerate(cmd) if c == "-e"]
        env_vals = [cmd[i + 1] for i in env_idx]
        assert "LOG_INTERVAL=50" in env_vals
        assert "MAX_STEPS=10000" in env_vals

    @pytest.mark.asyncio
    async def test_profiling_run_uses_isolated_checkpoint_dir(self, tmp_path: Path) -> None:
        """Profiling runs must use a .profiling/ subdirectory."""
        worker, _ = _make_fake_worker_db(is_profiling_run=True)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-job-ckpt-iso-1234")

        cmd = mock_popen.call_args[0][0]
        vol_args = [cmd[i + 1] for i, c in enumerate(cmd) if c == "-v"]
        ckpt_vol = [v for v in vol_args if "checkpoints" in v][0]
        host_part = ckpt_vol.split(":")[0]
        assert host_part.endswith(".profiling")

    @pytest.mark.asyncio
    async def test_standard_run_preserves_checkpoints(self, tmp_path: Path) -> None:
        """Standard runs with prev_exit_code must NOT clear checkpoints."""
        worker, _ = _make_fake_worker_db(is_profiling_run=False, prev_exit_code=0)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        ckpt_dir = tmp_path / "data" / "checkpoints" / "test-job-preserve1234"
        ckpt_dir.mkdir(parents=True)
        (ckpt_dir / "latest.pt").write_text("checkpoint_data")

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-job-preserve1234")

        assert (ckpt_dir / "latest.pt").exists()


# ---------------------------------------------------------------------------
# connect_db
# ---------------------------------------------------------------------------


class TestConnectDb:
    @pytest.mark.asyncio
    async def test_missing_database_url_raises(self) -> None:
        worker = JobWorker()
        worker.database_url = None
        with pytest.raises(RuntimeError, match="DATABASE_URL"):
            await worker.connect_db()

    @pytest.mark.asyncio
    async def test_with_database_url_calls_connect(self) -> None:
        worker = JobWorker()
        worker.database_url = "postgresql://localhost/test"
        with patch("worker.psycopg.AsyncConnection.connect", new_callable=AsyncMock) as mock_conn:
            mock_conn.return_value = MagicMock()
            result = await worker.connect_db()
            mock_conn.assert_called_once()
            assert result is mock_conn.return_value


# ---------------------------------------------------------------------------
# _ensure_streams
# ---------------------------------------------------------------------------


class TestEnsureStreams:
    @pytest.mark.asyncio
    async def test_creates_stream(self) -> None:
        worker = JobWorker()
        worker.js = AsyncMock()
        await worker._ensure_streams()
        worker.js.add_stream.assert_called_once()

    @pytest.mark.asyncio
    async def test_js_none_raises(self) -> None:
        worker = JobWorker()
        worker.js = None
        with pytest.raises(RuntimeError):
            await worker._ensure_streams()

    @pytest.mark.asyncio
    async def test_handles_existing_stream(self) -> None:
        worker = JobWorker()
        worker.js = AsyncMock()
        worker.js.add_stream.side_effect = Exception("stream exists")
        await worker._ensure_streams()


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


class TestReconciliation:
    @pytest.mark.asyncio
    async def test_marks_orphaned_running_as_failed(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT id": [
                {"id": "job-a", "container_name": "ijm-job-a", "status": JobStatus.RUNNING},
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._reconcile_job_states()

        updates = [(q, p) for q, p in fake_conn.queries if "SET" in q]
        assert len(updates) == 1

    @pytest.mark.asyncio
    async def test_skips_running_with_container_present(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT id": [
                {"id": "job-a", "container_name": "ijm-job-a", "status": JobStatus.RUNNING},
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            # Container is present
            mock_run.return_value = MagicMock(stdout="ijm-job-a\n", returncode=0)
            await worker._reconcile_job_states()

        updates = [(q, p) for q, p in fake_conn.queries if "SET" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_skips_queued_jobs(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT id": [
                {"id": "job-q", "container_name": None, "status": JobStatus.QUEUED},
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._reconcile_job_states()

        updates = [(q, p) for q, p in fake_conn.queries if "SET" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_no_jobs_to_reconcile(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT id": [],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._reconcile_job_states()

    @pytest.mark.asyncio
    async def test_reconcile_handles_db_error(self) -> None:
        """DB errors during reconciliation should be logged, not crash."""
        worker = JobWorker()
        worker.connect_db = AsyncMock(side_effect=Exception("connection refused"))
        await worker._reconcile_job_states()

    @pytest.mark.asyncio
    async def test_reconcile_profiling_orphan(self) -> None:
        """PROFILING jobs whose container vanished should also be marked FAILED."""
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT id": [
                {"id": "prof-job", "container_name": "ijm-prof-job", "status": JobStatus.PROFILING},
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._reconcile_job_states()

        updates = [(q, p) for q, p in fake_conn.queries if "SET" in q]
        assert len(updates) == 1


# ---------------------------------------------------------------------------
# Pickup queued
# ---------------------------------------------------------------------------


class TestPickupQueued:
    @pytest.mark.asyncio
    async def test_enqueues_missed_jobs(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT id": [{"id": "q1"}, {"id": "q2"}],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._pickup_queued_jobs()
        assert worker.job_queue.qsize() == 2

    @pytest.mark.asyncio
    async def test_no_queued_jobs(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT id": [],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._pickup_queued_jobs()
        assert worker.job_queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_handles_db_error(self) -> None:
        worker = JobWorker()
        worker.connect_db = AsyncMock(side_effect=Exception("db down"))
        await worker._pickup_queued_jobs()
        assert worker.job_queue.qsize() == 0


# ---------------------------------------------------------------------------
# NATS event handlers
# ---------------------------------------------------------------------------


def _make_nats_msg(data: dict[str, str]) -> MagicMock:
    msg = MagicMock()
    msg.data = json.dumps(data).encode()
    msg.ack = AsyncMock()
    msg.nak = AsyncMock()
    return msg


class TestHandleJobSubmitted:
    @pytest.mark.asyncio
    async def test_enqueues_job(self) -> None:
        worker = JobWorker()
        msg = _make_nats_msg({"job_id": "abc-123"})
        await worker._handle_job_submitted(msg)
        assert worker.job_queue.qsize() == 1
        action, job_id = worker.job_queue.get_nowait()
        assert action == "run"
        assert job_id == "abc-123"
        msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_naks_on_error(self) -> None:
        worker = JobWorker()
        msg = MagicMock()
        msg.data = b"not json"
        msg.ack = AsyncMock()
        msg.nak = AsyncMock()
        await worker._handle_job_submitted(msg)
        msg.nak.assert_called_once()


class TestHandleStopRequested:
    @pytest.mark.asyncio
    async def test_stop_tracked_job(self) -> None:
        """When a job is tracked in running_jobs, stop handler should stop it."""
        worker = JobWorker()
        job_id = "stop-test-1234"
        worker.running_jobs[job_id] = MagicMock()

        fake_conn = FlexFakeConn({})
        worker.connect_db = AsyncMock(return_value=fake_conn)

        msg = _make_nats_msg({"job_id": job_id})

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            await worker._handle_stop_requested(msg)

        msg.ack.assert_called_once()
        updates = [(q, p) for q, p in fake_conn.queries if "SET" in q]
        assert len(updates) >= 1

    @pytest.mark.asyncio
    async def test_naks_on_error(self) -> None:
        worker = JobWorker()
        msg = MagicMock()
        msg.data = b"not json"
        msg.ack = AsyncMock()
        msg.nak = AsyncMock()
        await worker._handle_stop_requested(msg)
        msg.nak.assert_called_once()


class TestHandleResumeRequested:
    @pytest.mark.asyncio
    async def test_enqueues_resume(self) -> None:
        worker = JobWorker()
        msg = _make_nats_msg({"job_id": "resume-123"})
        await worker._handle_resume_requested(msg)
        assert worker.job_queue.qsize() == 1
        msg.ack.assert_called_once()


# ---------------------------------------------------------------------------
# _update_job
# ---------------------------------------------------------------------------


class TestUpdateJob:
    @pytest.mark.asyncio
    async def test_updates_fields(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {}
        fake_conn = FlexFakeConn(responses)

        worker = JobWorker()
        await worker._update_job(fake_conn, "job-1", status="RUNNING", progress="50/100")  # type: ignore[arg-type]

        assert len(fake_conn.queries) == 1
        query, params = fake_conn.queries[0]
        assert "SET" in query
        assert isinstance(params, dict)
        assert params["status"] == "RUNNING"
        assert params["progress"] == "50/100"
        assert params["_id"] == "job-1"


# ---------------------------------------------------------------------------
# _run_job
# ---------------------------------------------------------------------------


class TestRunJob:
    @pytest.mark.asyncio
    async def test_skips_nonexistent_job(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {}
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._run_job("nonexistent-job")

    @pytest.mark.asyncio
    async def test_skips_non_runnable_status(self) -> None:
        job = _make_job_dict(is_profiling_run=False, status="SUCCEEDED")
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT image": [job],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._run_job("succeeded-job")
        updates = [(q, p) for q, p in fake_conn.queries if "SET" in q and "status" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_failed_exit_code_sets_failed(self, tmp_path: Path) -> None:
        """Non-zero exit code should mark the job as FAILED."""
        worker, queries = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 1
            mock_popen.return_value = proc

            await worker._run_job("test-job-fail-123456")

        status_updates = [
            p for q, p in queries if "SET" in q and isinstance(p, dict) and p.get("status") == JobStatus.FAILED
        ]
        assert len(status_updates) >= 1

    @pytest.mark.asyncio
    async def test_preempted_status_preserved(self, tmp_path: Path) -> None:
        """If job was marked PREEMPTED during run, keep that status."""
        preempted_job = _make_job_dict(is_profiling_run=False)
        status_row: dict[str, object] = {"status": JobStatus.PREEMPTED}
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT image": [preempted_job],
            "SELECT status": [status_row],
        }

        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.database_url = "mock://db"
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)
        worker.connect_db = AsyncMock(return_value=fake_conn)
        worker.js = AsyncMock()

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-job-preempt1234")

        # Should NOT have a SUCCEEDED update
        succeeded = [
            p
            for q, p in fake_conn.queries
            if "SET" in q and isinstance(p, dict) and p.get("status") == JobStatus.SUCCEEDED
        ]
        assert len(succeeded) == 0

    @pytest.mark.asyncio
    async def test_first_full_run_clears_checkpoints(self, tmp_path: Path) -> None:
        """First full run after profiling (exit_code=None) should clear checkpoint dir."""
        worker, _ = _make_fake_worker_db(is_profiling_run=False, prev_exit_code=None)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        ckpt_dir = tmp_path / "data" / "checkpoints" / "test-clear-ckpt1234"
        ckpt_dir.mkdir(parents=True)
        (ckpt_dir / "old.pt").write_text("stale")

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-clear-ckpt1234")

        assert not (ckpt_dir / "old.pt").exists()

    @pytest.mark.asyncio
    async def test_exception_during_run_marks_failed(self, tmp_path: Path) -> None:
        """If docker Popen raises, job should be marked FAILED."""
        worker, queries = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen", side_effect=OSError("docker not found")):
            await worker._run_job("test-exception-1234")

        failed = [p for q, p in queries if "SET" in q and isinstance(p, dict) and p.get("status") == JobStatus.FAILED]
        assert len(failed) >= 1

    @pytest.mark.asyncio
    async def test_profiling_run_clears_profiling_dir(self, tmp_path: Path) -> None:
        """Profiling runs should clear old profiling checkpoint files."""
        worker, _ = _make_fake_worker_db(is_profiling_run=True)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        ckpt_dir = tmp_path / "data" / "checkpoints" / "test-prof-clear1234"
        prof_dir = ckpt_dir / ".profiling"
        prof_dir.mkdir(parents=True)
        (prof_dir / "old_ckpt.pt").write_text("old")

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-prof-clear1234")

        assert not (prof_dir / "old_ckpt.pt").exists()

    @pytest.mark.asyncio
    async def test_progress_null_on_status_transition(self, tmp_path: Path) -> None:
        """Status update should clear progress to NULL."""
        worker, queries = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.stdout = iter([])
            proc.wait.return_value = 0
            mock_popen.return_value = proc

            await worker._run_job("test-progress-null1234")

        null_updates = [p for q, p in queries if "SET" in q and isinstance(p, dict) and p.get("progress") is None]
        assert len(null_updates) >= 1


# ---------------------------------------------------------------------------
# _check_and_report_profiling
# ---------------------------------------------------------------------------


class TestCheckAndReportProfiling:
    @pytest.mark.asyncio
    async def test_reports_profiling_via_nats(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT is_profiling_run": [
                {"is_profiling_run": True, "assigned_gpu_config": {"A40": 2}, "assigned_node": "node-1"}
            ],
        }
        fake_conn = FlexFakeConn(responses)

        worker = JobWorker()
        worker.js = AsyncMock()

        result = await worker._check_and_report_profiling(
            fake_conn,  # type: ignore[arg-type]
            "prof-job-id",
            datetime.now(UTC),
            step_timestamps=[(50, 10.0), (100, 15.0), (150, 19.0)],
        )

        assert result is True
        worker.js.publish.assert_called_once()
        call_args = worker.js.publish.call_args
        assert call_args[0][0] == NATS_SUBJECT_PROFILING_COMPLETE
        payload = json.loads(call_args[0][1])
        assert payload["job_id"] == "prof-job-id"
        assert payload["gpu_config"] == {"A40": 2}

    @pytest.mark.asyncio
    async def test_returns_false_for_standard_run(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT is_profiling_run": [
                {"is_profiling_run": False, "assigned_gpu_config": None, "assigned_node": None}
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.js = AsyncMock()

        result = await worker._check_and_report_profiling(fake_conn, "std-job-id", datetime.now(UTC))  # type: ignore[arg-type]
        assert result is False
        worker.js.publish.assert_not_called()


# ---------------------------------------------------------------------------
# _publish_completed
# ---------------------------------------------------------------------------


class TestPublishCompleted:
    @pytest.mark.asyncio
    async def test_publishes_event(self) -> None:
        worker = JobWorker()
        worker.js = AsyncMock()
        await worker._publish_completed("job-1")
        worker.js.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_nats_error(self) -> None:
        worker = JobWorker()
        worker.js = AsyncMock()
        worker.js.publish.side_effect = Exception("NATS down")
        await worker._publish_completed("job-1")

    @pytest.mark.asyncio
    async def test_no_js_no_crash(self) -> None:
        worker = JobWorker()
        worker.js = None
        await worker._publish_completed("job-1")


# ---------------------------------------------------------------------------
# _stop_job
# ---------------------------------------------------------------------------


class TestStopJob:
    @pytest.mark.asyncio
    async def test_stops_tracked_running_job(self) -> None:
        """If job is tracked in running_jobs, stop via docker stop."""
        worker = JobWorker()
        job_id = "stop-tracked-12345678"
        worker.running_jobs[job_id] = MagicMock()

        fake_conn = FlexFakeConn({})
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            await worker._stop_job(job_id)

        updates = [(q, p) for q, p in fake_conn.queries if "SET" in q]
        assert len(updates) >= 1

    @pytest.mark.asyncio
    async def test_stop_nonexistent_job(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {}
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._stop_job("nonexistent-job")

    @pytest.mark.asyncio
    async def test_stop_queued_job_marks_preempted(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT container_name": [{"container_name": None, "status": JobStatus.QUEUED}],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._stop_job("queued-job")

        updates = [(q, p) for q, p in fake_conn.queries if "SET" in q]
        assert len(updates) >= 1

    @pytest.mark.asyncio
    async def test_stop_running_container(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT container_name": [{"container_name": "ijm-running1", "status": JobStatus.RUNNING}],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._stop_job("running-job")

    @pytest.mark.asyncio
    async def test_stop_already_succeeded(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT container_name": [{"container_name": "ijm-done", "status": JobStatus.SUCCEEDED}],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._stop_job("done-job")

        # No docker stop should have been issued
        mock_run.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_with_docker_failure(self) -> None:
        responses: dict[str, list[dict[str, object] | None]] = {
            "SELECT container_name": [{"container_name": "ijm-fail", "status": JobStatus.RUNNING}],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(stdout="", returncode=1, stderr="error"),
            ]
            await worker._stop_job("fail-stop-job")

    @pytest.mark.asyncio
    async def test_handles_exception(self) -> None:
        """Exception during _stop_job should be logged, not crash."""
        worker = JobWorker()
        worker.connect_db = AsyncMock(side_effect=Exception("db connection failed"))
        await worker._stop_job("any-job-id")


# ---------------------------------------------------------------------------
# _subscribe_to_events
# ---------------------------------------------------------------------------


class TestSubscribeToEvents:
    @pytest.mark.asyncio
    async def test_subscribes_to_all_subjects(self) -> None:
        worker = JobWorker()
        worker.js = AsyncMock()
        await worker._subscribe_to_events()
        assert worker.js.subscribe.call_count == 3

    @pytest.mark.asyncio
    async def test_js_none_raises(self) -> None:
        worker = JobWorker()
        worker.js = None
        with pytest.raises(RuntimeError):
            await worker._subscribe_to_events()

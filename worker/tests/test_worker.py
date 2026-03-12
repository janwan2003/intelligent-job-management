"""Tests for the job worker."""

import asyncio
import contextlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from shared.constants import NATS_SUBJECT_PROFILING_COMPLETE, JobStatus

from constants import (
    CHECKPOINT_MOUNT_PATH,
    CONTAINER_NAME_PREFIX,
    DOCKER_STOP_GRACE_SECONDS,
    JOB_ID_DISPLAY_LENGTH,
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
            ckpt_host_path="/actual/host/path/data/checkpoints/xyz",
            runs_host_path="/actual/host/path/data/runs/xyz",
            image="img:latest",
            command=["echo", "hi"],
        )
        # None of the -v arguments should reference /host/
        v_args = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-v"]
        for v in v_args:
            assert not v.startswith("/host/"), (
                f"Volume mount '{v}' uses container-internal /host/ path. "
                "Docker daemon resolves -v paths on the HOST filesystem."
            )

    def test_empty_command(self) -> None:
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-test",
            ckpt_host_path="/data/ckpt",
            runs_host_path="/data/runs",
            image="img:latest",
            command=[],
        )
        assert cmd[-1] == "img:latest"

    def test_env_vars_placed_before_image(self) -> None:
        """Environment variables should appear as -e flags before the image name."""
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-test",
            ckpt_host_path="/data/ckpt",
            runs_host_path="/data/runs",
            image="ijm-runtime:dev",
            command=["python", "train.py"],
            env_vars={"MAX_STEPS": "100", "FOO": "bar"},
        )
        image_idx = cmd.index("ijm-runtime:dev")
        # Each -e flag and its value should come before the image
        e_indices = [i for i, arg in enumerate(cmd) if arg == "-e"]
        assert len(e_indices) == 2
        for idx in e_indices:
            assert idx < image_idx
            # The value follows immediately after -e
            assert "=" in cmd[idx + 1]

    def test_env_vars_values_correct(self) -> None:
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-test",
            ckpt_host_path="/data/ckpt",
            runs_host_path="/data/runs",
            image="img:latest",
            command=["echo"],
            env_vars={"MAX_STEPS": "200"},
        )
        e_idx = cmd.index("-e")
        assert cmd[e_idx + 1] == "MAX_STEPS=200"

    def test_no_env_vars_by_default(self) -> None:
        """When env_vars is None (default), no -e flags should appear."""
        cmd = JobWorker.build_docker_cmd(
            container_name="ijm-test",
            ckpt_host_path="/data/ckpt",
            runs_host_path="/data/runs",
            image="img:latest",
            command=["echo"],
        )
        assert "-e" not in cmd


class TestHostProjectRoot:
    """Tests for HOST_PROJECT_ROOT resolution."""

    def test_defaults_to_host_root(self) -> None:
        """When HOST_PROJECT_ROOT is unset, falls back to HOST_ROOT."""
        with patch.dict(os.environ, {"HOST_ROOT": "/host"}, clear=False):
            os.environ.pop("HOST_PROJECT_ROOT", None)
            w = JobWorker()
            assert w.host_project_root == "/host"

    def test_normpath_resolves_dotdot(self) -> None:
        """os.path.normpath resolves parent-dir references in the env var."""
        with patch.dict(
            os.environ,
            {
                "HOST_ROOT": "/host",
                "HOST_PROJECT_ROOT": "/home/user/project/infra/..",
            },
            clear=False,
        ):
            w = JobWorker()
            assert w.host_project_root == "/home/user/project"

    def test_explicit_host_project_root(self) -> None:
        with patch.dict(
            os.environ,
            {
                "HOST_ROOT": "/host",
                "HOST_PROJECT_ROOT": "/home/janek/myproject",
            },
            clear=False,
        ):
            w = JobWorker()
            assert w.host_project_root == "/home/janek/myproject"
            # host_root stays as the container-internal path
            assert w.host_root == "/host"

    def test_volume_paths_derived_from_host_project_root(self) -> None:
        """End-to-end: worker with HOST_PROJECT_ROOT produces correct volume paths."""
        with patch.dict(
            os.environ,
            {
                "HOST_ROOT": "/host",
                "HOST_PROJECT_ROOT": "/home/user/project/infra/..",
            },
            clear=False,
        ):
            w = JobWorker()
            job_id = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"
            ckpt = str(os.path.join(w.host_project_root, "data", "checkpoints", job_id))
            runs = str(os.path.join(w.host_project_root, "data", "runs", job_id))
            cmd = w.build_docker_cmd(
                container_name=f"ijm-{job_id[:8]}",
                ckpt_host_path=ckpt,
                runs_host_path=runs,
                image="ijm-runtime:dev",
                command=["python", "train.py"],
            )
            # Volume source paths should be fully resolved
            v_args = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-v"]
            assert v_args[0] == f"/home/user/project/data/checkpoints/{job_id}:{CHECKPOINT_MOUNT_PATH}"
            assert v_args[1] == f"/home/user/project/data/runs/{job_id}:{RUNS_MOUNT_PATH}"


class TestProgressRegex:
    """Tests for progress parsing from training output."""

    def test_matches_step_pattern(self) -> None:
        line = "2026-02-12 13:45:00 - __main__ - INFO - Step 50/10000 - Loss: 0.123456"
        match = PROGRESS_RE.search(line)
        assert match is not None
        assert match.group(1) == "50"
        assert match.group(2) == "10000"

    def test_matches_high_step(self) -> None:
        line = "Step 9999/10000 - Loss: 0.000001"
        match = PROGRESS_RE.search(line)
        assert match is not None
        assert match.group(1) == "9999"
        assert match.group(2) == "10000"

    def test_no_match_on_unrelated(self) -> None:
        line = "Loading checkpoint from /checkpoints/latest.pt"
        assert PROGRESS_RE.search(line) is None

    def test_no_match_on_partial(self) -> None:
        line = "Step 50 completed"
        assert PROGRESS_RE.search(line) is None

    def test_matches_step_with_spaces(self) -> None:
        line = "Step  100/5000"
        match = PROGRESS_RE.search(line)
        assert match is not None
        assert match.group(1) == "100"


# ---------------------------------------------------------------------------
# Concurrent job dispatch tests
# ---------------------------------------------------------------------------


class TestConcurrentDispatch:
    """Verify that the worker dispatches multiple jobs concurrently."""

    @pytest.mark.asyncio
    async def test_job_runner_dispatches_concurrently(self) -> None:
        """Multiple jobs in the queue should all start without waiting
        for the previous one to finish (concurrent, not sequential)."""
        worker = JobWorker()
        started: list[str] = []
        finished: list[str] = []

        async def slow_run_job(job_id: str) -> None:
            started.append(job_id)
            await asyncio.sleep(0.15)
            finished.append(job_id)

        worker._run_job = slow_run_job
        # Enqueue 3 jobs
        await worker.job_queue.put(("run", "job-1"))
        await worker.job_queue.put(("run", "job-2"))
        await worker.job_queue.put(("run", "job-3"))

        # Start the dispatcher
        runner = asyncio.create_task(worker._job_runner())

        # Give the dispatcher time to pick up all 3 from the queue
        await asyncio.sleep(0.05)

        # All 3 should have started (concurrent dispatch)
        assert len(started) == 3, f"Expected 3 jobs started concurrently, got {len(started)}: {started}"
        # But none should have finished yet (they sleep 0.15s)
        assert len(finished) == 0

        # Wait for all to finish
        await asyncio.sleep(0.2)
        assert len(finished) == 3

        worker.running = False
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner

    @pytest.mark.asyncio
    async def test_running_jobs_tracked_in_dict(self) -> None:
        """running_jobs dict should track all concurrently running jobs."""
        worker = JobWorker()
        worker.database_url = "mock://db"

        barrier = asyncio.Event()

        async def blocking_run_job(job_id: str) -> None:
            worker.running_jobs[job_id] = MagicMock()  # Simulate process tracking
            await barrier.wait()
            worker.running_jobs.pop(job_id, None)

        worker._run_job = blocking_run_job
        await worker.job_queue.put(("run", "job-a"))
        await worker.job_queue.put(("run", "job-b"))

        runner = asyncio.create_task(worker._job_runner())
        await asyncio.sleep(0.05)

        # Both should be tracked
        assert "job-a" in worker.running_jobs
        assert "job-b" in worker.running_jobs
        assert len(worker.running_jobs) == 2

        # Release the barrier — jobs finish
        barrier.set()
        await asyncio.sleep(0.05)
        assert len(worker.running_jobs) == 0

        worker.running = False
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


# ---------------------------------------------------------------------------
# Status transition tests (PROFILING vs RUNNING)
# ---------------------------------------------------------------------------


def _make_fake_worker_db(
    *,
    is_profiling_run: bool,
    status: str = "QUEUED",
    prev_exit_code: int | None = None,
) -> tuple[JobWorker, list[tuple[str, tuple[object, ...]]]]:
    """Create a JobWorker with mocked DB that returns the given job metadata.

    Returns (worker, queries_list) where queries_list captures all SQL
    executed during _run_job.
    """
    queries: list[tuple[str, tuple[object, ...]]] = []

    class FakeCursor:
        def __init__(self) -> None:
            self._last_query = ""

        async def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
            queries.append((query, params))
            self._last_query = query

        async def fetchone(self) -> tuple[object, ...] | None:
            if "SELECT image" in self._last_query:
                return (
                    "img:latest",
                    ["python", "train.py"],
                    status,
                    is_profiling_run,
                    100,
                    prev_exit_code,
                    50,  # log_interval
                    10000,  # epochs_total
                    "node-1",  # assigned_node
                )
            if "SELECT status" in self._last_query:
                return (JobStatus.PROFILING if is_profiling_run else JobStatus.RUNNING,)
            if "is_profiling_run" in self._last_query:
                return (is_profiling_run, {"A40": 2}, "node-1")
            return None

        async def __aenter__(self) -> "FakeCursor":
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        async def commit(self) -> None:
            pass

        async def close(self) -> None:
            pass

    worker = JobWorker()
    worker.database_url = "mock://db"
    worker.host_root = "/tmp/test-host"
    worker.host_project_root = "/tmp/test-host"
    worker.connect_db = AsyncMock(return_value=FakeConn())
    worker.js = AsyncMock()

    return worker, queries


class TestStatusTransitions:
    """Verify that _run_job sets PROFILING or RUNNING based on is_profiling_run."""

    @pytest.mark.asyncio
    async def test_profiling_run_sets_profiling_status(self, tmp_path: Path) -> None:
        """When is_profiling_run=True, worker must set status to PROFILING, not RUNNING."""
        worker, queries = _make_fake_worker_db(is_profiling_run=True)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job("job-prof-001")

        # Find the UPDATE that sets status
        status_updates = [(q, p) for q, p in queries if "SET status" in q and len(p) >= 1]
        # The first status update should be PROFILING
        assert any(p[0] == JobStatus.PROFILING for q, p in status_updates), (
            f"Expected PROFILING status, got updates: {status_updates}"
        )
        assert not any(p[0] == JobStatus.RUNNING for q, p in status_updates), (
            "Should NOT have set RUNNING status for a profiling run"
        )

    @pytest.mark.asyncio
    async def test_standard_run_sets_running_status(self, tmp_path: Path) -> None:
        """When is_profiling_run=False, worker must set status to RUNNING, not PROFILING."""
        worker, queries = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job("job-run-001")

        status_updates = [(q, p) for q, p in queries if "SET status" in q and len(p) >= 1]
        assert any(p[0] == JobStatus.RUNNING for q, p in status_updates), (
            f"Expected RUNNING status, got updates: {status_updates}"
        )
        assert not any(p[0] == JobStatus.PROFILING for q, p in status_updates), (
            "Should NOT have set PROFILING status for a standard run"
        )

    @pytest.mark.asyncio
    async def test_profiling_run_sets_max_steps_env(self, tmp_path: Path) -> None:
        """Profiling runs should pass MAX_STEPS env var to docker."""
        worker, _ = _make_fake_worker_db(is_profiling_run=True)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job("job-prof-002")

        # Check that the docker command includes MAX_STEPS and LOG_INTERVAL
        cmd = mock_popen.call_args[0][0]
        e_indices = [i for i, c in enumerate(cmd) if c == "-e"]
        env_pairs = [cmd[i + 1] for i in e_indices]
        assert "MAX_STEPS=100" in env_pairs
        assert "LOG_INTERVAL=50" in env_pairs

    @pytest.mark.asyncio
    async def test_standard_run_passes_env_vars(self, tmp_path: Path) -> None:
        """Standard runs pass LOG_INTERVAL and MAX_STEPS (from epochs_total)."""
        worker, _ = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job("job-run-002")

        cmd = mock_popen.call_args[0][0]
        e_indices = [i for i, c in enumerate(cmd) if c == "-e"]
        env_pairs = [cmd[i + 1] for i in e_indices]
        assert "LOG_INTERVAL=50" in env_pairs
        assert "MAX_STEPS=10000" in env_pairs

    @pytest.mark.asyncio
    async def test_profiling_run_uses_isolated_checkpoint_dir(self, tmp_path: Path) -> None:
        """Profiling runs must use a .profiling/ subdirectory so real checkpoints are preserved."""
        worker, _ = _make_fake_worker_db(is_profiling_run=True)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        # Create a real training checkpoint (should be preserved)
        job_id = "job-ckpt-isolate"
        ckpt_dir = tmp_path / "data" / "checkpoints" / job_id
        ckpt_dir.mkdir(parents=True)
        (ckpt_dir / "latest.pt").write_text("real training checkpoint")

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job(job_id)

        # Real checkpoint should still exist
        assert (ckpt_dir / "latest.pt").exists(), "Profiling run should NOT touch real training checkpoints"
        assert (ckpt_dir / "latest.pt").read_text() == "real training checkpoint"

        # Docker command should mount the .profiling/ subdirectory
        cmd = mock_popen.call_args[0][0]
        v_indices = [i for i, c in enumerate(cmd) if c == "-v"]
        ckpt_mount = cmd[v_indices[0] + 1]  # First -v is checkpoint mount
        assert ".profiling" in ckpt_mount, f"Profiling run should mount .profiling/ subdir, got: {ckpt_mount}"

    @pytest.mark.asyncio
    async def test_standard_run_preserves_checkpoints(self, tmp_path: Path) -> None:
        """Standard runs must NOT clear checkpoints (resume from where they left off)."""
        worker, _ = _make_fake_worker_db(is_profiling_run=False, prev_exit_code=0)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        job_id = "job-ckpt-keep"
        ckpt_dir = tmp_path / "data" / "checkpoints" / job_id
        ckpt_dir.mkdir(parents=True)
        (ckpt_dir / "latest.pt").write_text("keep this checkpoint")

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job(job_id)

        # Checkpoint should still exist
        assert (ckpt_dir / "latest.pt").exists(), "Standard run should preserve checkpoints for resume"

        # Docker command should mount the normal checkpoint dir (no .profiling)
        cmd = mock_popen.call_args[0][0]
        v_indices = [i for i, c in enumerate(cmd) if c == "-v"]
        ckpt_mount = cmd[v_indices[0] + 1]
        assert ".profiling" not in ckpt_mount, f"Standard run should mount normal checkpoint dir, got: {ckpt_mount}"


# ---------------------------------------------------------------------------
# Flexible DB mock helpers
# ---------------------------------------------------------------------------


class FlexFakeCursor:
    """Cursor that returns configurable rows keyed by query substring."""

    def __init__(self, responses: dict[str, list[tuple[object, ...] | None]]) -> None:
        self._responses = responses
        self._last_query = ""
        self._executions: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
        self._executions.append((query, params))
        self._last_query = query

    async def fetchone(self) -> tuple[object, ...] | None:
        for key, rows in self._responses.items():
            if key in self._last_query and rows:
                return rows.pop(0)
        return None

    async def fetchall(self) -> list[tuple[object, ...]]:
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

    def __init__(self, responses: dict[str, list[tuple[object, ...] | None]]) -> None:
        self._responses = responses
        self.queries: list[tuple[str, tuple[object, ...]]] = []
        self._cursor = FlexFakeCursor(responses)

    def cursor(self) -> FlexFakeCursor:
        # Share query log
        cur = FlexFakeCursor(self._responses)
        cur._executions = self.queries
        return cur

    async def commit(self) -> None:
        pass

    async def close(self) -> None:
        pass


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
            mock_conn.assert_called_once_with("postgresql://localhost/test")
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
    async def test_swallows_existing_stream_error(self) -> None:
        worker = JobWorker()
        worker.js = AsyncMock()
        worker.js.add_stream.side_effect = Exception("stream already exists")
        # Should not raise
        await worker._ensure_streams()


# ---------------------------------------------------------------------------
# _reconcile_job_states
# ---------------------------------------------------------------------------


class TestReconcileJobStates:
    @pytest.mark.asyncio
    async def test_marks_orphaned_running_jobs_as_failed(self) -> None:
        """RUNNING job with no container should be marked FAILED."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT id": [
                ("job-1", "ijm-job-1", JobStatus.RUNNING),
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._reconcile_job_states()

        # Should have issued an UPDATE to FAILED
        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert len(updates) == 1
        assert updates[0][1][0] == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_skips_jobs_with_existing_containers(self) -> None:
        """RUNNING job with a matching container should NOT be reconciled."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT id": [
                ("job-1", "ijm-job-1xxx", JobStatus.RUNNING),
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            # Container is present
            mock_run.return_value = MagicMock(stdout="ijm-job-1xxx\n", returncode=0)
            await worker._reconcile_job_states()

        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_reconcile_uses_generated_name_when_none(self) -> None:
        """When container_name is NULL, worker generates it from job_id prefix."""
        job_id = "abcdefgh-1234-5678-9012-aabbccddeeff"
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT id": [
                (job_id, None, JobStatus.RUNNING),
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            # No containers running
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._reconcile_job_states()

        # Job should be marked FAILED because generated name wasn't in containers
        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert len(updates) == 1
        assert updates[0][1][0] == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_reconcile_handles_queued_jobs(self) -> None:
        """QUEUED jobs without containers should NOT be reconciled (they haven't started)."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT id": [
                ("job-q", None, JobStatus.QUEUED),
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._reconcile_job_states()

        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_reconcile_handles_db_error(self) -> None:
        """DB errors during reconciliation should be logged, not crash."""
        worker = JobWorker()
        worker.connect_db = AsyncMock(side_effect=Exception("connection refused"))
        # Should not raise
        await worker._reconcile_job_states()

    @pytest.mark.asyncio
    async def test_reconcile_no_active_jobs(self) -> None:
        """No active jobs means no reconciliation needed."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT id": [],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._reconcile_job_states()

        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert len(updates) == 0


# ---------------------------------------------------------------------------
# _pickup_queued_jobs
# ---------------------------------------------------------------------------


class TestPickupQueuedJobs:
    @pytest.mark.asyncio
    async def test_enqueues_queued_jobs(self) -> None:
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT id": [("job-a",), ("job-b",)],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._pickup_queued_jobs()

        assert worker.job_queue.qsize() == 2
        item1 = await worker.job_queue.get()
        item2 = await worker.job_queue.get()
        assert item1 == ("run", "job-a")
        assert item2 == ("run", "job-b")

    @pytest.mark.asyncio
    async def test_no_queued_jobs(self) -> None:
        responses: dict[str, list[tuple[object, ...] | None]] = {
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
# NATS message handlers
# ---------------------------------------------------------------------------


def _make_nats_msg(data: dict[str, object]) -> MagicMock:
    """Create a mock NATS message with given JSON data."""
    msg = MagicMock()
    msg.data = json.dumps(data).encode()
    msg.ack = AsyncMock()
    return msg


class TestHandleJobSubmitted:
    @pytest.mark.asyncio
    async def test_enqueues_and_acks(self) -> None:
        worker = JobWorker()
        msg = _make_nats_msg({"job_id": "job-123"})

        await worker._handle_job_submitted(msg)

        assert worker.job_queue.qsize() == 1
        item = await worker.job_queue.get()
        assert item == ("run", "job-123")
        msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_malformed_message(self) -> None:
        worker = JobWorker()
        msg = MagicMock()
        msg.data = b"not json"
        msg.nak = AsyncMock()
        # Should not raise
        await worker._handle_job_submitted(msg)
        assert worker.job_queue.qsize() == 0
        msg.nak.assert_called_once()


class TestHandleStopRequested:
    @pytest.mark.asyncio
    async def test_stops_tracked_running_job(self) -> None:
        """When process is in running_jobs, it should update DB and stop container."""
        worker = JobWorker()
        job_id = "abcdefgh-1234-5678-9012-aabbccddeeff"
        worker.running_jobs[job_id] = MagicMock()

        fake_conn = FlexFakeConn({})
        worker.connect_db = AsyncMock(return_value=fake_conn)

        msg = _make_nats_msg({"job_id": job_id})

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            await worker._handle_stop_requested(msg)

        # Should have updated status to PREEMPTED
        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert any(p[0] == JobStatus.PREEMPTED for _, p in updates)

        # Should have called docker stop
        mock_run.assert_called_once()
        stop_cmd = mock_run.call_args[0][0]
        assert "docker" in stop_cmd
        assert "stop" in stop_cmd

        msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_delegates_to_stop_job_for_untracked(self) -> None:
        """When job is NOT in running_jobs, it delegates to _stop_job."""
        worker = JobWorker()
        job_id = "untracked-job-id"
        worker._stop_job = AsyncMock()

        msg = _make_nats_msg({"job_id": job_id})
        await worker._handle_stop_requested(msg)

        worker._stop_job.assert_called_once_with(job_id)
        msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_error(self) -> None:
        worker = JobWorker()
        msg = MagicMock()
        msg.data = b"not json"
        msg.nak = AsyncMock()
        await worker._handle_stop_requested(msg)
        msg.nak.assert_called_once()


class TestHandleResumeRequested:
    @pytest.mark.asyncio
    async def test_enqueues_and_acks(self) -> None:
        worker = JobWorker()
        msg = _make_nats_msg({"job_id": "job-resume-1"})

        await worker._handle_resume_requested(msg)

        assert worker.job_queue.qsize() == 1
        item = await worker.job_queue.get()
        assert item == ("run", "job-resume-1")
        msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_error(self) -> None:
        worker = JobWorker()
        msg = MagicMock()
        msg.data = b"bad"
        msg.nak = AsyncMock()
        await worker._handle_resume_requested(msg)
        assert worker.job_queue.qsize() == 0
        msg.nak.assert_called_once()


# ---------------------------------------------------------------------------
# _update_progress
# ---------------------------------------------------------------------------


class TestUpdateProgress:
    @pytest.mark.asyncio
    async def test_updates_progress_in_db(self) -> None:
        responses: dict[str, list[tuple[object, ...] | None]] = {}
        fake_conn = FlexFakeConn(responses)

        worker = JobWorker()
        await worker._update_progress(fake_conn, "job-prog-1", "50/100")  # type: ignore[arg-type]

        updates = [(q, p) for q, p in fake_conn.queries if "progress" in q.lower()]
        assert len(updates) == 1
        assert updates[0][1][0] == "50/100"


# ---------------------------------------------------------------------------
# _run_job edge cases
# ---------------------------------------------------------------------------


class TestRunJobEdgeCases:
    @pytest.mark.asyncio
    async def test_job_not_found(self) -> None:
        """When job doesn't exist in DB, _run_job returns early."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT image": [None],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._run_job("nonexistent-job")
        # No crash, no updates
        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_job_not_in_runnable_status(self) -> None:
        """Job with SUCCEEDED status should be skipped."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT image": [
                (
                    "img:latest",
                    ["python", "train.py"],
                    JobStatus.SUCCEEDED,
                    False,
                    100,
                    None,
                    50,  # log_interval
                    10000,  # epochs_total
                    "node-1",  # assigned_node
                ),
            ],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._run_job("succeeded-job")
        updates = [(q, p) for q, p in fake_conn.queries if "SET status" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_failed_exit_code_sets_failed(self, tmp_path: Path) -> None:
        """Non-zero exit code should mark the job as FAILED."""
        worker, queries = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 1  # Non-zero exit
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job("job-fail-001")

        status_updates = [(q, p) for q, p in queries if "SET status" in q]
        assert any(p[0] == JobStatus.FAILED for _, p in status_updates)

    @pytest.mark.asyncio
    async def test_preempted_status_preserved(self, tmp_path: Path) -> None:
        """If job was marked PREEMPTED during run, that status should be kept."""
        # Return PREEMPTED when checking current status
        queries: list[tuple[str, tuple[object, ...]]] = []

        class PreemptedCursor:
            def __init__(self) -> None:
                self._last_query = ""

            async def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
                queries.append((query, params))
                self._last_query = query

            async def fetchone(self) -> tuple[object, ...] | None:
                if "SELECT image" in self._last_query:
                    return (
                        "img:latest",
                        ["python", "train.py"],
                        JobStatus.QUEUED,
                        False,
                        100,
                        0,
                        50,  # log_interval
                        10000,  # epochs_total
                        "node-1",  # assigned_node
                    )
                if "SELECT status" in self._last_query:
                    return (JobStatus.PREEMPTED,)  # Already preempted
                return None

            async def __aenter__(self) -> "PreemptedCursor":
                return self

            async def __aexit__(self, *_args: object) -> None:
                pass

        class PreemptedConn:
            def cursor(self) -> PreemptedCursor:
                return PreemptedCursor()

            async def commit(self) -> None:
                pass

            async def close(self) -> None:
                pass

        worker = JobWorker()
        worker.database_url = "mock://db"
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)
        worker.connect_db = AsyncMock(return_value=PreemptedConn())
        worker.js = AsyncMock()

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 137  # Killed
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job("job-preempted-001")

        # Should have an exit_code update but NOT a status change to SUCCEEDED/FAILED
        exit_updates = [(q, p) for q, p in queries if "exit_code" in q and "SET status" not in q]
        assert len(exit_updates) >= 1
        # Should NOT set SUCCEEDED or FAILED
        status_updates = [(q, p) for q, p in queries if "SET status" in q]
        assert not any(p[0] in (JobStatus.SUCCEEDED, JobStatus.FAILED) for _, p in status_updates)

    @pytest.mark.asyncio
    async def test_first_full_run_clears_checkpoints(self, tmp_path: Path) -> None:
        """First full run after profiling (exit_code=None) should clear checkpoint dir."""
        worker, _ = _make_fake_worker_db(is_profiling_run=False, prev_exit_code=None)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        job_id = "job-clear-ckpt"
        ckpt_dir = tmp_path / "data" / "checkpoints" / job_id
        ckpt_dir.mkdir(parents=True)
        (ckpt_dir / "latest.pt").write_text("stale profiling checkpoint")

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job(job_id)

        # Checkpoint should have been cleared
        assert not (ckpt_dir / "latest.pt").exists()

    @pytest.mark.asyncio
    async def test_exception_during_run_marks_failed(self, tmp_path: Path) -> None:
        """If docker Popen raises, job should be marked FAILED."""
        worker, queries = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = OSError("docker not found")

            await worker._run_job("job-exception-001")

        status_updates = [(q, p) for q, p in queries if "SET status" in q]
        assert any(p[0] == JobStatus.FAILED for _, p in status_updates)

    @pytest.mark.asyncio
    async def test_profiling_run_clears_profiling_dir(self, tmp_path: Path) -> None:
        """Profiling runs should clear old profiling checkpoint files."""
        worker, _ = _make_fake_worker_db(is_profiling_run=True)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        job_id = "job-prof-clear"
        prof_dir = tmp_path / "data" / "checkpoints" / job_id / ".profiling"
        prof_dir.mkdir(parents=True)
        (prof_dir / "old_checkpoint.pt").write_text("old profiling data")

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job(job_id)

        # Old profiling checkpoint should be cleared
        assert not (prof_dir / "old_checkpoint.pt").exists()

    @pytest.mark.asyncio
    async def test_progress_null_on_status_transition(self, tmp_path: Path) -> None:
        """Status update should clear progress to NULL."""
        worker, queries = _make_fake_worker_db(is_profiling_run=False)
        worker.host_root = str(tmp_path)
        worker.host_project_root = str(tmp_path)

        with patch("worker.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = None
            mock_process.wait.return_value = 0
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            await worker._run_job("job-progress-null")

        # First SET status query should include progress = NULL
        status_updates = [q for q, _ in queries if "SET status" in q and "progress = NULL" in q]
        assert len(status_updates) >= 1


# ---------------------------------------------------------------------------
# _check_and_report_profiling
# ---------------------------------------------------------------------------


class TestCheckAndReportProfiling:
    @pytest.mark.asyncio
    async def test_profiling_run_publishes_to_nats(self) -> None:
        """Profiling run should publish result to NATS and return True."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "is_profiling_run": [(True, {"A40": 2}, "node-1")],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.js = AsyncMock()

        result = await worker._check_and_report_profiling(
            fake_conn,  # type: ignore[arg-type]
            "job-prof-report",
            datetime.now(UTC),
            step_timestamps=[],
        )

        assert result is True
        worker.js.publish.assert_called_once()
        call_args = worker.js.publish.call_args
        assert call_args[0][0] == NATS_SUBJECT_PROFILING_COMPLETE
        payload = json.loads(call_args[0][1].decode())
        assert payload["job_id"] == "job-prof-report"
        assert payload["gpu_config"] == {"A40": 2}
        assert payload["node_id"] == "node-1"
        assert "duration_seconds" in payload

    @pytest.mark.asyncio
    async def test_standard_run_returns_false(self) -> None:
        """Non-profiling run should return False."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "is_profiling_run": [(False, None, None)],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.js = AsyncMock()

        result = await worker._check_and_report_profiling(
            fake_conn,  # type: ignore[arg-type]
            "job-std",
            datetime.now(UTC),
        )

        assert result is False
        worker.js.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_meta_returns_false(self) -> None:
        """When job not found, return False."""
        responses: dict[str, list[tuple[object, ...] | None]] = {}
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.js = AsyncMock()

        result = await worker._check_and_report_profiling(
            fake_conn,  # type: ignore[arg-type]
            "job-gone",
            datetime.now(UTC),
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_profiling_without_jetstream(self) -> None:
        """Profiling run with js=None should still return True but not publish."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "is_profiling_run": [(True, {"L40S": 1}, "node-2")],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.js = None

        result = await worker._check_and_report_profiling(
            fake_conn,  # type: ignore[arg-type]
            "job-no-js",
            datetime.now(UTC),
        )

        assert result is True

    @pytest.mark.asyncio
    async def test_warmup_excluded_from_duration(self) -> None:
        """First interval (warmup) should be dropped, rest averaged."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "is_profiling_run": [(True, {"A40": 1}, "node-1")],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.js = AsyncMock()

        # 4 timestamps → 3 intervals; first interval (2.0s) is warmup
        base = 1000.0
        step_timestamps = [
            (10, base),
            (20, base + 2.0),  # interval 0: 2.0s (warmup, dropped)
            (30, base + 2.5),  # interval 1: 0.5s
            (40, base + 3.0),  # interval 2: 0.5s
        ]

        result = await worker._check_and_report_profiling(
            fake_conn,  # type: ignore[arg-type]
            "job-warmup",
            datetime.now(UTC),
            step_timestamps=step_timestamps,
        )

        assert result is True
        payload = json.loads(worker.js.publish.call_args[0][1].decode())
        # steady mean = 0.5s, total_steps = 40 → 0.5 * 40 = 20.0
        assert abs(payload["duration_seconds"] - 20.0) < 0.01


# ---------------------------------------------------------------------------
# _stop_job
# ---------------------------------------------------------------------------


class TestStopJob:
    @pytest.mark.asyncio
    async def test_stops_existing_container(self) -> None:
        """When container exists, docker stop should be called and status set to PREEMPTED."""
        job_id = "abcdefgh-stop-existing"
        container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT container_name": [(container_name, JobStatus.RUNNING)],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            # First call: docker ps — container exists
            # Second call: docker stop — success
            mock_run.side_effect = [
                MagicMock(stdout=f"{container_name}\n", returncode=0),
                MagicMock(returncode=0, stdout="", stderr=""),
            ]
            await worker._stop_job(job_id)

        # docker stop should have been called
        assert mock_run.call_count == 2
        stop_call = mock_run.call_args_list[1]
        assert "stop" in stop_call[0][0]
        assert str(DOCKER_STOP_GRACE_SECONDS) in stop_call[0][0]

        # Status should be updated to PREEMPTED
        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert any(p[0] == JobStatus.PREEMPTED for _, p in updates)

    @pytest.mark.asyncio
    async def test_job_not_found(self) -> None:
        """When job is not in DB, _stop_job returns early."""
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT container_name": [None],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        await worker._stop_job("nonexistent-job")
        # No docker commands should have been issued
        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_container_missing_queued_job_preempted(self) -> None:
        """QUEUED job with missing container should be marked PREEMPTED."""
        job_id = "queued-job-no-container"
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT container_name": [(None, JobStatus.QUEUED)],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._stop_job(job_id)

        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert any(p[0] == JobStatus.PREEMPTED for _, p in updates)

    @pytest.mark.asyncio
    async def test_container_missing_running_job_failed(self) -> None:
        """RUNNING job with missing container should be marked FAILED."""
        job_id = "running-job-no-container"
        container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT container_name": [(container_name, JobStatus.RUNNING)],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._stop_job(job_id)

        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert any(p[0] == JobStatus.FAILED for _, p in updates)

    @pytest.mark.asyncio
    async def test_container_missing_other_status_no_action(self) -> None:
        """Job with non-actionable status and missing container — no update."""
        job_id = "succeeded-job-stop"
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT container_name": [(None, JobStatus.SUCCEEDED)],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            await worker._stop_job(job_id)

        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q]
        assert len(updates) == 0

    @pytest.mark.asyncio
    async def test_docker_stop_failure(self) -> None:
        """When docker stop returns non-zero, status should NOT be updated."""
        job_id = "stop-fail-job-id12"
        container_name = f"{CONTAINER_NAME_PREFIX}{job_id[:JOB_ID_DISPLAY_LENGTH]}"
        responses: dict[str, list[tuple[object, ...] | None]] = {
            "SELECT container_name": [(container_name, JobStatus.RUNNING)],
        }
        fake_conn = FlexFakeConn(responses)
        worker = JobWorker()
        worker.connect_db = AsyncMock(return_value=fake_conn)

        with patch("worker.subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(stdout=f"{container_name}\n", returncode=0),  # docker ps
                MagicMock(returncode=1, stdout="", stderr="cannot stop"),  # docker stop fails
            ]
            await worker._stop_job(job_id)

        # Should NOT have updated status since stop failed
        updates = [(q, p) for q, p in fake_conn.queries if "UPDATE" in q and "status" in q.lower()]
        assert len(updates) == 0

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
    async def test_subscribes_to_three_subjects(self) -> None:
        worker = JobWorker()
        worker.js = AsyncMock()

        await worker._subscribe_to_events()

        assert worker.js.subscribe.call_count == 3


# ---------------------------------------------------------------------------
# _job_runner error path
# ---------------------------------------------------------------------------


class TestJobRunnerErrors:
    @pytest.mark.asyncio
    async def test_continues_on_error(self) -> None:
        """Job runner should continue running after an error in _run_job."""
        worker = JobWorker()
        call_count = 0

        async def failing_run_job(job_id: str) -> None:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("simulated failure")

        worker._run_job = failing_run_job
        await worker.job_queue.put(("run", "job-err-1"))
        await worker.job_queue.put(("run", "job-err-2"))

        runner = asyncio.create_task(worker._job_runner())
        await asyncio.sleep(0.1)

        # Both jobs should have been dispatched despite errors in run tasks
        assert call_count == 2

        worker.running = False
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


# ---------------------------------------------------------------------------
# main entry point
# ---------------------------------------------------------------------------


class TestMainEntryPoint:
    @pytest.mark.asyncio
    async def test_main_creates_and_starts_worker(self) -> None:
        with patch.object(JobWorker, "start", new_callable=AsyncMock) as mock_start:
            from worker import main

            await main()
            mock_start.assert_called_once()

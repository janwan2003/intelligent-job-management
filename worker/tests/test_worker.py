"""Tests for the job worker."""

import asyncio
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest  # type: ignore[import-not-found]

from constants import (
    CHECKPOINT_MOUNT_PATH,
    JobStatus,
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
            assert (
                v_args[0]
                == f"/home/user/project/data/checkpoints/{job_id}:{CHECKPOINT_MOUNT_PATH}"
            )
            assert (
                v_args[1] == f"/home/user/project/data/runs/{job_id}:{RUNS_MOUNT_PATH}"
            )


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

    @pytest.mark.asyncio  # type: ignore[misc]
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

        worker._run_job = slow_run_job  # type: ignore[method-assign]

        # Enqueue 3 jobs
        await worker.job_queue.put(("run", "job-1"))
        await worker.job_queue.put(("run", "job-2"))
        await worker.job_queue.put(("run", "job-3"))

        # Start the dispatcher
        runner = asyncio.create_task(worker._job_runner())

        # Give the dispatcher time to pick up all 3 from the queue
        await asyncio.sleep(0.05)

        # All 3 should have started (concurrent dispatch)
        assert (
            len(started) == 3
        ), f"Expected 3 jobs started concurrently, got {len(started)}: {started}"
        # But none should have finished yet (they sleep 0.15s)
        assert len(finished) == 0

        # Wait for all to finish
        await asyncio.sleep(0.2)
        assert len(finished) == 3

        worker.running = False
        runner.cancel()
        try:
            await runner
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio  # type: ignore[misc]
    async def test_running_jobs_tracked_in_dict(self) -> None:
        """running_jobs dict should track all concurrently running jobs."""
        worker = JobWorker()
        worker.database_url = "mock://db"

        barrier = asyncio.Event()

        async def blocking_run_job(job_id: str) -> None:
            worker.running_jobs[job_id] = MagicMock()  # Simulate process tracking
            await barrier.wait()
            worker.running_jobs.pop(job_id, None)

        worker._run_job = blocking_run_job  # type: ignore[method-assign]

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
        try:
            await runner
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# Status transition tests (PROFILING vs RUNNING)
# ---------------------------------------------------------------------------


def _make_fake_worker_db(
    *,
    is_profiling_run: bool,
    status: str = "QUEUED",
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
    worker.connect_db = AsyncMock(return_value=FakeConn())  # type: ignore[method-assign]
    worker.js = AsyncMock()

    return worker, queries


class TestStatusTransitions:
    """Verify that _run_job sets PROFILING or RUNNING based on is_profiling_run."""

    @pytest.mark.asyncio  # type: ignore[misc]
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
        status_updates = [
            (q, p) for q, p in queries if "SET status" in q and len(p) >= 1
        ]
        # The first status update should be PROFILING
        assert any(
            p[0] == JobStatus.PROFILING for q, p in status_updates
        ), f"Expected PROFILING status, got updates: {status_updates}"
        assert not any(
            p[0] == JobStatus.RUNNING for q, p in status_updates
        ), "Should NOT have set RUNNING status for a profiling run"

    @pytest.mark.asyncio  # type: ignore[misc]
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

        status_updates = [
            (q, p) for q, p in queries if "SET status" in q and len(p) >= 1
        ]
        assert any(
            p[0] == JobStatus.RUNNING for q, p in status_updates
        ), f"Expected RUNNING status, got updates: {status_updates}"
        assert not any(
            p[0] == JobStatus.PROFILING for q, p in status_updates
        ), "Should NOT have set PROFILING status for a standard run"

    @pytest.mark.asyncio  # type: ignore[misc]
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

        # Check that the docker command includes MAX_STEPS
        cmd = mock_popen.call_args[0][0]
        assert "-e" in cmd, "Expected -e flag for environment variables"
        e_idx = cmd.index("-e")
        assert cmd[e_idx + 1].startswith(
            "MAX_STEPS="
        ), f"Expected MAX_STEPS env var, got: {cmd[e_idx + 1]}"

    @pytest.mark.asyncio  # type: ignore[misc]
    async def test_standard_run_no_max_steps_env(self, tmp_path: Path) -> None:
        """Standard (non-profiling) runs should NOT pass MAX_STEPS."""
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
        assert "-e" not in cmd, f"Standard run should NOT have -e flag, got: {cmd}"

    @pytest.mark.asyncio  # type: ignore[misc]
    async def test_profiling_run_uses_isolated_checkpoint_dir(
        self, tmp_path: Path
    ) -> None:
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
        assert (
            ckpt_dir / "latest.pt"
        ).exists(), "Profiling run should NOT touch real training checkpoints"
        assert (ckpt_dir / "latest.pt").read_text() == "real training checkpoint"

        # Docker command should mount the .profiling/ subdirectory
        cmd = mock_popen.call_args[0][0]
        v_indices = [i for i, c in enumerate(cmd) if c == "-v"]
        ckpt_mount = cmd[v_indices[0] + 1]  # First -v is checkpoint mount
        assert (
            ".profiling" in ckpt_mount
        ), f"Profiling run should mount .profiling/ subdir, got: {ckpt_mount}"

    @pytest.mark.asyncio  # type: ignore[misc]
    async def test_standard_run_preserves_checkpoints(self, tmp_path: Path) -> None:
        """Standard runs must NOT clear checkpoints (resume from where they left off)."""
        worker, _ = _make_fake_worker_db(is_profiling_run=False)
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
        assert (
            ckpt_dir / "latest.pt"
        ).exists(), "Standard run should preserve checkpoints for resume"

        # Docker command should mount the normal checkpoint dir (no .profiling)
        cmd = mock_popen.call_args[0][0]
        v_indices = [i for i, c in enumerate(cmd) if c == "-v"]
        ckpt_mount = cmd[v_indices[0] + 1]
        assert (
            ".profiling" not in ckpt_mount
        ), f"Standard run should mount normal checkpoint dir, got: {ckpt_mount}"

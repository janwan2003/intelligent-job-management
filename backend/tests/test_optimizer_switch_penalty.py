"""Tests for the per-migration switch penalty in optimizer.optimize()."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.optimizer as optimizer_module
from src.cluster import cluster
from src.optimizer import optimize


class _Cur:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    async def execute(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    async def fetchall(self) -> list[Any]:
        return self._rows

    async def __aenter__(self) -> "_Cur":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        pass


class _Conn:
    """Returns ``active_jobs`` for the first cursor query and
    ``profiling_rows`` for the second (matches optimize()'s order)."""

    def __init__(self, active_jobs: list[Any], profiling_rows: list[Any]) -> None:
        self._queues = [active_jobs, profiling_rows]

    def cursor(self) -> _Cur:
        return _Cur(self._queues.pop(0))


def _now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


def _job_row(
    instance_id: str,
    *,
    assigned_node: str | None,
    assigned_gpu_config: dict[str, int] | None,
    progress: str | None = None,
    epochs_total: int = 10,
    status: str = "RUNNING",
) -> tuple[Any, ...]:
    # Matches the SELECT in optimize():
    # id, job_id, status, priority, deadline, created_at, epochs_total,
    # assigned_node, assigned_gpu_config, progress
    return (
        instance_id,
        "type-x",
        status,
        3,
        _now(),  # deadline (far future irrelevant; replaced by created+1y if None)
        _now(),
        epochs_total,
        assigned_node,
        assigned_gpu_config,
        progress,
    )


def _setup_cluster() -> None:
    cluster.nodes = [
        {
            "id": "matemagician",
            "isForProfiling": False,
            "cost": 1.0,
            "resources": [{"gpu_type": "QuadroP600", "gpu_count": 2}],
        },
        {
            "id": "polimi-gpu",
            "isForProfiling": False,
            "cost": 1.0,
            "resources": [{"gpu_type": "A40", "gpu_count": 2}],
        },
    ]
    cluster.gpu_energy_costs = {
        "QuadroP600": {"1": 0.03, "2": 0.06},
        "A40": {"1": 0.15, "2": 0.28},
    }


def _mock_optimizer_response(payload: dict[str, Any]) -> Any:
    """Build a context-manager mock for httpx.AsyncClient(...) returning ``payload``."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json = MagicMock(return_value=payload)

    client = MagicMock()
    client.post = AsyncMock(return_value=resp)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    return client


@pytest.fixture(autouse=True)
def _enable_optimizer(monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_cluster()
    monkeypatch.setattr(optimizer_module, "OPTIMIZER_URL", "http://opt.test")


async def _run(
    *,
    active_jobs: list[Any],
    profiling_rows: list[Any],
    response: dict[str, Any],
    node_gpu_usage: dict[str, dict[str, int]] | None = None,
) -> Any:
    conn = _Conn(active_jobs, profiling_rows)
    client_mock = _mock_optimizer_response(response)
    with patch.object(optimizer_module.httpx, "AsyncClient", return_value=client_mock):
        return await optimize(conn, node_gpu_usage or {})


@pytest.mark.asyncio
async def test_low_benefit_migration_suppressed() -> None:
    """tardiness=0 and benefit < penalty → migration dropped."""
    active = [
        _job_row(
            "job-keep",
            assigned_node="matemagician",
            assigned_gpu_config={"QuadroP600": 1},
        ),
    ]
    # 100 s/epoch on QuadroP600x1, 99 s/epoch on A40x1 → tiny benefit.
    profiling = [
        ("type-x", {"QuadroP600": 1}, 100.0),
        ("type-x", {"A40": 1}, 99.0),
    ]
    # Optimizer wants to migrate to A40 with tardiness=0.
    response = {
        "jobs": {
            "job-keep": {"node": "polimi-gpu", "nGPUs": 1, "expected_tardiness": 0},
        },
        "estimated_cost": 0.0,
    }
    result = await _run(
        active_jobs=active,
        profiling_rows=profiling,
        response=response,
        node_gpu_usage={"matemagician": {"QuadroP600": 1}},
    )
    assert result.preempt == []
    assert result.assignments == []


@pytest.mark.asyncio
async def test_tardy_migration_kept_regardless_of_cost() -> None:
    active = [
        _job_row(
            "job-tardy",
            assigned_node="matemagician",
            assigned_gpu_config={"QuadroP600": 1},
        ),
    ]
    profiling = [
        ("type-x", {"QuadroP600": 1}, 100.0),
        ("type-x", {"A40": 1}, 99.0),
    ]
    response = {
        "jobs": {
            "job-tardy": {"node": "polimi-gpu", "nGPUs": 1, "expected_tardiness": 5.0},
        },
        "estimated_cost": 0.0,
    }
    result = await _run(
        active_jobs=active,
        profiling_rows=profiling,
        response=response,
        node_gpu_usage={"matemagician": {"QuadroP600": 1}},
    )
    assert result.preempt == ["job-tardy"]
    assert len(result.assignments) == 1
    assert result.assignments[0].node_id == "polimi-gpu"


@pytest.mark.asyncio
async def test_high_benefit_migration_kept() -> None:
    active = [
        _job_row(
            "job-fast",
            assigned_node="polimi-gpu",
            assigned_gpu_config={"A40": 1},
            epochs_total=100,
        ),
    ]
    # Currently on A40 1x at 1000 s/epoch (expensive); proposed QuadroP600 1x
    # at 1 s/epoch (cheap).  Massive benefit, far exceeds any reasonable
    # migration overhead.
    profiling = [
        ("type-x", {"A40": 1}, 1000.0),
        ("type-x", {"QuadroP600": 1}, 1.0),
    ]
    response = {
        "jobs": {
            "job-fast": {"node": "matemagician", "nGPUs": 1, "expected_tardiness": 0},
        },
        "estimated_cost": 0.0,
    }
    result = await _run(
        active_jobs=active,
        profiling_rows=profiling,
        response=response,
        node_gpu_usage={"polimi-gpu": {"A40": 1}},
    )
    assert result.preempt == ["job-fast"]
    assert len(result.assignments) == 1


@pytest.mark.asyncio
async def test_pure_permutation_short_circuits() -> None:
    """Two jobs swapping identical slots → permutation filter handles it."""
    active = [
        _job_row("job-a", assigned_node="polimi-gpu", assigned_gpu_config={"A40": 1}),
        _job_row("job-b", assigned_node="polimi-gpu", assigned_gpu_config={"A40": 1}),
    ]
    profiling = [("type-x", {"A40": 1}, 50.0)]
    # Optimizer keeps the same multiset (2x A40 on polimi-gpu) — pure swap.
    response = {
        "jobs": {
            "job-a": {"node": "polimi-gpu", "nGPUs": 1, "expected_tardiness": 0},
            "job-b": {"node": "polimi-gpu", "nGPUs": 1, "expected_tardiness": 0},
        },
        "estimated_cost": 0.0,
    }
    result = await _run(
        active_jobs=active,
        profiling_rows=profiling,
        response=response,
        node_gpu_usage={"polimi-gpu": {"A40": 2}},
    )
    assert result.preempt == []
    assert result.assignments == []


@pytest.mark.asyncio
async def test_queued_with_stale_assignment_treated_as_fresh_placement() -> None:
    """QUEUED row carrying a stale assigned_node (failed dispatch) must NOT
    be locked into currentScheduling.  Otherwise the permutation filter
    treats it as immovable and the job stays stuck forever.

    Setup: one QUEUED row already has assigned_node=polimi-gpu / A40x1
    (Phase 1b wrote it, but the worker /run failed silently).  The
    optimizer's response keeps it on polimi-gpu/A40x1 — same shape as the
    stale assignment.  Pre-fix: permutation filter triggered, no
    Assignment returned.  Post-fix: status=QUEUED excludes it from
    currentScheduling, so the optimizer treats it as a fresh placement
    and we get a real Assignment back for the dispatcher to act on.
    """
    active = [
        _job_row(
            "job-stuck",
            assigned_node="polimi-gpu",
            assigned_gpu_config={"A40": 1},
            status="QUEUED",
        ),
    ]
    profiling = [("type-x", {"A40": 1}, 50.0)]
    response = {
        "jobs": {
            "job-stuck": {"node": "polimi-gpu", "nGPUs": 1, "expected_tardiness": 0},
        },
        "estimated_cost": 0.0,
    }
    # node_gpu_usage reflects the stale assignment (the orphan slot still
    # counted in DB until the watchdog clears it).  free=1 on polimi-gpu.
    result = await _run(
        active_jobs=active,
        profiling_rows=profiling,
        response=response,
        node_gpu_usage={"polimi-gpu": {"A40": 1}},
    )
    assert result.preempt == []
    assert len(result.assignments) == 1
    assert result.assignments[0].instance_id == "job-stuck"
    assert result.assignments[0].node_id == "polimi-gpu"
    assert result.assignments[0].gpu_config == {"A40": 1}


@pytest.mark.asyncio
async def test_running_with_assignment_still_protected_by_permutation_filter() -> None:
    """Regression guard: the QUEUED-status carve-out must not weaken the
    permutation filter for RUNNING jobs.  A RUNNING row whose plan stays
    on the same slot should still be filtered out (no churn)."""
    active = [
        _job_row(
            "job-run",
            assigned_node="polimi-gpu",
            assigned_gpu_config={"A40": 1},
            status="RUNNING",
        ),
    ]
    profiling = [("type-x", {"A40": 1}, 50.0)]
    response = {
        "jobs": {
            "job-run": {"node": "polimi-gpu", "nGPUs": 1, "expected_tardiness": 0},
        },
        "estimated_cost": 0.0,
    }
    result = await _run(
        active_jobs=active,
        profiling_rows=profiling,
        response=response,
        node_gpu_usage={"polimi-gpu": {"A40": 1}},
    )
    assert result.preempt == []
    assert result.assignments == []  # same slot, RUNNING → filtered


@pytest.mark.asyncio
async def test_new_job_unaffected_by_penalty() -> None:
    """New job (not in currentScheduling) is always placed."""
    active = [
        _job_row("job-new", assigned_node=None, assigned_gpu_config=None),
    ]
    profiling = [("type-x", {"A40": 1}, 50.0)]
    response = {
        "jobs": {
            "job-new": {"node": "polimi-gpu", "nGPUs": 1, "expected_tardiness": 0},
        },
        "estimated_cost": 0.0,
    }
    result = await _run(
        active_jobs=active,
        profiling_rows=profiling,
        response=response,
    )
    assert result.preempt == []
    assert len(result.assignments) == 1
    assert result.assignments[0].instance_id == "job-new"

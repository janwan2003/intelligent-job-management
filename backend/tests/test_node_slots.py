"""Tests for ``NodeSlots`` — per-node GPU semaphore coordination."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from src.cluster import ClusterManager
from src.node_slots import NodeSlots

# ---------------------------------------------------------------------------
# Test cluster fixture
# ---------------------------------------------------------------------------


def _make_cluster() -> ClusterManager:
    """Two nodes: matemagician (2 QuadroP600), polimi-gpu (2 A40)."""
    cm = ClusterManager()
    cm.nodes = [
        {
            "id": "matemagician",
            "isForProfiling": True,
            "cost": 0.10,
            "resources": [{"gpu_type": "QuadroP600", "gpu_count": 2}],
        },
        {
            "id": "polimi-gpu",
            "isForProfiling": True,
            "cost": 0.30,
            "resources": [{"gpu_type": "A40", "gpu_count": 2}],
        },
    ]
    return cm


# ---------------------------------------------------------------------------
# FakeConn for reconcile() tests
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    async def fetchall(self) -> list[Any]:
        return self._rows

    async def fetchone(self) -> Any | None:
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    async def execute(self, _query: str, _params: tuple[Any, ...] = ()) -> _FakeCursor:
        return _FakeCursor(self._rows)


def _make_get_conn(rows: list[Any]) -> Any:
    @asynccontextmanager
    async def get_conn() -> AsyncGenerator[_FakeConn]:
        yield _FakeConn(rows)

    return get_conn


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_construction_matches_total_gpus() -> None:
    slots = NodeSlots(_make_cluster())
    assert slots.total("matemagician") == 2
    assert slots.total("polimi-gpu") == 2
    assert slots.available("matemagician") == 2
    assert slots.available("polimi-gpu") == 2


def test_construction_unknown_node_returns_zero() -> None:
    slots = NodeSlots(_make_cluster())
    assert slots.total("does-not-exist") == 0
    assert slots.available("does-not-exist") == 0


# ---------------------------------------------------------------------------
# Acquire / release accounting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acquire_then_release_round_trip() -> None:
    slots = NodeSlots(_make_cluster())
    await slots.acquire("matemagician", 1)
    assert slots.available("matemagician") == 1
    slots.release("matemagician", 1)
    assert slots.available("matemagician") == 2


@pytest.mark.asyncio
async def test_acquire_multiple_gpus() -> None:
    """A 2-GPU job should consume both permits on a 2-slot node."""
    slots = NodeSlots(_make_cluster())
    await slots.acquire("polimi-gpu", 2)
    assert slots.available("polimi-gpu") == 0
    slots.release("polimi-gpu", 2)
    assert slots.available("polimi-gpu") == 2


def test_release_unknown_node_does_not_raise() -> None:
    slots = NodeSlots(_make_cluster())
    # Tolerated so a stale ``ijm_slot_freed`` notification can't crash the listener.
    slots.release("removed-node", 1)


@pytest.mark.asyncio
async def test_acquire_unknown_node_raises() -> None:
    slots = NodeSlots(_make_cluster())
    with pytest.raises(KeyError):
        await slots.acquire("removed-node", 1)


# ---------------------------------------------------------------------------
# Blocking semantics: acquire waits for release
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acquire_blocks_when_full_and_unblocks_on_release() -> None:
    slots = NodeSlots(_make_cluster())
    # Drain matemagician (2 permits)
    await slots.acquire("matemagician", 2)
    assert slots.available("matemagician") == 0

    # The next acquire should block until release.
    waiter = asyncio.create_task(slots.acquire("matemagician", 1))
    await asyncio.sleep(0.05)
    assert not waiter.done()

    # Release one permit; the waiter wakes up.
    slots.release("matemagician", 1)
    await asyncio.wait_for(waiter, timeout=1.0)
    assert waiter.done()
    assert slots.available("matemagician") == 0  # waiter took it


@pytest.mark.asyncio
async def test_two_waiters_wake_in_order() -> None:
    """FIFO-ish: first waiter gets permit when first release fires."""
    slots = NodeSlots(_make_cluster())
    await slots.acquire("polimi-gpu", 2)

    order: list[str] = []

    async def waiter(name: str) -> None:
        await slots.acquire("polimi-gpu", 1)
        order.append(name)

    t1 = asyncio.create_task(waiter("first"))
    await asyncio.sleep(0.01)  # ensure t1 suspends before t2 starts
    t2 = asyncio.create_task(waiter("second"))
    await asyncio.sleep(0.01)

    slots.release("polimi-gpu", 1)
    await asyncio.wait_for(t1, timeout=1.0)
    slots.release("polimi-gpu", 1)
    await asyncio.wait_for(t2, timeout=1.0)

    assert order == ["first", "second"]


# ---------------------------------------------------------------------------
# reconcile()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_pre_acquires_for_running_jobs() -> None:
    """A row with assigned_gpu_config={"A40":1} on polimi-gpu should consume 1 permit."""
    slots = NodeSlots(_make_cluster())
    rows = [
        ("polimi-gpu", {"A40": 1}),
        ("matemagician", {"QuadroP600": 1}),
        ("matemagician", {"QuadroP600": 1}),  # both matemagician permits taken
    ]
    await slots.reconcile(_make_get_conn(rows))

    assert slots.available("polimi-gpu") == 1  # 2 - 1
    assert slots.available("matemagician") == 0  # 2 - 2


@pytest.mark.asyncio
async def test_reconcile_unknown_node_logs_and_skips() -> None:
    """A row pointing at a node that's been removed from config is skipped, not fatal."""
    slots = NodeSlots(_make_cluster())
    rows = [
        ("polimi-gpu", {"A40": 1}),
        ("ghost-node", {"A40": 1}),  # not in cluster config
    ]
    await slots.reconcile(_make_get_conn(rows))

    # Real node was acquired correctly; ghost was ignored.
    assert slots.available("polimi-gpu") == 1
    # No KeyError or crash.


@pytest.mark.asyncio
async def test_reconcile_empty_db_keeps_full_capacity() -> None:
    slots = NodeSlots(_make_cluster())
    await slots.reconcile(_make_get_conn([]))
    assert slots.available("matemagician") == 2
    assert slots.available("polimi-gpu") == 2


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_detect_drift_returns_empty_when_in_sync() -> None:
    slots = NodeSlots(_make_cluster())
    # Initial state matches DB (both empty).
    drift = await slots.detect_drift(_make_get_conn([]))
    assert drift == {}


@pytest.mark.asyncio
async def test_detect_drift_flags_in_memory_leak() -> None:
    """Permit leaked in-memory but DB shows slot empty → drift reported."""
    slots = NodeSlots(_make_cluster())
    await slots.acquire("matemagician", 1)  # in-memory: 1 used

    # DB says no jobs are running (no rows).
    drift = await slots.detect_drift(_make_get_conn([]))
    assert "matemagician" in drift
    assert drift["matemagician"] == (1, 0)  # mem=1, db=0

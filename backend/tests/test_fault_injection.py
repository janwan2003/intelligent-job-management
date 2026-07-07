"""Fault-injection tests — prove the slot/claim invariants hold under induced faults.

These run **real SQL against PostgreSQL** (skipped when unreachable, exactly
like ``test_sql_integration``) so the atomic conditional-UPDATE claims and the
reaper's recovery path are exercised against the real engine's row-locking, not
a mock that can't reproduce a race.

Each test injects one fault and asserts the corresponding invariant from
``documentation/SLOT_INVARIANTS.md`` holds:

  * **Atomic placement claim — no double placement.** Two concurrent optimiser
    applies for one QUEUED row → exactly one wins
    (``test_optimizer_claim_is_exclusive``).
  * **Worker Phase-1 claim — no duplicate container.** Two concurrent dispatch
    claims for one row → exactly one starts
    (``test_worker_phase1_claim_is_exclusive``).
  * **Reaper-clear races worker-claim — no phantom RUNNING (the 5/4-GPU bug).**
    A reaper ``assigned_node := NULL`` makes a concurrent Phase-1 claim miss
    (``test_reaper_clear_makes_worker_claim_miss``).
  * **Stuck-dispatch reaper reclaims a leaked slot.** An orphaned QUEUED+assigned
    row with no live dispatch task is reset and its permit reclaimed
    (``test_reaper_resets_orphaned_assignment``); a row with a live task or a
    fresh ``updated_at`` is left alone
    (``test_reaper_skips_live_dispatch_task``, ``test_reaper_skips_fresh_row``).
  * **Drift reconcile (dropped slot-freed NOTIFY / API restart).** A leaked
    in-memory permit is released and a missed acquire is re-taken to match the
    DB authority (``test_drift_recovers_leaked_permit``,
    ``test_drift_recovers_missed_acquire``).
  * **Drift heartbeat only fires at quiescence.** ``drift_tick`` recovers a
    real leak when the cluster is idle, but treats in-flight dispatches
    (live task, QUEUED+assigned row) and pending migrates as legal
    transitional state — no recovery, no scheduler wake
    (``test_drift_tick_*``).
  * **Reassigned row stops streaming.** A progress write guarded on
    ``container_name`` no-ops once the row is reassigned
    (``test_progress_write_noops_on_reassigned_row``).
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Json
from shared.constants import JobStatus

from src.app import drift_tick, gc_stale_pending_migrates, reset_stuck_queued_assignments
from src.cluster import ClusterManager
from src.node_slots import NodeSlots

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ijm")

# Worker Phase-1 only claims rows that are still QUEUED (see worker/constants.py
# RUNNABLE_STATUSES); we hard-code the value rather than import the worker
# package, which isn't on the backend test path.
_RUNNABLE = [JobStatus.QUEUED]


def _make_cluster() -> ClusterManager:
    """Two nodes: matemagician (2 QuadroP600), polimi-gpu (2 A40) — the Ch.6 cluster."""
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


_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY, job_id TEXT NOT NULL, image TEXT NOT NULL,
    command JSONB NOT NULL, script_path TEXT, directory_to_mount TEXT,
    status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    container_name TEXT, exit_code INT, progress TEXT, priority INT DEFAULT 3,
    deadline TIMESTAMPTZ, batch_size INT, epochs_total INT, profiling_epochs_no INT,
    assigned_node TEXT, assigned_gpu_config JSONB, is_profiling_run BOOLEAN DEFAULT FALSE
);
"""


@pytest_asyncio.fixture
async def pool() -> AsyncGenerator[Any]:
    """Function-scoped connection pool against a real Postgres.

    Skips the whole test when the DB is unreachable so offline runs and CI
    without the compose stack stay green (mirrors ``test_sql_integration``).
    """
    from psycopg_pool import AsyncConnectionPool

    p = AsyncConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=8, open=False)
    try:
        await p.open(wait=True, timeout=1.5)
    except Exception as exc:
        await p.close()
        pytest.skip(f"PostgreSQL not reachable ({exc})")
    async with p.connection() as c:
        await c.execute(_SCHEMA)
        await c.execute("TRUNCATE jobs")
        await c.commit()
    yield p
    await p.close()


def _get_conn_factory(p: Any) -> Callable[[], Any]:
    """Mirror ``state.get_conn``: a zero-arg async-CM factory over the pool."""

    @asynccontextmanager
    async def get_conn() -> AsyncGenerator[Any]:
        async with p.connection() as conn:
            yield conn

    return get_conn


async def _insert_job(
    p: Any,
    *,
    job_uuid: str,
    status: str,
    assigned_node: str | None = None,
    gpu_config: dict[str, int] | None = None,
    container_name: str | None = None,
    updated_at: datetime | None = None,
    is_profiling: bool = False,
    job_type: str = "cnn_big",
) -> None:
    now = datetime.now(UTC)
    async with p.connection() as c:
        await c.execute(
            """INSERT INTO jobs
                 (id, job_id, image, command, status, created_at, updated_at,
                  container_name, assigned_node, assigned_gpu_config, is_profiling_run)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                job_uuid,
                job_type,
                "ijm-cnn-big:latest",
                Json(["python3", "train.py"]),
                status,
                now,
                updated_at or now,
                container_name,
                assigned_node,
                Json(gpu_config) if gpu_config is not None else None,
                is_profiling,
            ),
        )
        await c.commit()


async def _row(p: Any, job_uuid: str) -> dict[str, Any]:
    async with p.connection() as c:
        cur = await c.execute(
            "SELECT status, assigned_node, assigned_gpu_config, container_name FROM jobs WHERE id = %s",
            (job_uuid,),
        )
        r = await cur.fetchone()
    assert r is not None
    return {"status": r[0], "assigned_node": r[1], "assigned_gpu_config": r[2], "container_name": r[3]}


# ---------------------------------------------------------------------------
# Atomic placement claim — no double placement
# ---------------------------------------------------------------------------


async def _claim_assigned_node(url: str, job_uuid: str, node: str, cfg: dict[str, int]) -> bool:
    """Run the optimiser-apply claim (app.py) on its own autocommit connection.

    ``UPDATE ... WHERE status=QUEUED AND assigned_node IS NULL RETURNING id`` —
    returns True iff this caller won the row.
    """
    conn = await psycopg.AsyncConnection.connect(url, autocommit=True)
    try:
        cur = await conn.execute(
            """UPDATE jobs SET assigned_node = %s, assigned_gpu_config = %s,
                   is_profiling_run = FALSE, updated_at = %s
               WHERE id = %s AND status = %s AND assigned_node IS NULL
               RETURNING id""",
            (node, Json(cfg), datetime.now(UTC), job_uuid, JobStatus.QUEUED),
        )
        return (await cur.fetchone()) is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_optimizer_claim_is_exclusive(pool: Any) -> None:
    """Two optimiser applies race for one QUEUED row → exactly one wins.

    Postgres row-locks the UPDATE; the loser re-evaluates ``assigned_node IS
    NULL`` after the winner commits, matches 0 rows, and returns nothing — so
    a row can never be placed onto two nodes.
    """
    jid = str(uuid4())
    await _insert_job(pool, job_uuid=jid, status=JobStatus.QUEUED)

    results = await asyncio.gather(
        _claim_assigned_node(DATABASE_URL, jid, "polimi-gpu", {"A40": 1}),
        _claim_assigned_node(DATABASE_URL, jid, "matemagician", {"QuadroP600": 1}),
    )

    assert results.count(True) == 1, f"expected exactly one winner, got {results}"
    row = await _row(pool, jid)
    assert row["assigned_node"] in ("polimi-gpu", "matemagician")


# ---------------------------------------------------------------------------
# Worker Phase-1 claim — no duplicate container per instance
# ---------------------------------------------------------------------------


async def _worker_claim(url: str, job_uuid: str, node: str) -> bool:
    """Run the worker Phase-1 atomic claim (worker/execution.py)."""
    conn = await psycopg.AsyncConnection.connect(url, autocommit=True)
    try:
        cur = await conn.execute(
            """UPDATE jobs SET status = %s, container_name = %s, updated_at = %s
               WHERE id = %s AND status = ANY(%s) AND assigned_node = %s
               RETURNING id""",
            (JobStatus.RUNNING, f"ijm-{job_uuid[:8]}", datetime.now(UTC), job_uuid, _RUNNABLE, node),
        )
        return (await cur.fetchone()) is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_worker_phase1_claim_is_exclusive(pool: Any) -> None:
    """Two dispatch tasks race the Phase-1 claim → at most one starts a container."""
    jid = str(uuid4())
    await _insert_job(pool, job_uuid=jid, status=JobStatus.QUEUED, assigned_node="polimi-gpu", gpu_config={"A40": 1})

    results = await asyncio.gather(
        _worker_claim(DATABASE_URL, jid, "polimi-gpu"),
        _worker_claim(DATABASE_URL, jid, "polimi-gpu"),
    )

    assert results.count(True) == 1, f"expected exactly one container start, got {results}"
    row = await _row(pool, jid)
    assert row["status"] == JobStatus.RUNNING


@pytest.mark.asyncio
async def test_reaper_clear_makes_worker_claim_miss(pool: Any) -> None:
    """The 5/4-GPU bug guard: a reaper ``assigned_node:=NULL`` racing a Phase-1
    claim makes the claim miss, so no phantom RUNNING row is left without a slot.
    """
    jid = str(uuid4())
    await _insert_job(pool, job_uuid=jid, status=JobStatus.QUEUED, assigned_node="polimi-gpu", gpu_config={"A40": 1})

    # Reaper clears the assignment first (simulating it firing in the gap before
    # the worker's claim lands).
    async with pool.connection() as c:
        await c.execute(
            "UPDATE jobs SET assigned_node = NULL, assigned_gpu_config = NULL WHERE id = %s",
            (jid,),
        )
        await c.commit()

    # The worker claim still targets the old node — it must miss.
    won = await _worker_claim(DATABASE_URL, jid, "polimi-gpu")
    assert won is False
    row = await _row(pool, jid)
    assert row["status"] == JobStatus.QUEUED, "no phantom RUNNING after reaper clear"


# ---------------------------------------------------------------------------
# Stuck-dispatch reaper — reclaim a leaked slot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reaper_resets_orphaned_assignment(pool: Any) -> None:
    """An orphaned QUEUED+assigned row (no live dispatch task, stale updated_at)
    is reset and its leaked permit reclaimed by the drift-recover sweep.
    """
    slots = NodeSlots(_make_cluster())
    # Fault: a dispatch acquired the permit, then the /run never landed and the
    # task died, leaving the permit held in memory and the row pinned.
    await slots.acquire("polimi-gpu", 1)
    assert slots.available("polimi-gpu") == 1

    jid = str(uuid4())
    await _insert_job(
        pool,
        job_uuid=jid,
        status=JobStatus.QUEUED,
        assigned_node="polimi-gpu",
        gpu_config={"A40": 1},
        updated_at=datetime.now(UTC) - timedelta(seconds=120),  # older than the 30 s threshold
    )

    reset_any = await reset_stuck_queued_assignments(_get_conn_factory(pool), slots, {})

    assert reset_any is True
    row = await _row(pool, jid)
    assert row["assigned_node"] is None
    assert row["assigned_gpu_config"] is None
    assert slots.available("polimi-gpu") == 2, "leaked permit reclaimed"
    assert slots.metrics()["drift_recovery_count"] >= 1


@pytest.mark.asyncio
async def test_reaper_skips_live_dispatch_task(pool: Any) -> None:
    """A stuck row whose dispatch task is still alive is left to that task —
    the reaper must not double-release its permit.
    """
    slots = NodeSlots(_make_cluster())
    jid = str(uuid4())
    await _insert_job(
        pool,
        job_uuid=jid,
        status=JobStatus.QUEUED,
        assigned_node="polimi-gpu",
        gpu_config={"A40": 1},
        updated_at=datetime.now(UTC) - timedelta(seconds=120),
    )

    # A live (never-completing) dispatch task registered for this row.
    live = asyncio.create_task(asyncio.Event().wait())
    try:
        reset_any = await reset_stuck_queued_assignments(_get_conn_factory(pool), slots, {jid: live})
        assert reset_any is False
        row = await _row(pool, jid)
        assert row["assigned_node"] == "polimi-gpu", "row left for its live dispatch task"
    finally:
        live.cancel()


@pytest.mark.asyncio
async def test_reaper_skips_fresh_row(pool: Any) -> None:
    """A QUEUED+assigned row younger than the threshold is an in-flight dispatch,
    not an orphan — the reaper leaves it alone.
    """
    slots = NodeSlots(_make_cluster())
    jid = str(uuid4())
    await _insert_job(
        pool,
        job_uuid=jid,
        status=JobStatus.QUEUED,
        assigned_node="polimi-gpu",
        gpu_config={"A40": 1},
        updated_at=datetime.now(UTC),  # fresh
    )

    reset_any = await reset_stuck_queued_assignments(_get_conn_factory(pool), slots, {}, threshold_s=30)
    assert reset_any is False
    row = await _row(pool, jid)
    assert row["assigned_node"] == "polimi-gpu"


# ---------------------------------------------------------------------------
# Drift reconcile — dropped slot-freed NOTIFY / API restart
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drift_recovers_leaked_permit(pool: Any) -> None:
    """Dropped ``ijm_slot_freed``: the in-memory permit stays held after the job
    is already gone from the DB.  ``recover_from_drift`` releases it to match
    the DB authority and reports the negative delta.
    """
    slots = NodeSlots(_make_cluster())
    await slots.acquire("matemagician", 1)  # in-memory: 1 used
    # DB shows nothing RUNNING/PROFILING on matemagician (the slot-freed NOTIFY
    # that should have released the permit was lost on a tunnel reset).
    assert slots.available("matemagician") == 1

    deltas = await slots.recover_from_drift(_get_conn_factory(pool))

    assert deltas.get("matemagician") == -1
    assert slots.available("matemagician") == 2
    # Idempotent: a second sweep finds nothing to do.
    assert await slots.recover_from_drift(_get_conn_factory(pool)) == {}


@pytest.mark.asyncio
async def test_drift_recovers_missed_acquire(pool: Any) -> None:
    """API restart with an in-flight container: the DB shows a RUNNING row but
    the freshly-built semaphore thinks the slot is free.  ``recover_from_drift``
    acquires the missing permit so the node isn't oversubscribed by the next
    dispatch.
    """
    slots = NodeSlots(_make_cluster())
    jid = str(uuid4())
    await _insert_job(
        pool,
        job_uuid=jid,
        status=JobStatus.RUNNING,
        assigned_node="polimi-gpu",
        gpu_config={"A40": 2},
        container_name=f"ijm-{str(uuid4())[:8]}",
    )
    assert slots.available("polimi-gpu") == 2  # memory thinks fully free

    deltas = await slots.recover_from_drift(_get_conn_factory(pool))

    assert deltas.get("polimi-gpu") == 2
    assert slots.available("polimi-gpu") == 0, "both A40 permits taken to match DB"
    assert await slots.recover_from_drift(_get_conn_factory(pool)) == {}


# ---------------------------------------------------------------------------
# Reassigned row stops streaming
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_progress_write_noops_on_reassigned_row(pool: Any) -> None:
    """The streamer's progress UPDATE is guarded on ``container_name``; once the
    row is reassigned to a new container the old worker's write matches 0 rows
    and it stops streaming (no clobbering the new owner's progress).
    """
    jid = str(uuid4())
    old_container = f"ijm-{str(uuid4())[:8]}"
    await _insert_job(
        pool,
        job_uuid=jid,
        status=JobStatus.RUNNING,
        assigned_node="polimi-gpu",
        gpu_config={"A40": 1},
        container_name=old_container,
    )

    async def progress_write(container: str, value: str) -> int:
        async with pool.connection() as c:
            cur = await c.execute(
                "UPDATE jobs SET progress = %s, updated_at = %s "
                "WHERE id = %s AND status = ANY(%s) AND container_name = %s",
                (value, datetime.now(UTC), jid, [JobStatus.RUNNING, JobStatus.PROFILING], container),
            )
            rowcount = cur.rowcount
            await c.commit()
        return rowcount

    # While the old container owns the row, its write lands.
    assert await progress_write(old_container, "3/50") == 1

    # Row is reassigned (migration): a new container takes over.
    new_container = f"ijm-{str(uuid4())[:8]}"
    async with pool.connection() as c:
        await c.execute("UPDATE jobs SET container_name = %s WHERE id = %s", (new_container, jid))
        await c.commit()

    # The OLD container's next progress write now matches nothing → it stops.
    assert await progress_write(old_container, "4/50") == 0
    row = await _row(pool, jid)
    assert row["container_name"] == new_container


# ---------------------------------------------------------------------------
# Drift heartbeat quiescence gate (drift_tick)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drift_tick_recovers_leak_at_quiescence(pool: Any) -> None:
    """A leaked permit (lost slot-freed NOTIFY) with no in-flight work is the
    one case the heartbeat exists for: drift_tick recovers it and returns
    True so the caller wakes the scheduler.  A finished dispatch task left in
    the registry must not block the check.
    """
    slots = NodeSlots(_make_cluster())
    await slots.acquire("matemagician", 1)  # permit held, DB has no row behind it

    done_task = asyncio.create_task(asyncio.sleep(0))
    await done_task  # completed → not in-flight

    recovered = await drift_tick(_get_conn_factory(pool), slots, {"finished-job": done_task}, {})

    assert recovered is True
    assert slots.available("matemagician") == 2
    # Idempotent: nothing left to do, no wake requested.
    assert await drift_tick(_get_conn_factory(pool), slots, {}, {}) is False


@pytest.mark.asyncio
async def test_drift_tick_skips_live_dispatch_task(pool: Any) -> None:
    """A live dispatch task holds its permit legitimately (acquire happened,
    worker hasn't flipped the row to RUNNING yet).  drift_tick must neither
    'recover' that permit nor request a scheduler wake.
    """
    slots = NodeSlots(_make_cluster())
    await slots.acquire("matemagician", 1)  # held by the in-flight dispatch

    inflight = asyncio.create_task(asyncio.sleep(30))
    try:
        recovered = await drift_tick(_get_conn_factory(pool), slots, {"job-a": inflight}, {})
        assert recovered is False
        assert slots.available("matemagician") == 1, "permit must stay held"
    finally:
        inflight.cancel()


@pytest.mark.asyncio
async def test_drift_tick_skips_pending_migrate(pool: Any) -> None:
    """A stashed migrate plan whose source row is still RUNNING means the
    /stop drain + re-dispatch are in flight — transitional state, not drift.
    No recovery, no wake.  (The entry must have a live source row: drift_tick
    GCs entries whose row is terminal or gone before honouring the gate.)
    """
    slots = NodeSlots(_make_cluster())
    # 1 permit matches the RUNNING row below; the 2nd is a transient extra
    # that WOULD read as drift — the pending-migrate gate must stop the
    # heartbeat from "recovering" it mid-plan.
    await slots.acquire("polimi-gpu", 2)
    jid = str(uuid4())
    await _insert_job(pool, job_uuid=jid, status=JobStatus.RUNNING, assigned_node="polimi-gpu", gpu_config={"A40": 1})
    pending = {jid: object()}

    recovered = await drift_tick(_get_conn_factory(pool), slots, {}, pending)

    assert recovered is False
    assert jid in pending, "live migrate entry must survive the GC"
    assert slots.available("polimi-gpu") == 0, "no permit may be force-released mid-plan"


@pytest.mark.asyncio
async def test_drift_tick_skips_queued_assigned_row(pool: Any) -> None:
    """A QUEUED row with assigned_node set is the DB-visible signature of a
    dispatch between /run-accepted and the worker's Phase-1 claim (the
    dispatch task itself may already be done).  drift_tick must treat the
    cluster as non-quiescent and leave the permit alone.
    """
    slots = NodeSlots(_make_cluster())
    await slots.acquire("matemagician", 1)
    await _insert_job(
        pool,
        job_uuid=str(uuid4()),
        status=JobStatus.QUEUED,
        assigned_node="matemagician",
        gpu_config={"QuadroP600": 1},
    )

    recovered = await drift_tick(_get_conn_factory(pool), slots, {}, {})

    assert recovered is False
    assert slots.available("matemagician") == 1, "permit must stay held"


@pytest.mark.asyncio
async def test_drift_tick_quiet_when_consistent(pool: Any) -> None:
    """Quiescent AND consistent (permit held, matching RUNNING row): nothing to
    recover, no wake — the steady-state tick is silent.
    """
    slots = NodeSlots(_make_cluster())
    await slots.acquire("polimi-gpu", 1)
    await _insert_job(
        pool,
        job_uuid=str(uuid4()),
        status=JobStatus.RUNNING,
        assigned_node="polimi-gpu",
        gpu_config={"A40": 1},
        container_name="ijm-consistent",
    )

    assert await drift_tick(_get_conn_factory(pool), slots, {}, {}) is False
    assert slots.available("polimi-gpu") == 1


# ---------------------------------------------------------------------------
# Pending-migrate GC (gc_stale_pending_migrates)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gc_pops_migrate_whose_job_finished(pool: Any) -> None:
    """The 2026-07-16 wedge: a migrate was planned, but the job SUCCEEDED
    before the preempt landed.  The entry can never apply (row never reaches
    QUEUED+NULL) and must be GC'd, or it reads as in-flight plan work forever
    and mutes the drop-preempt wake + drift heartbeat.
    """
    jid = str(uuid4())
    await _insert_job(pool, job_uuid=jid, status=JobStatus.SUCCEEDED)
    pending = {jid: object()}

    popped = await gc_stale_pending_migrates(_get_conn_factory(pool), pending)

    assert popped == [jid]
    assert not pending


@pytest.mark.asyncio
async def test_gc_keeps_migrate_still_draining(pool: Any) -> None:
    """Source row still RUNNING → the /stop drain is in flight; the plan is
    alive and must be kept."""
    jid = str(uuid4())
    await _insert_job(pool, job_uuid=jid, status=JobStatus.RUNNING, assigned_node="polimi-gpu", gpu_config={"A40": 1})
    pending = {jid: object()}

    assert await gc_stale_pending_migrates(_get_conn_factory(pool), pending) == []
    assert jid in pending


@pytest.mark.asyncio
async def test_gc_keeps_drained_row_ready_to_apply(pool: Any) -> None:
    """QUEUED + assigned_node NULL is exactly the state the apply UPDATE
    claims — GC must leave it for the apply step."""
    jid = str(uuid4())
    await _insert_job(pool, job_uuid=jid, status=JobStatus.QUEUED)
    pending = {jid: object()}

    assert await gc_stale_pending_migrates(_get_conn_factory(pool), pending) == []
    assert jid in pending


@pytest.mark.asyncio
async def test_gc_pops_deleted_and_superseded_rows(pool: Any) -> None:
    """A deleted row (DELETE /jobs) and a QUEUED row someone else already
    re-assigned are both dead plans."""
    gone = str(uuid4())  # never inserted → deleted
    superseded = str(uuid4())
    await _insert_job(
        pool,
        job_uuid=superseded,
        status=JobStatus.QUEUED,
        assigned_node="matemagician",
        gpu_config={"QuadroP600": 1},
    )
    pending = {gone: object(), superseded: object()}

    popped = await gc_stale_pending_migrates(_get_conn_factory(pool), pending)

    assert sorted(popped) == sorted([gone, superseded])
    assert not pending


@pytest.mark.asyncio
async def test_drift_tick_unwedged_by_stale_migrate(pool: Any) -> None:
    """End-to-end wedge check: a leaked permit AND a stale migrate entry.
    drift_tick must GC the entry and still recover the leak (pre-GC it
    returned False forever)."""
    slots = NodeSlots(_make_cluster())
    await slots.acquire("matemagician", 1)  # leaked permit, no DB row
    dead = str(uuid4())
    await _insert_job(pool, job_uuid=dead, status=JobStatus.FAILED)
    pending = {dead: object()}

    recovered = await drift_tick(_get_conn_factory(pool), slots, {}, pending)

    assert recovered is True
    assert not pending, "stale entry must be purged"
    assert slots.available("matemagician") == 2

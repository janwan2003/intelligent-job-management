"""FastAPI application factory, lifespan, and CORS setup."""

import asyncio
import logging
import os
import random
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool
from shared.constants import (
    PG_NOTIFY_SCHEDULE,
    PG_NOTIFY_SLOT_FREED,
    JobStatus,
    SlotFreedReason,
)

import src.state as state
from src.cluster import cluster
from src.constants import CORS_ALLOWED_ORIGINS, DEFAULT_DATABASE_URL
from src.job_dispatcher import JobDispatcher
from src.node_slots import NodeSlots
from src.optimizer import Assignment, optimize
from src.profiling import scheduler
from src.routers import router

logger = logging.getLogger(__name__)


def parse_slot_payload(raw: str | None) -> tuple[str, int, SlotFreedReason] | None:
    """Parse a slot-freed NOTIFY payload.

    Primary form: ``"<node_id>:<n_gpus>:<reason>"`` — what every emitter
    sends.  A 2-field ``"<node_id>:<n_gpus>"`` is also accepted and
    defaults to ``TERMINAL`` (safe — wakes the optimiser, never drops a
    slot release) as a defensive fallback against a truncated reason.

    Hoisted to module scope so the unit tests can exercise it directly
    without instantiating the FastAPI lifespan.
    """
    payload = (raw or "").strip()
    if not payload:
        return None
    parts = payload.split(":")
    if len(parts) not in (2, 3):
        logger.warning("malformed slot-freed payload: %r", payload)
        return None
    try:
        n_gpus = int(parts[1])
    except ValueError:
        logger.warning("malformed slot-freed payload: %r", payload)
        return None
    node_id = parts[0]
    if len(parts) == 2:
        return node_id, n_gpus, SlotFreedReason.TERMINAL
    try:
        reason = SlotFreedReason(parts[2])
    except ValueError:
        logger.warning("unknown slot-freed reason in payload %r — defaulting to TERMINAL", payload)
        reason = SlotFreedReason.TERMINAL
    return node_id, n_gpus, reason


# Threshold for considering an assigned QUEUED row as a silently-failed
# dispatch (image pull failure, etc.).  Healthy /run is sub-second; 30 s
# is well above the floor and won't race in-flight dispatches.
_STUCK_DISPATCH_THRESHOLD_S = 30


async def reset_stuck_queued_assignments(
    get_conn: Any,
    node_slots: Any,
    dispatch_tasks: dict[str, asyncio.Task[None]],
    *,
    threshold_s: int = _STUCK_DISPATCH_THRESHOLD_S,
    now: datetime | None = None,
) -> bool:
    """Clear assigned_node + release leaked slot for orphaned QUEUED rows.

    A row can be left ``QUEUED`` with ``assigned_node`` set but no container
    behind it (a dispatch the optimiser/migrate planned that never landed —
    image pull failed, worker unreachable, API restarted between writing the
    assignment and posting ``/run``).  Such a row would otherwise stay pinned
    to a node forever, holding a slot the optimiser believes is occupied.

    If an in-flight dispatch task is still alive (usually blocked on
    ``acquire``) we skip the row — its own ``BaseException`` handler releases
    the permit; releasing here too would double-release the semaphore.

    Returns ``True`` if any row was reset (the caller should then wake the
    scheduler so the optimiser re-places the now-unassigned row).  Hoisted out
    of the lifespan closure so the recovery path is unit-testable against a
    real connection; ``now`` is injectable so a test can control the cutoff.
    """
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(seconds=threshold_s)
    needs_drift_recover = False
    reset_any = False
    async with get_conn() as conn:
        # Ownership is decided in-process: a QUEUED row with assigned_node set
        # is the API's responsibility iff ``dispatch_tasks[jid]`` is alive.  If
        # no live task exists, the row is orphaned regardless of what the
        # worker has written to container_name.
        cur = await conn.execute(
            """SELECT id, assigned_node, assigned_gpu_config FROM jobs
               WHERE status = %s AND assigned_node IS NOT NULL
                 AND updated_at < %s""",
            (JobStatus.QUEUED, cutoff),
        )
        stuck = await cur.fetchall()
        for jid, node, gpu_config in stuck:
            inflight = dispatch_tasks.get(jid)
            if inflight is not None and not inflight.done():
                # Live dispatch task — the API still owns this row; leave it.
                continue
            n = sum(int(v) for v in (gpu_config or {}).values())
            ucur = await conn.execute(
                """UPDATE jobs
                   SET assigned_node = NULL, assigned_gpu_config = NULL,
                       updated_at = %s
                   WHERE id = %s AND status = %s AND assigned_node = %s
                   RETURNING id""",
                (now, jid, JobStatus.QUEUED, node),
            )
            if not await ucur.fetchone():
                continue
            reset_any = True
            # No live task: a direct release() would underflow if the slot was
            # never acquired.  Defer to drift-recover which rebases _used from
            # the DB authoritative count.
            needs_drift_recover = True
            logger.warning(
                "Reset stuck QUEUED job %s on %s (%dx %s) — no dispatch task; queuing drift-recover sweep",
                jid[:8],
                node,
                n,
                gpu_config,
            )
        await conn.commit()

    # Rebase _used from the DB authority for stuck rows with no live task.
    if needs_drift_recover and node_slots is not None:
        try:
            await node_slots.recover_from_drift(get_conn)
        except Exception:
            logger.exception("Reaper drift-recover sweep failed")

    return reset_any


def should_wake_on_slot_freed(
    reason: SlotFreedReason,
    pending_migrates: dict[str, Any],
    dispatch_tasks: dict[str, asyncio.Task[None]],
) -> bool:
    """Decide whether a slot-freed NOTIFY should wake the optimiser.

    Non-AUTO_PREEMPT reasons (TERMINAL, USER_STOP, ORPHAN_DRAIN — plus the
    defensive TERMINAL fallback for unknown payloads) always wake: the free
    is an external event the scheduler hasn't planned around.

    AUTO_PREEMPT frees are the API's own ``/stop?reason=auto`` — but they
    come in two flavours the payload cannot distinguish (it carries no
    job id):

    - **migrate**: the same plan queued a follow-up dispatch for the freed
      slot (``pending_migrates`` → ``_apply_pending_migrates`` → a
      ``dispatch_tasks`` entry).  Waking here would let RG's near-tie
      nondeterminism flip the plan we are still executing — stay quiet.
    - **drop**: the optimiser evicted the instance with NO re-assignment
      (its plan omitted it entirely).  Nothing consumes the freed slot and
      the evicted row sits QUEUED with no other wake source — the next
      scheduled pass could be the 60-min queue watcher.  (The pre-
      quiescence-gate drift watcher used to paper over this by waking every
      15 s; observed stranding a live job for minutes on 2026-07-16 once
      that was fixed.)  Waking is exactly what's needed.

    We distinguish by plan state: any pending migrate or live dispatch task
    means plan work is in flight and the suppression rationale holds.
    Otherwise the plan has fully landed (or was drop-only) and the scheduler
    must be woken to re-place whatever the drop left QUEUED.  A spurious
    wake from the small window between a migrate's apply and its dispatch
    registration is harmless — the pass no-ops as kept-same.

    Hoisted to module scope for unit testing.
    """
    if reason != SlotFreedReason.AUTO_PREEMPT:
        return True
    plan_in_flight = bool(pending_migrates) or any(not t.done() for t in dispatch_tasks.values())
    return not plan_in_flight


async def gc_stale_pending_migrates(get_conn: Any, pending_migrates: dict[str, Any]) -> list[str]:
    """Drop pending-migrate plans whose source row can no longer drain.

    An entry waits for its row to reach QUEUED + ``assigned_node IS NULL``
    (the state the worker leaves after ``/stop?reason=auto``).  That never
    happens when the job outruns its own preempt: the container exits
    SUCCEEDED/FAILED before the kill lands, the user stops or deletes the
    job, or another path re-claims the row.  Such entries are dead plans —
    but left in ``state.pending_migrates`` they read as "plan work in
    flight" forever, muting both the drop-preempt wake
    (``should_wake_on_slot_freed``) and the drift heartbeat's quiescence
    gate (``drift_tick``).  Observed live 2026-07-16: two completed jobs'
    migrate entries suppressed wakes on a fully idle cluster.

    Keeps entries whose row is RUNNING/PROFILING (drain still in flight)
    or QUEUED+NULL (drained — the caller's conditional UPDATE claims it
    right after).  Everything else — terminal row, PREEMPTED (user-stop
    owns it), QUEUED with a node already assigned (superseded), deleted
    row — is popped.  Returns the popped instance ids.
    """
    if not pending_migrates:
        return []
    async with get_conn() as conn:
        cur = await conn.execute(
            "SELECT id, status, assigned_node FROM jobs WHERE id = ANY(%s)",
            (list(pending_migrates.keys()),),
        )
        rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}
    stale: list[str] = []
    for jid in list(pending_migrates.keys()):
        row = rows.get(jid)
        if row is not None:
            status, assigned_node = row
            if status in (JobStatus.RUNNING, JobStatus.PROFILING):
                continue
            if status == JobStatus.QUEUED and assigned_node is None:
                continue
        stale.append(jid)
    for jid in stale:
        pending_migrates.pop(jid, None)
        logger.info("Dropped stale pending migrate for %s (source row can no longer drain)", jid[:8])
    return stale


async def drift_tick(
    get_conn: Any,
    node_slots: Any,
    dispatch_tasks: dict[str, asyncio.Task[None]],
    pending_migrates: dict[str, Any],
) -> bool:
    """One drift-heartbeat iteration.  Returns ``True`` iff drift was recovered
    (the caller should then wake the scheduler — capacity may have changed).

    The ``mem_used == db_used`` invariant (SLOT_INVARIANTS.md) is only defined
    at quiescent points: while a dispatch is between ``acquire`` and the
    worker's Phase-1 RUNNING flip, or a migrate is between the source
    ``/stop`` and its re-dispatch, the two counters legitimately disagree.
    Sampling mid-flight used to be misread as drift; the "recovery" then
    released permits that live dispatch tasks were holding and woke the
    optimiser, whose fresh cost-equivalent RG plan started the next migration
    — a self-sustaining shuffle loop (observed 2026-07-15: identical tardy
    jobs ping-ponged between nodes every 15–30 s for the whole run).  So the
    check runs ONLY at quiescence.

    Quiescence = no live dispatch task, no pending migrate plan, and no
    QUEUED row holding an ``assigned_node`` (the DB-visible signature of a
    dispatch between /run-accepted and the worker's claim; the stuck-dispatch
    reaper clears stale ones after 30 s, so a wedged dispatch cannot mute the
    watcher for long).  A true leak — a lost ``ijm_slot_freed`` NOTIFY — has
    no in-flight work behind it and persists, so it is still healed on the
    first quiescent tick after the loss.

    Hoisted to module scope so the unit tests can exercise the gate against a
    real connection (same pattern as ``reset_stuck_queued_assignments``).
    """
    if pending_migrates:
        # A stale entry (job outran its own preempt) must not mute the
        # heartbeat forever — purge first, then re-check.
        await gc_stale_pending_migrates(get_conn, pending_migrates)
    if pending_migrates:
        return False
    if any(not t.done() for t in dispatch_tasks.values()):
        return False
    async with get_conn() as conn:
        cur = await conn.execute(
            "SELECT 1 FROM jobs WHERE status = %s AND assigned_node IS NOT NULL LIMIT 1",
            (JobStatus.QUEUED,),
        )
        if await cur.fetchone() is not None:
            return False
    drift = await node_slots.detect_drift(get_conn)
    if not drift:
        return False
    logger.warning("Drift watcher: %s — reconciling", drift)
    await node_slots.recover_from_drift(get_conn)
    return True


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Application lifespan manager.

    Wraps ``_lifespan_body`` to guarantee the connection pool is closed
    when startup fails partway (e.g. the DB is unreachable and the schema
    apply times out).  A leaked ``AsyncConnectionPool`` keeps background
    reconnect tasks alive after uvicorn logs "Application startup failed.
    Exiting." and can prevent the process from exiting — leaving a
    running container with nothing listening, which the ``unless-stopped``
    restart policy can never heal.
    """
    try:
        async with _lifespan_body(app):
            yield
    except BaseException:
        if state.pool is not None:
            await state.pool.close()
        raise


@asynccontextmanager
async def _lifespan_body(_app: FastAPI) -> AsyncGenerator[None]:
    """Application startup/shutdown; see ``lifespan`` for the failure guard."""
    # Load cluster configuration (nodes + GPU energy costs)
    cluster.load_nodes()
    cluster.load_gpu_energy_costs()

    # Get configuration from environment
    database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

    # Connect to database (connection pool)
    masked_url = database_url.split("@")[-1] if "@" in database_url else database_url
    logger.info("Connecting to database: %s", masked_url)
    state.pool = AsyncConnectionPool(conninfo=database_url, min_size=2, max_size=50, open=False)
    await state.pool.open()

    # Create tables, indexes, and constraints from schema.sql
    schema_path = Path(__file__).resolve().parent.parent / "schema.sql"
    schema_sql = schema_path.read_text()
    async with state.get_conn() as conn, conn.transaction():
        await conn.execute(schema_sql)
    logger.info("Database initialized")

    # ------------------------------------------------------------------
    # Job dispatcher
    # ------------------------------------------------------------------
    #
    # Jobs run on per-node worker containers (worker/), reached over HTTP.
    # The worker records profiling results (worker/profiling.py handle_complete)
    # and releases slots on completion via ``NOTIFY ijm_slot_freed`` /
    # ``ijm_schedule`` — picked up by ``_slot_listener`` / ``_notify_listener``
    # below — so the API needs no in-process completion callbacks.

    state.job_runner = JobDispatcher(state.get_conn, cluster)

    # NodeSlots: per-node semaphores that gate dispatches.  Reconciled from
    # DB so that already-occupied slots are pre-acquired.
    state.node_slots = NodeSlots(cluster)
    await state.node_slots.reconcile(state.get_conn)
    state.job_runner.set_node_slots(state.node_slots)

    # ------------------------------------------------------------------
    # Slot-coordinated scheduler
    # ------------------------------------------------------------------

    async def _preempt_and_release(job_id: str) -> None:
        """Auto-preempt a job; the slot is released asynchronously by the
        worker via ``NOTIFY ijm_slot_freed`` once kill+drain+persist completes.

        ``runner.stop(reason="auto")`` is fire-and-forget for remote workers
        (`asyncio.create_task` in ``JobDispatcher``), so we cannot use its
        return as the "slot is free" signal — it returns ~10 ms after task
        creation, long before the worker has actually killed anything.
        Instead, the worker's ``/stop`` handler emits ``ijm_slot_freed`` after
        ``_persist_stop`` commits, and the API's ``_slot_listener`` picks
        that up and calls ``node_slots.release(...)``.  This keeps the
        semaphore release in lockstep with the actual kill.

        Defensive: refuse to stop a profiling run.  Profiling is sacred —
        only manual /stop?reason=user may preempt it (the explicit Stop
        button in the UI).  This is a belt-and-suspenders check; every
        automated path that funnels into here should already filter
        ``is_profiling_run = FALSE`` upstream.  An ERROR here means a
        caller has a bug and is about to evict measurement work.
        """
        try:
            async with state.get_conn() as gconn, gconn.cursor() as gcur:
                await gcur.execute(
                    "SELECT is_profiling_run, status, assigned_node, assigned_gpu_config FROM jobs WHERE id = %s",
                    (job_id,),
                )
                grow = await gcur.fetchone()
            if grow and grow[0]:
                logger.error(
                    "REFUSED auto-preempt of profiling run %s (status=%s) — caller bug; "
                    "profiling jobs may only be stopped via manual /stop?reason=user",
                    job_id[:8],
                    grow[1] if grow else "?",
                )
                return
            if grow and grow[1] == JobStatus.PREEMPTED:
                # User-stop in flight; auto-preempt would clobber PREEMPTED back to QUEUED.
                logger.info(
                    "Skipping auto-preempt of %s — row is already PREEMPTED (user-stop in flight)",
                    job_id[:8],
                )
                return
            # NOTE on the post-pending_evictions invariant.  An earlier version
            # of this code tracked the (node, gpu_config) of every in-flight
            # stop in ``state.pending_evictions`` and ``get_node_gpu_usage``
            # subtracted those slots so the scheduler treated them as already
            # freed.  That mechanism is gone: the scheduler view now treats
            # only RUNNING/PROFILING rows (+ unmeasured profile claims) as
            # occupied, so a row that is still RUNNING during the stop drain
            # *already* looks occupied to the next scheduler pass.  This is
            # the more conservative direction — it closes the 2+ GPU
            # staggered-commit race (where the first eviction commit let a
            # standard run claim the half-freed slot before the second
            # commit landed) at the cost of a brief window where the
            # optimiser may decline to place into a slot that will free
            # within seconds.  Do NOT reintroduce eviction tracking without
            # re-examining that trade-off.
            await state.job_runner.stop(job_id, reason="auto")
            logger.info("Issued preempt /stop for job %s (slot release awaits NOTIFY)", job_id[:8])
        except Exception:
            logger.exception("Failed to issue preempt for job %s", job_id[:8])

    # Dispatch tasks live on ``state`` so the reaper can cancel a stuck task
    # rather than racing its exception handler on the slot release (double-release
    # would inflate the per-node semaphore by 1 each cycle).
    _dispatch_tasks = state.dispatch_tasks

    async def _dispatch_when_slot_free(a: Assignment) -> None:
        """Wait for the target node's slot to be free, then post /run.

        Each dispatch is its own task, so a slow preempt on node A doesn't
        gate dispatches for node B.  Delegates the actual acquire-then-post
        to ``JobDispatcher.dispatch_with_slot`` so this code path stays in
        lockstep with the direct submission/resume paths.
        """
        try:
            t0 = asyncio.get_running_loop().time()
            try:
                await state.job_runner.dispatch_with_slot(a.instance_id, a.node_id, a.gpu_config)
                elapsed = asyncio.get_running_loop().time() - t0
                logger.info(
                    "Dispatched job %s on %s %s (waited %.2fs for slot)",
                    a.instance_id[:8],
                    a.node_id,
                    a.gpu_config,
                    elapsed,
                )
            except asyncio.CancelledError:
                # dispatch_with_slot already released the permit on cancellation.
                logger.info("Dispatch for %s cancelled (likely by reaper)", a.instance_id[:8])
                raise
            except Exception:
                # Wake scheduler so the just-freed slot is reused without waiting for the watcher.
                notify_event.set()
                raise
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Dispatch failed for job %s on %s", a.instance_id[:8], a.node_id)

    async def _reset_stuck_queued_assignments() -> None:
        """Reaper closure: reset orphaned QUEUED rows, then wake the scheduler.

        Delegates to the module-level ``reset_stuck_queued_assignments`` (unit-
        testable) and, on any reset, wakes the optimiser so the now-unassigned
        row is re-placed.  Without this wake the planned-channel preempt path
        leaves the row idle forever (no other wake source after the reaper
        clears the assignment).
        """
        reset_any = await reset_stuck_queued_assignments(
            state.get_conn,
            state.node_slots,
            _dispatch_tasks,
            threshold_s=_STUCK_DISPATCH_THRESHOLD_S,
        )
        if reset_any:
            notify_event.set()

    async def _schedule_waiting_jobs() -> None:
        """Run the optimizer, then spawn parallel preempt/dispatch tasks.

        The schedule lock wraps only the DB writes — the optimizer HTTP call
        runs unlocked so its timeout can't block other scheduler work.  Stale
        optimizer suggestions are safe: the apply step filters on
        ``assigned_node IS NULL`` so already-claimed rows simply don't match.
        """
        async with state.schedule_lock:
            await _reset_stuck_queued_assignments()
            async with state.get_conn() as conn:
                node_gpu_usage = await scheduler.get_node_gpu_usage(conn)

        # Optimizer call OUTSIDE the schedule lock — see docstring.
        async with state.get_conn() as conn:
            opt_result = await optimize(conn, node_gpu_usage)

        # Hoisted so the spawn loops below the lock can see it.
        profile_evictions: list[str] = []

        async with state.schedule_lock:
            optimizer_handled: set[str] = set(opt_result.preempt)
            new_dispatches: list[Assignment] = []

            # Apply optimizer assignments.  Each successful UPDATE claims the
            # row; /run is dispatched in a background task that waits on the
            # per-node semaphore.  Profile-always is enforced at the instance
            # level (PROFILING_CONFIGS_PER_JOB); proactive preempts only fire
            # for instances that haven't yet profiled.
            async with state.get_conn() as conn:
                now = datetime.now(UTC)
                for a in opt_result.assignments:
                    optimizer_handled.add(a.instance_id)
                    # Migrate: the optimiser wants to move an already-RUNNING
                    # instance to a different node.  The conditional UPDATE
                    # below requires ``assigned_node IS NULL`` so it can't
                    # write the new destination while the row is still
                    # RUNNING on the source.  Stash the planned assignment
                    # in ``state.pending_migrates``; the slot listener
                    # applies it the moment the source ``/stop`` drains the
                    # row to QUEUED+NULL, then queues the dispatch task.
                    # Pre-AUTO_PREEMPT-wake-suppression this used to work
                    # because the slot-freed NOTIFY would wake a fresh
                    # optimiser pass that re-applied the same plan; with
                    # the suppression the migrate has to be carried across
                    # the preempt explicitly.
                    if a.instance_id in opt_result.preempt:
                        state.pending_migrates[a.instance_id] = a
                        logger.info(
                            "Queued migrate %s → node %s %s (awaiting source /stop drain)",
                            a.instance_id[:8],
                            a.node_id,
                            a.gpu_config,
                        )
                        continue
                    cur = await conn.execute(
                        """UPDATE jobs
                           SET assigned_node = %s, assigned_gpu_config = %s,
                               is_profiling_run = FALSE, updated_at = %s
                           WHERE id = %s AND status = %s AND assigned_node IS NULL
                           RETURNING id""",
                        (a.node_id, Json(a.gpu_config), now, a.instance_id, JobStatus.QUEUED),
                    )
                    if await cur.fetchone():
                        new_dispatches.append(a)
                        logger.info(
                            "Optimizer assigned job %s → node %s %s",
                            a.instance_id[:8],
                            a.node_id,
                            a.gpu_config,
                        )
                await conn.commit()

                # Profile-placement pass for QUEUED jobs the optimizer skipped.
                # Jobs that still owe a profile but can't find a free slot fall
                # through to the proactive-preempt pass below.
                cur = await conn.execute(
                    "SELECT id, job_id FROM jobs WHERE status = %s AND assigned_node IS NULL ORDER BY created_at ASC",
                    (JobStatus.QUEUED,),
                )
                unassigned = [(row[0], row[1]) for row in await cur.fetchall()]

                still_unassigned: list[tuple[str, str]] = []
                for instance_id, type_id in unassigned:
                    if instance_id in optimizer_handled:
                        continue
                    result = await scheduler.schedule_job(conn, instance_id, job_type_id=type_id)
                    if result.node_id is not None and result.gpu_config is not None:
                        # schedule_job already wrote the assignment; refresh updated_at.
                        await conn.execute(
                            "UPDATE jobs SET status = %s, updated_at = %s WHERE id = %s",
                            (JobStatus.QUEUED, datetime.now(UTC), instance_id),
                        )
                        new_dispatches.append(
                            Assignment(
                                instance_id=instance_id,
                                node_id=result.node_id,
                                gpu_config=result.gpu_config,
                            )
                        )
                    else:
                        still_unassigned.append((instance_id, type_id))
                await conn.commit()

                # Proactive profile preempts: a profile-owing instance is
                # invisible to the optimizer, so without an eviction it sits
                # until the blocking jobs drain naturally.  Profile-always
                # (the report's policy) says measurement outranks standard
                # work, so we fire both for instances that can still claim a
                # new config AND for instances holding a claimed-but-
                # unmeasured cell that can't fit (e.g. an A40×2 claim behind
                # a standard A40×1 run).  The historical thrash concern
                # ("standard re-claims the freed slot before profile can")
                # is closed by the pre-claim UPDATE below, which reserves
                # the slot before the victims are stopped.
                evicted_in_round: set[str] = set()
                for instance_id, type_id in still_unassigned:
                    # Fire when the instance can still claim (budget left) or
                    # already owns an unmeasured cell to execute.  Never
                    # creates claims beyond configs_per_job: the chase for a
                    # budget-exhausted instance only targets its own claims.
                    already_profiled = await scheduler._count_profiled_this_round(conn, instance_id)
                    if already_profiled >= scheduler.configs_per_job and not await scheduler._get_in_flight_claims(
                        conn, instance_id
                    ):
                        continue
                    victims, target_node, target_config = await scheduler.try_preempt_for_profile(
                        conn, instance_id, type_id
                    )
                    if not victims or target_node is None or target_config is None:
                        continue
                    # Pre-claim the soon-to-be-free slot so the next optimizer round
                    # can't re-dispatch the victim onto it and trigger eviction thrash.
                    now2 = datetime.now(UTC)
                    cur2 = await conn.execute(
                        """UPDATE jobs
                           SET assigned_node = %s, assigned_gpu_config = %s,
                               is_profiling_run = TRUE, updated_at = %s
                           WHERE id = %s AND status = %s AND assigned_node IS NULL
                           RETURNING id""",
                        (target_node, Json(target_config), now2, instance_id, JobStatus.QUEUED),
                    )
                    if not await cur2.fetchone():
                        continue
                    # Mirror _persist_assignment's profiling_results insert so the
                    # worker's _record_profile_result UPDATE (matching duration IS NULL)
                    # has a row to land on — otherwise the result is lost and the
                    # scheduler restarts the trainer from epoch 0 next round.
                    await conn.execute(
                        """INSERT INTO profiling_results
                               (id, job_id, instance_id, gpu_config, node_id, duration_seconds, created_at)
                           VALUES (%s, %s, %s, %s, %s, NULL, %s)
                           ON CONFLICT (job_id, gpu_config) DO NOTHING""",
                        (
                            str(uuid4()),
                            type_id,
                            instance_id,
                            Json(target_config),
                            target_node,
                            now2,
                        ),
                    )
                    new_dispatches.append(
                        Assignment(
                            instance_id=instance_id,
                            node_id=target_node,
                            gpu_config=target_config,
                        )
                    )
                    for victim in victims:
                        if victim in evicted_in_round:
                            continue
                        profile_evictions.append(victim)
                        evicted_in_round.add(victim)
                    logger.info(
                        "Profile-preempt (unprofiled type): reserving %s %s for %s (type=%s); evicting %d victim(s) [%s]",
                        target_node,
                        target_config,
                        instance_id[:8],
                        type_id,
                        len(victims),
                        ",".join(v[:8] for v in victims),
                    )
                await conn.commit()

        # Spawn preempt + dispatch tasks OUTSIDE the lock.  Each runs
        # independently and coordinates via NodeSlots.
        for jid in opt_result.preempt:
            logger.info("Optimizer preempting job %s", jid[:8])
            asyncio.create_task(_preempt_and_release(jid), name=f"preempt-{jid[:8]}")

        for jid in profile_evictions:
            asyncio.create_task(_preempt_and_release(jid), name=f"profile-preempt-{jid[:8]}")

        for a in new_dispatches:
            existing = _dispatch_tasks.get(a.instance_id)
            if existing is not None and not existing.done():
                # A dispatch task for this row is already in flight — don't
                # spawn a duplicate.  This matters because spawning twice
                # means two acquires, and only one container ever runs (the
                # second /run hits the worker's 409), so one permit leaks.
                logger.info(
                    "Skipping duplicate dispatch task for %s (already in flight)",
                    a.instance_id[:8],
                )
                continue
            task = asyncio.create_task(_dispatch_when_slot_free(a), name=f"dispatch-{a.instance_id[:8]}")
            _dispatch_tasks[a.instance_id] = task

            def _cleanup(_t: asyncio.Task[None], jid: str = a.instance_id) -> None:
                _dispatch_tasks.pop(jid, None)

            task.add_done_callback(_cleanup)

    async def _queue_watcher() -> None:
        """Safety net: retry scheduling every 60 min in case something was missed.

        Lowered cadence (was 60 s).  Periodic ticks were contributing to
        the RG random-swap noise: every minute, with cost-equivalent
        placements on the cost surface, RG would re-roll its seed and
        propose a fresh tie-breaking swap even when nothing in the world
        had changed.  We rely instead on NOTIFY-driven wakes for real
        state changes and on the 15-s drift heartbeat for in-memory/DB
        divergence.
        """
        while True:
            await asyncio.sleep(60 * 60)
            try:
                # GC orphaned profile claims first (long-lease safety net);
                # then run the normal schedule pass.  The lifecycle paths
                # (DELETE /jobs, worker reconcile on FAILED) clean up claims
                # promptly — this only catches what they miss (API crash
                # mid-claim, worker disappearance before reconcile).
                async with state.get_conn() as conn:
                    await scheduler.gc_orphaned_claims(conn)
                    await conn.commit()
                await _schedule_waiting_jobs()
            except Exception:
                logger.exception("Queue watcher error")

    # Debounce notifies: at most one pending scheduler run at a time.
    # Without this, a notify storm spawns unbounded create_task() calls that
    # all serialize on state.schedule_lock.
    notify_event = asyncio.Event()

    async def _notify_listener() -> None:
        """Wake the scheduler immediately when a worker sends NOTIFY after profiling."""
        backoff = 1.0
        max_backoff = 60.0
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
                    await conn.execute(f"LISTEN {PG_NOTIFY_SCHEDULE}")
                    backoff = 1.0  # connection established → reset backoff
                    async for _ in conn.notifies():
                        logger.debug("Received schedule notification")
                        notify_event.set()
            except Exception:
                # Exponential backoff with jitter caps reconnect storms when
                # the DB is down for an extended period.  At max backoff the
                # listener tries roughly once a minute, which is enough for
                # the queue watcher heartbeat to pick up state changes.
                jitter = random.uniform(0, backoff * 0.25)
                wait = backoff + jitter
                logger.warning("Notify listener lost connection, reconnecting in %.1fs", wait)
                await asyncio.sleep(wait)
                backoff = min(max_backoff, backoff * 2)

    # Minimum interval between optimizer passes.  The optimizer doesn't
    # penalise migrations, so back-to-back calls can pick equivalent-cost but
    # different placements and thrash jobs before they complete an epoch.
    _SCHEDULE_MIN_INTERVAL_S = 5.0
    _last_schedule_at = 0.0

    async def _notify_consumer() -> None:
        """Coalesce notifies → at most one in-flight scheduler pass.

        Also enforces a minimum interval between optimizer calls so the
        cluster has time to apply the previous plan before re-evaluating.
        """
        nonlocal _last_schedule_at
        while True:
            await notify_event.wait()
            notify_event.clear()
            now = asyncio.get_running_loop().time()
            wait = _SCHEDULE_MIN_INTERVAL_S - (now - _last_schedule_at)
            if wait > 0:
                await asyncio.sleep(wait)
            try:
                _last_schedule_at = asyncio.get_running_loop().time()
                await _schedule_waiting_jobs()
            except Exception:
                logger.exception("Scheduler pass triggered by notify failed")

    async def _apply_pending_migrates() -> None:
        """Apply migrate plans whose source ``/stop`` has just drained.

        Called from the slot listener after every NOTIFY.  Each entry in
        ``state.pending_migrates`` is a planned move whose UPDATE we
        deferred at apply time because the source row was still RUNNING.
        The conditional UPDATE here writes the new destination only if
        the row is now QUEUED with assigned_node=NULL (the state the
        worker leaves it in after ``/stop?reason=auto``), so calling
        this on every NOTIFY is safe — it only fires for the migrating
        instance whose stop has actually drained.

        On a successful write we spawn the dispatch task the same way
        the in-lock apply path would have.
        """
        if not state.pending_migrates:
            return
        # Purge plans whose source row can no longer drain (job finished,
        # user-stopped, deleted, or re-claimed) so they don't read as
        # in-flight work forever — see gc_stale_pending_migrates.
        await gc_stale_pending_migrates(state.get_conn, state.pending_migrates)
        if not state.pending_migrates:
            return
        now = datetime.now(UTC)
        applied: list[Assignment] = []
        async with state.schedule_lock, state.get_conn() as conn:
            for jid, a in list(state.pending_migrates.items()):
                cur = await conn.execute(
                    """UPDATE jobs
                       SET assigned_node = %s, assigned_gpu_config = %s,
                           is_profiling_run = FALSE, updated_at = %s
                       WHERE id = %s AND status = %s AND assigned_node IS NULL
                       RETURNING id""",
                    (a.node_id, Json(a.gpu_config), now, jid, JobStatus.QUEUED),
                )
                if await cur.fetchone():
                    state.pending_migrates.pop(jid, None)
                    applied.append(a)
                    logger.info(
                        "Applied pending migrate %s → node %s %s",
                        jid[:8],
                        a.node_id,
                        a.gpu_config,
                    )
            await conn.commit()
        for a in applied:
            existing = _dispatch_tasks.get(a.instance_id)
            if existing is not None and not existing.done():
                logger.info(
                    "Skipping duplicate dispatch task for %s (already in flight)",
                    a.instance_id[:8],
                )
                continue
            task = asyncio.create_task(
                _dispatch_when_slot_free(a),
                name=f"dispatch-migrate-{a.instance_id[:8]}",
            )
            _dispatch_tasks[a.instance_id] = task

            def _cleanup(_t: asyncio.Task[None], jid: str = a.instance_id) -> None:
                _dispatch_tasks.pop(jid, None)

            task.add_done_callback(_cleanup)

    async def _slot_listener() -> None:
        """LISTEN ijm_slot_freed: release the permit and (conditionally) wake the optimiser.

        Payload is "<node_id>:<n_gpus>:<reason>".  Receipt means the worker
        has fully cleaned up that slot's container.

        The wake is gated by ``should_wake_on_slot_freed``:
        - TERMINAL / USER_STOP / ORPHAN_DRAIN: external event — wake the
          optimiser so the freed slot can be reassigned.
        - AUTO_PREEMPT with plan work in flight (pending migrate or live
          dispatch task): the dispatch step of that same plan will acquire
          the just-released slot; re-running the optimiser here would let
          RG's near-tie nondeterminism flip the plan we are still
          executing.  Skip the wake.
        - AUTO_PREEMPT with NO plan work in flight: a drop-preempt — the
          optimiser evicted the instance without re-assigning it, so no
          dispatch will ever consume the slot and the evicted row would
          otherwise sit QUEUED until the 60-min watcher.  Wake.

        Plan-execution safety net is preserved: if a planned preempt
        somehow fails to be followed by its planned dispatch (worker
        hang, dispatch exception, etc.), the dispatch-exception path,
        the 15-s drift watcher, the stuck-dispatch reaper, and the 60-min
        queue watcher all still wake the optimiser independently.
        """
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
                    await conn.execute(f"LISTEN {PG_NOTIFY_SLOT_FREED}")
                    if state.node_slots is not None:
                        try:
                            deltas = await state.node_slots.recover_from_drift(state.get_conn)
                            if deltas:
                                logger.warning(
                                    "Slot listener (re)connect: drift recovery applied %s",
                                    deltas,
                                )
                                notify_event.set()
                        except Exception:
                            logger.exception("Slot listener: drift recovery failed")
                    async for n in conn.notifies():
                        parsed = parse_slot_payload(n.payload)
                        if parsed is None or state.node_slots is None:
                            continue
                        node_id, count, reason = parsed
                        state.node_slots.release(node_id, count)
                        # The just-released slot may be the source of an
                        # in-flight migrate whose new assignment we stashed
                        # in ``state.pending_migrates`` at apply time.  Apply
                        # (and GC) those SYNCHRONOUSLY before the wake
                        # decision below: it pops entries whose row drained
                        # or died, so ``should_wake_on_slot_freed`` sees the
                        # true plan state — a stale entry would otherwise
                        # suppress the wake this free is owed.
                        if state.pending_migrates:
                            try:
                                await _apply_pending_migrates()
                            except Exception:
                                logger.exception("apply_pending_migrates from slot listener failed")
                        if not should_wake_on_slot_freed(reason, state.pending_migrates, state.dispatch_tasks):
                            logger.info(
                                "slot freed by auto-preempt on %s (%d GPU) — skipping wake "
                                "(in-flight plan's dispatch will pick it up)",
                                node_id,
                                count,
                            )
                            continue
                        if reason == SlotFreedReason.AUTO_PREEMPT:
                            logger.info(
                                "slot freed by auto-preempt on %s (%d GPU) with no in-flight plan work — "
                                "waking scheduler to re-place the dropped instance",
                                node_id,
                                count,
                            )
                        notify_event.set()
            except Exception:
                logger.warning("Slot listener lost connection, reconnecting in 5s")
                await asyncio.sleep(5)

    async def _drift_watcher() -> None:
        """Periodic drift heartbeat — corrects in-memory ``_used`` if it has
        diverged from the DB authoritative count (missed ``ijm_slot_freed``
        NOTIFY, crashed dispatch task).  Delegates to the module-level
        ``drift_tick``, which only checks at quiescence — sampling while a
        dispatch/migrate is in flight would misread legal transitional state
        as drift and feed the optimiser spurious wakes.  Wakes the scheduler
        only when something was actually recovered.  Period configurable via
        IJM_DRIFT_HEARTBEAT_S (default 15s).
        """
        period_s = float(os.getenv("IJM_DRIFT_HEARTBEAT_S", "15"))
        while True:
            await asyncio.sleep(period_s)
            try:
                if state.node_slots is None:
                    continue
                recovered = await drift_tick(
                    state.get_conn,
                    state.node_slots,
                    state.dispatch_tasks,
                    state.pending_migrates,
                )
                if recovered:
                    notify_event.set()
            except Exception:
                logger.exception("Drift watcher iteration failed")

    watcher_task = asyncio.create_task(_queue_watcher())
    listener_task = asyncio.create_task(_notify_listener())
    consumer_task = asyncio.create_task(_notify_consumer())
    slot_listener_task = asyncio.create_task(_slot_listener())
    drift_task = asyncio.create_task(_drift_watcher(), name="drift-watcher")

    # Kick the scheduler once at startup so jobs queued before this
    # process started (or left QUEUED across a restart) get placed
    # immediately rather than waiting up to 60 s for _queue_watcher.
    notify_event.set()

    yield

    # Cleanup
    watcher_task.cancel()
    listener_task.cancel()
    consumer_task.cancel()
    slot_listener_task.cancel()
    drift_task.cancel()
    if state.pool:
        await state.pool.close()


app = FastAPI(
    title="Intelligent Job Management Platform",
    description="API for Intelligent Job Management",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS configuration for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

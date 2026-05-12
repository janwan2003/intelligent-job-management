"""FastAPI application factory, lifespan, and CORS setup."""

import asyncio
import logging
import os
import random
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool
from shared.constants import PG_NOTIFY_SCHEDULE, PG_NOTIFY_SLOT_FREED, JobStatus

import src.state as state
from src.cluster import cluster
from src.constants import CORS_ALLOWED_ORIGINS, DEFAULT_DATABASE_URL
from src.executors import create_executor
from src.job_dispatcher import JobDispatcher
from src.job_runner import JobRunner
from src.node_slots import NodeSlots
from src.optimizer import Assignment, optimize
from src.profiling import scheduler
from src.routers import router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Application lifespan manager."""
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
    # Job runner
    # ------------------------------------------------------------------

    executor_name = os.getenv("EXECUTOR", "docker")
    executor = create_executor(executor_name)
    logger.info("Executor: %s", executor_name)

    async def _on_profiling_complete(
        job_id: str,
        gpu_config: dict[str, int],
        node_id: str,
        duration: float,
        job_type_id: str | None = None,
    ) -> None:
        """Handle profiling result — record it, then defer placement to the
        full scheduling pass so the optimizer (not greedy) decides the next run."""
        type_id = job_type_id or job_id
        async with state.get_conn() as pconn, pconn.transaction():
            now = datetime.now(UTC)
            # Filter by instance_id so a stray finish from a different instance
            # can never overwrite this row, and surface a warning if no claim
            # row matches (e.g. it was deleted by a concurrent failure path).
            cur = await pconn.execute(
                """UPDATE profiling_results
                   SET duration_seconds = %s, node_id = %s
                   WHERE job_id = %s AND gpu_config = %s::jsonb
                     AND instance_id = %s AND duration_seconds IS NULL""",
                (duration, node_id, type_id, Json(gpu_config), job_id),
            )
            if cur.rowcount == 0:
                logger.warning(
                    "No in-flight profiling claim for job %s (type=%s, %s) — result not recorded",
                    job_id[:8],
                    type_id,
                    gpu_config,
                )
            await pconn.execute(
                """UPDATE jobs
                   SET status = %s, assigned_node = NULL, assigned_gpu_config = NULL,
                       is_profiling_run = FALSE, updated_at = %s
                   WHERE id = %s""",
                (JobStatus.QUEUED, now, job_id),
            )

        logger.info(
            "Recorded profiling result for job %s (type=%s): %s = %.1fs", job_id[:8], type_id, gpu_config, duration
        )
        await _schedule_waiting_jobs()

    async def _on_job_completed(job_id: str) -> None:
        """When an in-process job finishes, release its slot semaphore and
        re-trigger scheduling.

        The slot release uses (assigned_node, assigned_gpu_config) read from
        the row — at this point the row's status is terminal (SUCCEEDED /
        FAILED / PREEMPTED) but ``assigned_node`` is still set (the auto-
        preempt path that nullifies it doesn't go through this callback).
        Tolerate a missing row / missing assignment defensively.
        """
        if state.node_slots is not None:
            try:
                async with state.get_conn() as conn:
                    cur = await conn.execute(
                        "SELECT assigned_node, assigned_gpu_config FROM jobs WHERE id = %s",
                        (job_id,),
                    )
                    row = await cur.fetchone()
                if row and row[0] and row[1]:
                    n_gpus = sum(int(v) for v in row[1].values())
                    state.node_slots.release(row[0], n_gpus)
            except Exception:
                logger.exception("Failed to release slot for completed job %s", job_id[:8])
        await _schedule_waiting_jobs()

    local_runner = JobRunner(
        executor=executor,
        get_conn=state.get_conn,
        on_profiling_complete=_on_profiling_complete,
        on_job_completed=_on_job_completed,
    )
    state.job_runner = JobDispatcher(local_runner, state.get_conn, cluster)
    await state.job_runner.start()

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
                # A user-/stop just flipped this row before our task ran.
                # Skip silently — the user-stop is already killing the
                # container; an auto-preempt now would clobber PREEMPTED
                # back to QUEUED and re-dispatch the row.
                logger.info(
                    "Skipping auto-preempt of %s — row is already PREEMPTED (user-stop in flight)",
                    job_id[:8],
                )
                return
            # Record the pending eviction so the scheduler treats the slot as
            # freed in subsequent rounds even before the worker's /stop commit
            # lands.  See ``state.pending_evictions`` for the race rationale.
            if grow and grow[2] and grow[3]:
                state.pending_evictions[job_id] = (grow[2], dict(grow[3]))
            try:
                await state.job_runner.stop(job_id, reason="auto")
                logger.info("Issued preempt /stop for job %s (slot release awaits NOTIFY)", job_id[:8])
                # Clear after the worker's stop commit lands.  ``stop`` is
                # fire-and-forget for remote workers, so wait briefly for
                # the row to actually leave RUNNING/PROFILING before clearing.
                deadline = asyncio.get_running_loop().time() + 30
                while asyncio.get_running_loop().time() < deadline:
                    await asyncio.sleep(0.5)
                    async with state.get_conn() as cconn, cconn.cursor() as ccur:
                        await ccur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
                        crow = await ccur.fetchone()
                    if not crow or crow[0] not in (JobStatus.RUNNING, JobStatus.PROFILING):
                        break
            finally:
                state.pending_evictions.pop(job_id, None)
        except Exception:
            state.pending_evictions.pop(job_id, None)
            logger.exception("Failed to issue preempt for job %s", job_id[:8])

    # Registry of in-flight dispatch tasks lives on ``state`` so the
    # ``/admin/dispatch-tasks`` endpoint can read it.  The reaper consults
    # this so it can cancel a stuck dispatch task instead of racing it on
    # a release — without coordination both the reaper and the task's
    # exception handler would release the same permit, inflating the
    # semaphore by 1 each cycle (this was the actual root cause of the
    # matemagician=3 oversubscription we were seeing).
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
                # Reaper cancelled us — dispatch_with_slot already released
                # the permit on its way out.  Don't log as an exception.
                logger.info("Dispatch for %s cancelled (likely by reaper)", a.instance_id[:8])
                raise
            except Exception:
                # Wake the scheduler immediately so the just-freed slot can
                # be reused without waiting up to 60 s for ``_queue_watcher``.
                notify_event.set()
                raise
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Dispatch failed for job %s on %s", a.instance_id[:8], a.node_id)

    # Reap QUEUED rows whose assigned_node has been set for longer than
    # this without the worker advancing them to RUNNING/PROFILING.  These
    # are dispatches that silently failed (e.g. the worker couldn't pull
    # the image) — leaving them stuck would cause the optimizer to treat
    # the slot as occupied (permutation filter), so no re-placement ever
    # fires.  A normal /run round-trip is well under a second; 30 s is a
    # generous floor that won't race with healthy in-flight dispatches.
    _STUCK_DISPATCH_THRESHOLD_S = 30

    async def _reset_stuck_queued_assignments() -> None:
        """Clear assigned_node + release leaked slot for orphaned QUEUED rows.

        Race-safe with in-flight dispatch tasks: if a dispatch task for the
        stuck row is still alive (typically blocked on ``acquire`` because
        the node is at full capacity), we *cancel* it and let it release
        its own permit via ``dispatch_with_slot``'s ``BaseException``
        handler.  Releasing here as well would double-release — the bug
        that caused matemagician=3 in our last run.
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=_STUCK_DISPATCH_THRESHOLD_S)
        async with state.get_conn() as conn:
            # ``container_name IS NULL`` excludes rows whose worker has run
            # Phase 1 atomic claim (which sets container_name).  Without this
            # filter, the reaper races between "dispatch task completed
            # (slot acquired, /run sent)" and "worker Phase 1 atomic claim
            # commits".  Symptom: a job that's actually being dispatched
            # gets its assigned_node cleared, the optimizer re-places it
            # elsewhere, and we end up bouncing the row across nodes while
            # the worker is starting the container on the original target.
            cur = await conn.execute(
                """SELECT id, assigned_node, assigned_gpu_config FROM jobs
                   WHERE status = %s AND assigned_node IS NOT NULL
                     AND container_name IS NULL
                     AND updated_at < %s""",
                (JobStatus.QUEUED, cutoff),
            )
            stuck = await cur.fetchall()
            tasks_to_await: list[asyncio.Task[None]] = []
            for jid, node, gpu_config in stuck:
                n = sum(int(v) for v in (gpu_config or {}).values())
                # Conditional UPDATE so we don't race with a fresh dispatch
                # that just flipped the row to RUNNING/PROFILING.  Repeat
                # the ``container_name IS NULL`` guard in the WHERE clause
                # so a worker that just-now wrote container_name in Phase 1
                # (between our SELECT above and this UPDATE) is respected.
                ucur = await conn.execute(
                    """UPDATE jobs
                       SET assigned_node = NULL, assigned_gpu_config = NULL,
                           updated_at = %s
                       WHERE id = %s AND status = %s AND assigned_node = %s
                         AND container_name IS NULL
                       RETURNING id""",
                    (datetime.now(UTC), jid, JobStatus.QUEUED, node),
                )
                if not await ucur.fetchone():
                    continue

                inflight = _dispatch_tasks.get(jid)
                if inflight is not None and not inflight.done():
                    # Cancel and let the task's BaseException handler
                    # release the permit (or no-op if it never acquired).
                    inflight.cancel()
                    tasks_to_await.append(inflight)
                    logger.warning(
                        "Reset stuck QUEUED job %s on %s (%dx %s) — cancelled in-flight dispatch task",
                        jid[:8],
                        node,
                        n,
                        gpu_config,
                    )
                else:
                    # No live task (e.g. crashed or pre-restart). Release
                    # directly — this is the only safe path that doesn't
                    # race anyone.
                    if state.node_slots is not None and node and n > 0:
                        state.node_slots.release(node, n)
                    logger.warning(
                        "Reset stuck QUEUED job %s on %s (%dx %s) — no dispatch task; slot released",
                        jid[:8],
                        node,
                        n,
                        gpu_config,
                    )
            await conn.commit()

        # Wait for cancelled tasks to actually finish releasing.  Cheap:
        # they're either at the acquire suspension point (cancellation is
        # immediate) or in a quick post-acquire await.
        if tasks_to_await:
            await asyncio.gather(*tasks_to_await, return_exceptions=True)

    async def _schedule_waiting_jobs() -> None:
        """Run the optimizer + greedy scheduler, then spawn parallel
        preempt/dispatch tasks coordinated by per-node semaphores.

        The schedule lock is held only for the DB write phases (Phase 1a
        reset + Phase 1b apply).  The optimizer HTTP call runs WITHOUT
        the lock — its 30 s timeout would otherwise block every other
        scheduler invocation (including the queue watcher heartbeat) any
        time the optimizer is slow or unreachable.  Phase 1b's
        ``WHERE assigned_node IS NULL`` filter makes stale optimizer
        suggestions safe to apply: rows that were claimed in the meantime
        simply don't match.
        """
        async with state.schedule_lock:
            await _reset_stuck_queued_assignments()
            # Reclaim every stale profile claim before the optimizer reads
            # ``profiling_results``.  Without this, a claim row whose owning
            # instance was repurposed (is_profiling_run flipped to FALSE) or
            # terminated mid-profile sits forever, makes ``remaining`` look
            # empty for that config, and permanently blocks re-profile
            # attempts.  Cheap query (indexed on instance_id / job_id).
            async with state.get_conn() as conn:
                await scheduler.sweep_all_stale_claims(conn)
                await conn.commit()
                node_gpu_usage = await scheduler.get_node_gpu_usage(conn)

        # Optimizer call OUTSIDE the schedule lock — see docstring.
        async with state.get_conn() as conn:
            opt_result = await optimize(conn, node_gpu_usage)

        # Hoisted so the spawn loops below the lock can see it.
        profile_evictions: list[str] = []

        async with state.schedule_lock:
            optimizer_handled: set[str] = set(opt_result.preempt)
            new_dispatches: list[Assignment] = []

            # Phase 1b: apply optimizer assignments.  Each successful UPDATE
            # claims the row (assigned_node set, status still QUEUED).  The
            # actual /run dispatch happens in a background task that waits
            # on the per-node semaphore.
            #
            # The profile-always policy is enforced at the *instance* level
            # via ``PROFILING_CONFIGS_PER_JOB`` (default 1): each job
            # instance gets one profile run before falling through to a
            # standard run.  Filling every (type, config) cell across the
            # cluster is *not* a goal — it would deadlock when the number
            # of submissions for a type is smaller than the number of cells.
            # The optimizer is therefore free to place instances that have
            # spent their profile budget, and Phase 1c only evicts for
            # instances that have NOT yet profiled.
            async with state.get_conn() as conn:
                now = datetime.now(UTC)
                for a in opt_result.assignments:
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
                    optimizer_handled.add(a.instance_id)
                await conn.commit()

                # Greedy fallback for QUEUED jobs the optimizer didn't
                # place (typically pre-profiling).
                cur = await conn.execute(
                    "SELECT id, job_id FROM jobs WHERE status = %s AND assigned_node IS NULL ORDER BY created_at ASC",
                    (JobStatus.QUEUED,),
                )
                unassigned = [(row[0], row[1]) for row in await cur.fetchall()]

                # Track which still-unassigned jobs remain after greedy, so
                # Phase 1c (below) only considers genuinely-stuck ones.
                still_unassigned: list[tuple[str, str]] = []
                for instance_id, type_id in unassigned:
                    if instance_id in optimizer_handled:
                        continue
                    result = await scheduler.schedule_job(conn, instance_id, job_type_id=type_id)
                    if result.node_id is not None and result.gpu_config is not None:
                        # ``schedule_job`` already wrote assigned_node /
                        # assigned_gpu_config to the row.  Update status's
                        # updated_at timestamp for visibility and queue the
                        # dispatch.
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

                # Phase 1c: proactive profile preempts.
                #
                # When a QUEUED job has no profile rows yet for its type,
                # the optimizer silently skips it
                # ([optimizer.py:229] ``if not profiling_rows: continue``),
                # so without an eviction it sits forever.  We evict the
                # lowest-priority non-profiling job on a node that can host
                # the target profile config so the next round can pick it
                # up.  Profiling rows themselves are never evicted (the SQL
                # filter ``status='RUNNING'`` excludes them; the defensive
                # guard in ``_preempt_and_release`` is belt-and-suspenders).
                #
                # We do NOT also fire "next-config" preempts for types
                # that have *some* profile rows but more configs remain
                # unprofiled (e.g. A40×1 profiled, A40×2 not) — the
                # eviction succeeds but the freed slot gets re-claimed by
                # standard placement before profile can grab it, and the
                # cycle thrashes.  Profile-everything-eventually is left
                # to natural cluster churn (``configs_per_job > 1`` lets a
                # single instance carry multiple profiles in sequence).
                evicted_in_round: set[str] = set()
                for instance_id, type_id in still_unassigned:
                    # One profile per job: only evict for a queued instance
                    # that has never been profiled.  Once an instance has
                    # spent its profile budget (configs_per_job, default 1),
                    # it's just waiting for a standard slot — evicting
                    # someone else to backfill a different (type, config)
                    # cell would create extra profile runs the user never
                    # asked for.  Natural cluster churn fills remaining
                    # cells when *new* unprofiled instances arrive.
                    already_profiled = await scheduler._count_profiled_this_round(conn, instance_id)
                    if already_profiled >= scheduler.configs_per_job:
                        continue
                    victims = await scheduler.try_preempt_for_profile(conn, instance_id, type_id)
                    for victim in victims:
                        if victim in evicted_in_round:
                            continue
                        profile_evictions.append(victim)
                        evicted_in_round.add(victim)
                        logger.info(
                            "Profile-preempt (unprofiled type): evicting %s for unprofiled %s (type=%s)",
                            victim[:8],
                            instance_id[:8],
                            type_id,
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
        """Safety net: retry scheduling every 60 s in case something was missed."""
        while True:
            await asyncio.sleep(60)
            try:
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

    # Minimum interval between optimizer passes — dampens churn.
    # The optimizer doesn't penalise migrations, so consecutive calls under
    # similar inputs may produce slightly different placements (an
    # equivalent point on the cost/tardiness frontier).  We faithfully
    # execute each plan, which means jobs can be killed and restarted before
    # they finish a single epoch.  Holding off ~5 s between passes lets the
    # cluster settle into the previous plan; the next pass either reaffirms
    # it (no preempts) or has a substantively better one to act on.
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
                # Re-check the event so we coalesce notifies that arrived
                # while we were sleeping (without losing them — the next
                # iteration will pick them up via the event still being set).
            try:
                _last_schedule_at = asyncio.get_running_loop().time()
                await _schedule_waiting_jobs()
            except Exception:
                logger.exception("Scheduler pass triggered by notify failed")

    async def _slot_listener() -> None:
        """Release per-node slot permits when workers fire ``ijm_slot_freed``.

        Payload is ``"<node_id>:<n_gpus>"``.  Receipt of a notification means
        the worker has fully cleaned up that slot's container, so the
        semaphore release is safe.

        On every reconnect we also call ``recover_from_drift`` to catch up on
        NOTIFYs lost during the disconnect window — without this, a tunnel
        drop while a job completes leaves the API holding a permit that can
        never be released.
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
                        payload = (n.payload or "").strip()
                        if not payload:
                            continue
                        try:
                            node_id, n_str = payload.rsplit(":", 1)
                            count = int(n_str)
                        except ValueError:
                            logger.warning("malformed slot-freed payload: %r", payload)
                            continue
                        if state.node_slots is None:
                            continue
                        state.node_slots.release(node_id, count)
                        # Wake the scheduler: a freshly-vacant slot may
                        # unblock a previously-unplaceable QUEUED row.  Also
                        # required for migrations — after auto-preempt
                        # nullifies the old assignment, Phase 1b can finally
                        # apply the optimizer's intended new placement.
                        notify_event.set()
            except Exception:
                logger.warning("Slot listener lost connection, reconnecting in 5s")
                await asyncio.sleep(5)

    async def _drift_watcher() -> None:
        """Periodic drift heartbeat — corrects in-memory ``_used`` if it has
        diverged from the DB authoritative count.  Catches anything the
        listener missed: leftover containers from pre-fix runs completing,
        bugs in new release paths, etc.  At 5 min cadence it's cheap and
        the warnings double as an alerting signal — drift in steady state
        means a code path is leaking somewhere worth investigating.
        """
        while True:
            await asyncio.sleep(300)
            try:
                if state.node_slots is None:
                    continue
                drift = await state.node_slots.detect_drift(state.get_conn)
                if drift:
                    logger.warning("Drift watcher: %s — reconciling", drift)
                    await state.node_slots.recover_from_drift(state.get_conn)
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
    await state.job_runner.shutdown()
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

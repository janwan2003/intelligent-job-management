# Per-node slot semaphore — invariants & lifecycle

The API holds an `asyncio.Semaphore` per cluster node sized at the node's GPU count. Every container that occupies a GPU must correspond to exactly one acquired permit; every release must match exactly one acquire. This file lists the invariants and every code path that touches them, so future contributors don't accidentally re-introduce the bugs we hit during the matemagician=3 incident.

## Hard invariants

1. **Capacity:** for every node `N`, at all times `node_slots._used[N] ≤ node_slots._totals[N]`. `acquire` increments `_used` only after `await sem.acquire()` returns; release decrements with `max(0, ...)`. `SLOT-OVERSUB`/`SLOT-UNDERFLOW` ERROR lines fire if either bound is breached.
2. **Pairing:** every successful `node_slots.acquire(N, n)` is matched by exactly one `node_slots.release(N, n)`. The pairing happens via *one* of the paths in [Section: Release sources](#release-sources) — never two of them, never zero.
3. **Single dispatch path:** any code that causes a container to start on a worker MUST first go through [`JobDispatcher.dispatch_with_slot`](../backend/src/job_dispatcher.py). Calling `JobDispatcher.enqueue` or worker `dispatch_job` directly bypasses the acquire and is forbidden.
4. **Authoritative state lives in the DB.** In-memory `_used` is a derived count. If they disagree, `recover_from_drift` reconciles to the DB. The drift watcher runs this every 5 min; the slot listener runs it on every reconnect.

## Acquire sources

| Path | Where | When |
|---|---|---|
| `dispatch_with_slot` | [`backend/src/job_dispatcher.py:47`](../backend/src/job_dispatcher.py#L47) | Called by `_dispatch_when_slot_free` (optimizer/greedy), `routers/jobs.create_job`, `routers/jobs.resume_job` |
| `NodeSlots.reconcile` startup pre-acquire | [`backend/src/node_slots.py:reconcile`](../backend/src/node_slots.py) | At API startup, for every RUNNING/PROFILING row |
| `NodeSlots.recover_from_drift` (positive delta) | [`backend/src/node_slots.py:recover_from_drift`](../backend/src/node_slots.py) | When DB count > in-memory count after a missed NOTIFY |

## Release sources

| Path | Where | Condition |
|---|---|---|
| `dispatch_with_slot`'s `BaseException` handler | [`backend/src/job_dispatcher.py:dispatch_with_slot`](../backend/src/job_dispatcher.py) | enqueue raises (worker unreachable, 409 retries exhausted, RuntimeError on cleared `assigned_node`, CancelledError) |
| Slot listener on `NOTIFY ijm_slot_freed` | [`backend/src/app.py:_slot_listener`](../backend/src/app.py) | Worker emitted NOTIFY |
| Worker `_run_job` Phase 4 → NOTIFY | [`worker/execution.py:_notify_slot_freed`](../worker/execution.py) | Container exited (success, failure, or kill-driven 137) and row's status was still RUNNING/PROFILING when Phase 4 read it |
| Worker `/stop`'s `_persist_stop` → NOTIFY | [`worker/app.py:_persist_stop`](../worker/app.py) | `/stop` arrived, prev_status was RUNNING/PROFILING (otherwise Phase 4 already NOTIFY'd) |
| Worker `_run_job` exception path → NOTIFY | [`worker/execution.py`](../worker/execution.py) (`except Exception` block) | Pre-Phase-4 exception (mkdir failure, docker daemon hiccup) — without this NOTIFY the permit would leak |
| Worker `reconcile_job_states` → NOTIFY | [`worker/reconcile.py:reconcile_job_states`](../worker/reconcile.py) | At worker startup, RUNNING row whose container is gone — flips to FAILED + emits NOTIFY |
| Worker `/stop` zombie-kill defence → NOTIFY | [`worker/app.py:_zombie_release`](../worker/app.py) | `kill_container` raised after retries — last-resort to avoid permanent permit lock |
| Reaper, when no live dispatch task | [`backend/src/app.py:_reset_stuck_queued_assignments`](../backend/src/app.py) | Stuck QUEUED row, NO entry in `state.dispatch_tasks` — direct release |
| Reaper, when dispatch task is alive | (same file) | Cancels the task; the task's `BaseException` handler is the actual releaser |
| `NodeSlots.recover_from_drift` (negative delta) | [`backend/src/node_slots.py:recover_from_drift`](../backend/src/node_slots.py) | DB count < in-memory count |

## Forbidden patterns

- ❌ `JobDispatcher.enqueue(job_id)` directly from a router. Use `dispatch_with_slot`. (Was the matemagician=3 root cause.)
- ❌ Worker `dispatch_job(row_id)` from `pickup_queued_jobs`. The pickup path now resets `assigned_node = NULL` and lets the API re-place via `dispatch_with_slot`. (Was a phantom-release source on every worker restart.)
- ❌ Both Phase 4 and `_persist_stop` emitting NOTIFY for the same kill. Phase 4 emits when status was still RUNNING/PROFILING when it read the row; `_persist_stop` emits only when its own pre-read found RUNNING/PROFILING. The two are mutually exclusive on a single kill cycle.
- ❌ Reaper releasing for a stuck row whose dispatch task is still alive. The task's exception handler does it. Reaper cancels the task instead.

## Operator handles

- `GET /admin/slots` — per-node `total`, `available`, `used_mem`, `used_db`, `drift`, plus lifetime `metrics` (acquire/release counts, oversub/underflow flags, drift recovery count).
- `POST /admin/reconcile-slots` — force a reconcile.
- `GET /admin/dispatch-tasks` — in-flight `_dispatch_when_slot_free` tasks; alive count >> placed jobs is a smell.
- `IJM_SLOT_VERBOSE=1` (env var) — bumps slot.acquire/release lines to INFO. Set on the API container when diagnosing leaks.

## How we got here

The original codebase had only `reconcile()` and a private `detect_drift()` that nothing called. Across two oversubscription incidents we found:

1. Direct `routers.enqueue` in submission/resume paths bypassed the acquire entirely (matemagician=3 root cause). Fixed: route everything through `dispatch_with_slot`.
2. Reaper double-release race when a dispatch task was blocked on `acquire`. Fixed: reaper consults `state.dispatch_tasks` and cancels rather than racing.
3. Worker `_persist_stop` always emitted NOTIFY, including after Phase 4 had already done so. Fixed: `_persist_stop` checks pre-read status and only NOTIFYs when slot was still occupied.
4. Worker `pickup_queued_jobs` dispatched directly on the worker, bypassing API accounting. Fixed: pickup just clears `assigned_node` and NOTIFYs the API scheduler.
5. Worker startup `reconcile_job_states` marked RUNNING→FAILED rows without NOTIFYing the slot release, leaking permits across worker restarts. Fixed: emit NOTIFY in that branch too.
6. Pre-Phase-4 exceptions in `_run_job` left the slot held forever. Fixed: emit NOTIFY in the exception path.
7. NOTIFY listener disconnect lost slot-freed events during the silent window. Fixed: `recover_from_drift` runs on every reconnect; periodic drift watcher catches everything else.

Each is summarised in this doc's release-source table — keep them there if you ever extend the lifecycle.

# Plan — eliminate the slot-tracker / scheduler race

## Symptom (what we keep hitting)

`/admin/slots` reports `drift=true` after profile→standard transitions
and after migration storms.  Concretely:

| Failure mode               | Snapshot of /admin/slots                                              |
|---------------------------|-----------------------------------------------------------------------|
| Phantom-full (over-acquire) | `used_mem=2 / used_db=0` — dispatch task blocks on a slot that's free |
| Phantom-empty (under-count) | `used_mem=0 / used_db=2` — next acquire over-subscribes the bundle    |
| `release_underflow_count>0` | release path tried to free a permit it didn't hold (cascades)         |

All three are observable today on a real cluster and are auto-recovered
within ≤ minutes by `recover_from_drift`, but no recovery happens
automatically between calls today (only on demand via the admin endpoint).
While drift persists, every dispatch task waiting on the affected node is
wedged — the scheduler's logical progress halts even though the cluster
is physically idle.

## Root cause

Slot state is updated from multiple uncoordinated code paths:

1.  **Worker→API notification path.** When the worker finishes / kills a
    container, it commits the DB row to `SUCCEEDED` / `FAILED` /
    `QUEUED`-with-`assigned_node=NULL` and emits `NOTIFY ijm_slot_freed`.
    The API's listener calls `node_slots.release(node, n)`.
2.  **Dispatch task.** Just before POSTing `/run` to the worker, the
    dispatch task calls `node_slots.acquire(node, n)`.  Reads
    `node_gpu_usage` from the DB to verify there's room.
3.  **Phase 1c scheduler.** Writes `assigned_node + assigned_gpu_config`
    on a `QUEUED` row (a "reservation") and spawns a dispatch task.
    Reads `node_gpu_usage` from the DB to pick a target node.
4.  **Reaper.** Resets stuck `QUEUED` rows that have an
    `assigned_node` but no live dispatch task; calls
    `node_slots.release(node, n)` if it cleans up after a partially-
    completed reservation.
5.  **Drift recovery.** Periodic or manual reconciliation; rebases
    `_used` to match the DB authoritative count.

Each path is individually correct, but they share state without a single
serialization point:

*   Path (1)'s `release(n)` and path (2)'s `acquire(n)` for **the same
    job moving slots** race against each other.  If (2) runs before
    (1) (because the new dispatch task was scheduled by Phase 1c that
    looked at a stale snapshot), (2) sees the old usage, succeeds on the
    BoundedSemaphore (until oversub) and then (1) lands and decrements
    `_used` past the actual occupancy.
*   Path (3)'s read of `node_gpu_usage` runs outside any lock — by the
    time Phase 1c writes the row, another scheduler tick may have
    already changed the cluster state.  Two near-simultaneous Phase 1c
    decisions for the same node can both think there's room.
*   Path (4)'s `release` doesn't check whether the dispatch task is
    actually still in flight; if it cancels a task that *did* acquire,
    we get an over-release; if it cancels a task that hadn't yet
    acquired, we get an over-release on the next worker NOTIFY.

The current safety net is `BoundedSemaphore` + `release_underflow_count`
counter, which catches over-release but doesn't fix over-acquire.

## Design: single per-node serialization queue

Replace the per-node `asyncio.Lock` with a per-node operation queue.
Every slot-affecting operation submits a typed message to the queue and
awaits its completion.  The queue is drained by a single coroutine per
node, which is the **only** place `_used`, the semaphore, and any
DB read used for placement decisions can be touched.

Operation types:

```python
@dataclass
class SlotOp:
    kind: Literal["acquire", "release", "reserve", "transition", "reconcile"]
    n: int                       # how many GPUs
    n_new: int | None = None     # only for "transition"
    job_id: str | None = None    # for tracing
    done: asyncio.Future         # caller awaits on this
```

Each op runs to completion before the next op on the same node starts.
Two key invariants this gives us for free:

1.  Between the moment a `release` is dequeued and the moment the next
    `acquire` is dequeued, no other code can read `node_gpu_usage` from
    the DB and act on stale state — because all code that does that
    routes through the queue.
2.  `transition(old_n, new_n)` becomes a single atomic op: dequeue,
    release old, acquire new, signal completion.  No window where a
    different op can sneak in.

Cross-node operations (e.g. "migrate job from polimi-gpu to matemagician")
still need two ops, one on each node's queue.  That's fine: each is
internally atomic; the only thing that can happen between them is
*nothing*, which is exactly what we want — the job is unassigned during
the gap, the optimizer won't double-place it because the row says so.

### API surface

```python
class NodeSlots:
    # Existing entrypoints — same signatures, internally enqueue ops:
    async def acquire(self, node_id: str, n: int = 1) -> None: ...
    def     release(self, node_id: str, n: int = 1) -> None: ...

    # New entrypoint for atomic config transitions on the same node:
    async def transition(
        self, node_id: str, old_n: int, new_n: int
    ) -> None: ...

    # New entrypoint that scheduler uses BEFORE writing assigned_node
    # to the DB.  Returns True iff the reservation succeeded; the
    # caller is then guaranteed the slot is theirs until released.
    async def reserve(
        self, node_id: str, n: int = 1, job_id: str | None = None
    ) -> bool: ...
```

The current `acquire` is renamed `_acquire_blocking` and used internally
by the queue worker.  External callers keep using `acquire` for
backwards compatibility; internally it now enqueues a `SlotOp(kind="acquire")`.

### Migration path

1.  Land the queue worker per node behind a feature flag
    (`IJM_SLOT_QUEUE=1`).  Default off so we can revert by env if we
    find a latent bug under load.  Both code paths share `_used` and
    the `BoundedSemaphore`, so the flag flip is hot-swappable.
2.  Convert the dispatch task's `release+acquire` pair to a single
    `transition` call when the job stays on the same node.  Same-node
    transitions are the most common race in practice (profile→standard).
3.  Add `reserve` to Phase 1c.  Currently Phase 1c writes
    `assigned_node` to the DB before any slot bookkeeping; with `reserve`
    the write happens **after** the reservation succeeds, so a Phase 1c
    decision can never be ahead of the in-memory tracker.
4.  Move the reaper's `release` calls into the queue so they're ordered
    against in-flight dispatches.  This eliminates the "reaper cancels a
    dispatch that just acquired" double-release.
5.  Once those four steps are in production for a few days, default
    `IJM_SLOT_QUEUE=1` and delete the legacy lock-only path.

### Tests to add

*   **Unit:** stress test `NodeSlots` with `N` concurrent
    `acquire`/`release`/`transition` ops on a 2-permit semaphore and
    assert that `_used` always equals the count of currently-acquired
    callers.  Run 10 000 ops per node per test.
*   **Integration:** simulate the profile→standard transition race
    (worker NOTIFY arrives while dispatch task is mid-acquire).  Today
    this corrupts `_used`; under the queue it must not.
*   **End-to-end:** rerun scenario 1 with `IJM_SLOT_QUEUE=1` and
    assert `release_underflow_count == 0` and `drift_recovery_count
    == 0` across the full run.  Today both are routinely > 0.

### Out of scope (intentional)

*   Cross-node atomic migration.  The optimizer can already plan a
    migration in two halves (preempt + re-dispatch); making it look
    atomic to the user would require a different abstraction
    (transactional placement plans) and isn't required to fix drift.
*   Reservation timeouts.  If a `reserve` succeeds but the dispatcher
    crashes before the corresponding `acquire`/`release` lands, the
    reservation leaks.  Today the reaper handles this; under the queue
    a `reserve(timeout=…)` could auto-release, but we'd rather keep the
    reaper authoritative for "did this job actually run".
*   Per-GPU bookkeeping vs.\ per-bundle bookkeeping.  Currently we count
    *permits per node*, not which physical GPU each one represents.
    That's separate work and orthogonal to drift.

## Risk

The biggest risk is throughput: serializing all slot ops per node means
the queue worker is on the critical path of every dispatch.  In
practice the work per op is tiny (semaphore counter math + a couple of
dict lookups), and the queue handles ≥ 10⁴ ops/sec on a single Python
coroutine, so a ~10-node cluster running ≤ 100 ops/min per node is
nowhere near the throughput limit.  The only operation that holds the
queue longer than microseconds is the DB read inside `reserve`, which
runs ~1 ms over a local socket — acceptable.

Secondary risk: a bug in the queue worker hangs the queue and blocks
**all** future ops on that node, making the drift problem worse than
today.  Mitigation: aggressive logging + a watchdog that resets the
queue worker if no op completes for > 30 s + the feature flag from
step 1.

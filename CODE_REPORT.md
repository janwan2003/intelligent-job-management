# IJM — Code Report

A file-by-file engineering report on **Intelligent Job Management (IJM)**: the
backend, worker, shared library, training runtime, tests, and infra we built
over many months of iteration. It documents *what each file is, what's inside
it, the data flow it sits in, the hard problems it solved, and where the
remaining hacks live* — so the **reasons** behind the code are preserved, not
just its current shape.

> This is a companion to — not a replacement for — [README.md](README.md)
> (setup, ports, env vars, deployment) and [CLAUDE.md](CLAUDE.md) (engineering
> principles). Where this report says "why", those say "how to run". The
> authoritative correctness spec for the slot accounting is
> [documentation/SLOT_INVARIANTS.md](documentation/SLOT_INVARIANTS.md).

**What IJM is, in one paragraph.** A job-management system for GPU deep-learning
clusters with **stoppable/resumable** training jobs, modelled after Polimi's
ANDREAS project. A FastAPI **backend** on the user's machine dispatches jobs over
SSH-tunnelled HTTP to per-node **workers** that drive the Docker CLI directly.
**PostgreSQL** is the single durable source of truth and the event bus
(`LISTEN/NOTIFY`). An external **GPUspb optimizer** makes cost-aware placement
decisions. Every job type is **profiled** on real hardware before the optimizer
trusts it. The same checkpoint must resume across nodes running wildly different
PyTorch/CUDA versions. Most of the engineering effort — and most of this report
— is about the *invisible correctness problems* that only show up in a
distributed, preemptive, multi-version cluster.

---

## Table of contents

1. [How to read this document](#1-how-to-read-this-document)
2. [Architecture at a glance](#2-architecture-at-a-glance)
3. [The hard problems & iterations (the "why months" section)](#3-the-hard-problems--iterations)
4. [Backend — `backend/src/`](#4-backend--backendsrc)
5. [Worker — `worker/`](#5-worker--worker)
6. [Shared — `shared/`](#6-shared--shared)
7. [Runtime — `runtime/`](#7-runtime--runtime)
8. [Tests — `backend/tests/`](#8-tests--backendtests)
9. [Infra & deployment — `infra/`](#9-infra--deployment--infra)
10. [Design docs — `documentation/`](#10-design-docs--documentation)
11. [Consolidated hacks & band-aids](#11-consolidated-hacks--band-aids)
12. [Appendix: full file inventory](#12-appendix-full-file-inventory)

---

## 1. How to read this document

The report is organized top-down. **§2–§3 are the narrative** — read them first
to understand the system and the problems that shaped it. **§4–§10 are the
reference** — per-file entries you can jump to. Each core-file entry has four
parts:

- **Purpose** — the one role this file plays.
- **Inside** — the key classes/functions/endpoints/data structures.
- **Data flow** — what it reads/writes, who calls it, what it calls.
- **Challenges & hacks** — the hard problems it solves and any workaround that
  diverges from the "no band-aids" principle (cross-referenced to §11).

A recurring word in this codebase is **instance vs. type**. A job *type* is the
`job_id` column (a logical workload, e.g. "cnn_big"); a job *instance* is the
`id` column (one submission, a UUID). Profiling results are keyed by **type** so
all instances share them; in-flight profile claims and slots are tracked by
**instance**. Keep this distinction in mind — it's load-bearing throughout.

---

## 2. Architecture at a glance

### 2.1 Topology

The **supported, target topology is distributed** (the local `docker compose up`
path exists only for dev convenience):

```
            ┌──────────────────────── user's machine ────────────────────────┐
            │                                                                 │
   browser ─┼─▶ React SPA ──▶ FastAPI backend (backend/src) ──▶ GPUspb        │
            │      (Vite)         │     ▲          │              optimizer    │
            │                     │     │          │              (HTTP /v5)   │
            └─────────────────────┼─────┼──────────┼────────────────────────-─┘
                                  │     │          │  HTTP /run /stop /logs
                       psycopg    │     │ LISTEN/   │  (SSH-tunnelled)
                       pool       ▼     │ NOTIFY    ▼
                            ┌───────────────┐   ┌──────────── GPU nodes ────────────┐
                            │  PostgreSQL   │◀──│ worker (worker/) → Docker CLI      │
                            │  (18+)        │   │   matemagician: P600×2, torch 1.5.1│
                            │  source of    │◀──│ worker (worker/) → Docker CLI      │
                            │  truth + bus  │   │   polimi-gpu:   A40×2,  torch 2.6  │
                            └───────────────┘   └────────────────────────────────────┘
```

Key consequence: **the API process is the single in-memory consumer** of slot
state, but it holds **no durable state of its own** — Postgres is authoritative,
and the API rebuilds its in-memory view from the DB on startup
(`NodeSlots.reconcile`). The workers and the API never talk directly; they
coordinate **only** through Postgres rows and `NOTIFY` channels.

### 2.2 Job lifecycle

```
            ┌─────────── profile-always policy ───────────┐
submit ──▶ QUEUED ──▶ PROFILING ──▶ QUEUED ──▶ RUNNING ──▶ SUCCEEDED
              ▲   (measure 1 GPU       │  (now      │  │
              │    config per round)   │  trusted)  │  └─▶ FAILED
              │                        │            │
              └────────────────────────┴────────────┴─▶ PREEMPTED ──(user resume)──▶ QUEUED
                                       (auto-preempt
                                        re-queues;     user /stop is "sticky":
                                        no resume       stays PREEMPTED until
                                        needed)         the user resumes)
```

- **QUEUED → PROFILING → QUEUED**: a fresh job type is benchmarked on one real
  GPU configuration per scheduler round before the optimizer is allowed to place
  it. The re-queue happens via `NOTIFY ijm_schedule` after the worker records the
  measurement — never via an in-memory fast-path.
- **QUEUED → RUNNING**: the optimizer (or a greedy/profile-preempt fallback)
  assigns a node, the API acquires a slot permit, and posts `/run`.
- **RUNNING → PREEMPTED / QUEUED**: a `/stop` arrives. `reason=user` parks the
  job at **PREEMPTED** (sticky — only the user resumes it). `reason=auto`
  (optimizer/profile eviction) drains the row back to **QUEUED** so the next
  scheduler pass re-places it.
- **RUNNING → SUCCEEDED / FAILED**: container exits; the worker records the exit
  code and frees the slot.

### 2.3 Three core data flows

**(a) Submission → dispatch** (`POST /jobs`):

```
JobCreate ─▶ INSERT jobs(status=QUEUED, id=uuid)
          ─▶ scheduler.schedule_job()          # claim a profile config OR mark standard
          ─▶ dispatch_with_slot(instance, node, cfg)
                 ├─ node_slots.acquire(n)       # block on per-node semaphore
                 ├─ await in-flight /stop drains on this node
                 └─ POST /run ─▶ worker          # on failure: release permit, leave QUEUED
```

**(b) NOTIFY-driven scheduler loop** (the steady-state engine):

```
worker emits NOTIFY ijm_schedule  ──┐
worker emits NOTIFY ijm_slot_freed ─┤
drift watcher / reaper / startup ───┤
                                    ▼
                          notify_event.set()
                                    │  (debounced; ≥5s between optimizer passes)
                                    ▼
                       _schedule_waiting_jobs():
                         1. reset stuck QUEUED assignments (reaper)
                         2. optimize()  [HTTP, OUTSIDE the lock]
                         3. apply optimizer assignments  (Phase 1b)
                         4. profile-place leftovers       (Phase 1a)
                         5. profile-preempt unprofiled    (Phase 1c)
                         6. spawn preempt + dispatch tasks in parallel
```

**(c) Slot acquire/release pairing** (the invariant that took the longest to get
right):

```
acquire:  dispatch_with_slot ──▶ node_slots.acquire(node, n)   (in-memory semaphore++)
                                          │
                                 POST /run ─▶ worker runs container
                                          │
release:  worker frees container ──▶ NOTIFY ijm_slot_freed "node:n:reason"
                                          │
          API _slot_listener ──▶ node_slots.release(node, n)    (in-memory semaphore--)
```

Every `acquire` must pair with **exactly one** `release` from **exactly one** of
nine code paths. Getting that pairing wrong is what caused the
`matemagician=3` oversubscription incidents — see §3.1.

---

## 3. The hard problems & iterations

This section is the "why we spent months" story. None of these problems are
visible from the code shape; each was found by running the system under
preemption and migration, watching it drift, and tracing the root cause. The
git history records the arc: single-node in-process prototype → HTTP multi-node
workers → optimizer integration → **months of slot-correctness hardening** →
deterministic reproducible scenarios.

### 3.1 GPU-slot oversubscription — the `matemagician=3` incident

**Symptom.** matemagician (a 2-GPU node) ended up running **3 containers on 2
GPUs**. Training jobs fought over the same physical GPU and slowed to a crawl.

**Root causes (plural — this was several bugs wearing one symptom):**
1. Some router paths called `JobDispatcher.enqueue()` **directly**, bypassing the
   slot `acquire`. The container started but no permit was taken — so the
   accounting was off by one immediately.
2. The worker's old `pickup_queued_jobs()` **dispatched rows itself** on restart.
   When that container later finished, the worker emitted `ijm_slot_freed` and
   the API released a permit it had **never acquired** — leaking one phantom
   permit per worker restart.
3. Pre-Phase-4 exceptions in the worker (`mkdir` failure, docker hiccup) left the
   slot held forever (no `NOTIFY` ever fired).

**Fix — one invariant, enforced everywhere:** *every container start goes through
[`JobDispatcher.dispatch_with_slot`](backend/src/job_dispatcher.py), which is the
only acquire site.* The worker's `pickup_queued_jobs` now just **clears
`assigned_node`** and notifies the API to re-place the row through the proper
acquire path. Every worker exit path — success, failure, kill, pre-Phase-4
exception, zombie cleanup — emits exactly one `ijm_slot_freed`.

**Why it matters.** This is the spine of the whole system. Oversubscription
silently destroys the cost model the optimizer is built on. The invariants and
all nine release paths are now pinned in
[SLOT_INVARIANTS.md](documentation/SLOT_INVARIANTS.md) and guarded by
[test_node_slots.py](backend/tests/test_node_slots.py).

### 3.2 Permit leaks & drift — DB as source of truth

**Symptom.** Even with the single dispatch path, a dropped SSH tunnel during a
`NOTIFY` could lose a slot-freed event. The in-memory semaphore would then
believe a slot was occupied forever; dispatches to that node blocked.

**Fix.** Treat the **DB as authoritative** and the in-memory `_used` count as
**derived**. `NodeSlots.detect_drift()` compares the two (counting only
RUNNING/PROFILING rows, which are the only ones holding a live container), and
`recover_from_drift()` force-rebases the semaphore to the DB count. This runs:
on every slot-listener reconnect (catch events lost during downtime), on a
15-second `_drift_watcher` heartbeat (`IJM_DRIFT_HEARTBEAT_S`), on
`POST /admin/reconcile-slots`, and from the stuck-QUEUED reaper.

**Honesty note (→ §11).** The drift watcher *is* a safety net layered over an
event system — a "rule of thumb that compensates". It's justified because a
distributed event bus over SSH tunnels genuinely *will* drop events, and the DB
is a real, cheap source of truth to reconcile against. But it's the place to
look first if a release path is ever found to be missing a `NOTIFY`.

### 3.3 Optimizer churn on self-preemption

**Symptom.** When the API preempted a job to execute a migration plan, the
worker's `ijm_slot_freed` `NOTIFY` woke the optimizer again. GPUspb's near-tie
cost surface is nondeterministic — so it would propose a *different but
cost-equivalent* plan, flip-flopping jobs mid-execution and never converging.

**Fix.** The slot-freed payload carries a **reason**
([`SlotFreedReason`](shared/constants.py)). The API's `_slot_listener` releases
the permit on every event but only **wakes the optimizer** for *external* events
(`TERMINAL` / `USER_STOP` / `ORPHAN_DRAIN`). `AUTO_PREEMPT` — the API's own
in-flight plan — releases silently; the plan's own dispatch step picks up the
freed slot. Plan-execution safety is preserved because four independent wake
sources (dispatch-exception, drift watcher, reaper, 60-min queue watcher) still
recover if the plan stalls. Pinned by
[test_slot_payload.py](backend/tests/test_slot_payload.py).

### 3.4 Phantom preempts in the dispatch window

**Symptom.** Between the API posting `/run` and the worker flipping the row to
`RUNNING` (a ~5s window over a tunnel), the row looked QUEUED-with-assignment.
The optimizer's `currentScheduling` only listed RUNNING jobs, so it thought the
slot was free and proposed a spurious preempt "to make room" that already
existed.

**Fix.** The optimizer's `currentScheduling` payload includes QUEUED rows that
already have an `assigned_node` — they reserve their slot through the dispatch
window. Symmetrically, `NodeSlots` deliberately does **not** count
QUEUED-with-assignment as occupied for drift purposes (the permit is acquired at
`/run` time, not assignment time), keeping acquire/release symmetric.

### 3.5 Silently-dropped migrations

**Symptom.** When the optimizer wanted to **move** a RUNNING job to a different
node, the apply step's `UPDATE ... WHERE assigned_node IS NULL` could never
match (the row was still RUNNING on the source). The migration was silently
dropped.

**Fix.** A migrate is detected as an instance appearing in **both** the
optimizer's assignments and its preempt list. The new placement is stashed in
`state.pending_migrates`; the slot listener calls `_apply_pending_migrates()`
after *every* slot-freed `NOTIFY`. The conditional `UPDATE` only fires once the
source `/stop` has drained the row to `QUEUED + NULL`, so calling it on every
event is safe. This became necessary *because of* the §3.3 fix — previously the
churn-wake accidentally re-applied the plan; once we suppressed that wake, the
migrate had to be carried across the preempt explicitly. Pinned by
[test_pending_migrates.py](backend/tests/test_pending_migrates.py).

### 3.6 Profile-always policy

**Symptom & rationale.** The GPUspb optimizer needs **per-epoch execution times**
per GPU configuration to make cost decisions. A brand-new job type has none, so
the optimizer can't place it sensibly. The policy: **profile every type on real
hardware before trusting it.**

**Fix.** `ProfilingScheduler` claims one untested GPU config per submission
(`PROFILING_CONFIGS_PER_JOB`, default 2 over rounds), runs a short PROFILING run
on a profiling-designated node, records the measured per-epoch time, and
re-queues. The optimizer **gates** instances that still owe a profile so they
can't be placed prematurely. When no free profiling slot exists, a dedicated
**profile-preempt** pass (Phase 1c) evicts the cheapest victim to make room —
otherwise an unprofiled type (invisible to the optimizer) would sit forever.

### 3.7 Cross-node checkpoint resume across torch 1.5.1 ↔ 2.6

**Symptom.** `cnn_big` migrates between the modern A40 node (torch 2.6) and the
legacy P600 node (torch 1.5.1). A checkpoint written by one must load on the
other, across a 6-year PyTorch version gap and over a FUSE-mounted shared
filesystem.

**Fix (all in [`runtime/base.py`](runtime/base.py)):**
- **Always save in pre-1.6 tar+pickle format** (`_use_new_zipfile_serialization=
  False`, with a `TypeError` fallback for torch <1.6 where the kwarg doesn't
  exist) — torch 1.5.1 can't read the newer zip format.
- **Pre-read the file into `io.BytesIO`** before `torch.load` — FUSE mounts can
  `EPERM` on the `mmap`/`seek` syscalls `torch.load` issues directly on a path.
- **`load_state_dict(strict=False)`** tolerates BN-naming / `module.`-prefix
  differences between versions.
- **Optimizer state load wrapped in try/except** — if the legacy torch refuses
  the newer Adam state shape, restart the optimizer from scratch (a few epochs
  of LR warmup, invisible at scenario timescales) rather than fail the resume.
- **`weights_only=True`** with a `TypeError` fallback (the kwarg is torch ≥1.13).

**Why it matters.** This is the feature that makes "stoppable/resumable across a
heterogeneous cluster" real rather than aspirational, and it's deliberately
*loose* — CLAUDE.md explicitly warns against tightening the loader.

### 3.8 Python 3.7 legacy-image constraint

**Symptom.** The legacy CUDA-10.1 image ships Python 3.7, but the worker/runtime
code is written for modern Python.

**Fix.** Any code that runs inside the legacy image (`runtime/base.py` and the
training scripts) uses `from __future__ import annotations` so PEP 604 (`str |
None`) and PEP 585 (`tuple[X, Y]`) annotations stay stringified at runtime;
nested (non-parenthesized) `with` blocks for 3.7 syntax; and torch API fallbacks
(`weights_only`, `_use_new_zipfile_serialization`). A subtle one:
**legacy torch 1.5.1 emits a cuDNN warning via C++ `std::cerr` (fd 2)**,
bypassing Python's `warnings` and `sys.stderr` entirely — so `base.py` installs
a **file-descriptor-level filter** that redirects fd 2 through a pipe and a
background thread drops the noise lines.

### 3.9 Cross-node filesystem, UID, and rootless-docker issues

**Symptoms & fixes (in [`worker/docker.py`](worker/docker.py) and
[`worker/execution.py`](worker/execution.py)):**
- **GPU pinning.** `--gpus all` plus `NVIDIA_VISIBLE_DEVICES` (or CDI
  `--device nvidia.com/gpu=N` for rootless) both default to GPU 0, so two 1-GPU
  jobs landed on the same physical GPU. Fix: the worker tracks
  `_claimed_gpus` and pins **concrete distinct indices** per job under a lock.
- **Rootless DNS.** slirp4netns's nameserver can't reach public mirrors, turning
  a cached dataset into a hard failure. Fix: pin public DNS (8.8.8.8 / 1.1.1.1)
  on rootless workers + prefer pre-staged on-disk datasets.
- **Cross-node UID.** Checkpoints written `root:root 0600` on one node couldn't
  be read over an SFTP/rclone mount by another. Fix: pin `--user uid:gid` to the
  host data-dir owner and `chmod 0777` the checkpoint dirs.

---

## 4. Backend — `backend/src/`

FastAPI application. **3,433 LOC across 19 files.** The center of gravity is
`app.py` (the scheduler engine), `node_slots.py` (the in-memory slot semaphores),
`job_dispatcher.py` (the single dispatch path), `profiling.py` (profile-always),
and `optimizer.py` (the GPUspb client). Everything else is models, config,
routers, and helpers.

### 4.1 `app.py` — application factory, lifespan, scheduler engine *(752 LOC)*

**Purpose.** Build the FastAPI app, own the application lifespan, and run the
five background tasks that *are* the scheduler.

**Inside.**
- `lifespan()` — loads cluster config, opens the psycopg `AsyncConnectionPool`
  (min 2 / max 50), runs `schema.sql`, builds the `JobDispatcher` and
  `NodeSlots` (then `reconcile()`s slots from the DB), and starts five tasks:
  `_queue_watcher` (60-min safety net), `_notify_listener` (LISTEN
  `ijm_schedule`), `_notify_consumer` (debounce + ≥5s min interval between
  optimizer passes), `_slot_listener` (LISTEN `ijm_slot_freed`), `_drift_watcher`
  (15s heartbeat). It kicks `notify_event` once at startup so jobs queued across
  a restart get placed immediately.
- `parse_slot_payload()` — module-scope parser for `"node:n:reason"` (and legacy
  `"node:n"` → `TERMINAL`); hoisted out of the lifespan so unit tests can call it.
- `_schedule_waiting_jobs()` — **the core loop.** Acquires `state.schedule_lock`
  only around DB writes; runs `optimize()` *outside* the lock so the HTTP call
  can't block other scheduler work. Three placement phases: **1b** apply
  optimizer assignments (stashing migrates into `pending_migrates`), **1a**
  profile-place QUEUED rows the optimizer skipped via `scheduler.schedule_job()`,
  **1c** profile-preempt for still-unassigned unprofiled instances. Preempt and
  dispatch tasks are spawned **outside** the lock and coordinate via `NodeSlots`.
- `_preempt_and_release()` — fire-and-forget auto-preempt; **refuses to preempt a
  profiling run** ("profiling is sacred") and skips rows already PREEMPTED by a
  user-stop in flight.
- `_dispatch_when_slot_free()` — one task per assignment; delegates to
  `dispatch_with_slot` so it shares the exact acquire-then-`/run` path as the
  direct submission/resume routes.
- `_reset_stuck_queued_assignments()` — 30-s reaper for orphaned
  QUEUED-with-assignment rows; if a live dispatch task exists it cancels it
  (the task's own handler releases the permit) rather than double-releasing.
- `_apply_pending_migrates()` / `_slot_listener()` / `_drift_watcher()` — the
  migration and slot-reconciliation machinery described in §3.3–§3.5.

**Data flow.** Reads/writes `jobs` and `profiling_results`; LISTENs on two
channels; calls `optimize()`, `scheduler.*`, and `dispatch_with_slot()`; shares
in-memory state through [`state.py`](backend/src/state.py).

**Challenges & hacks.** This file *is* §3.2–§3.6 made concrete. Notable inline
band-aids (→ §11): the 30-s `_STUCK_DISPATCH_THRESHOLD_S` reaper, the 15-s drift
heartbeat, the duplicate-dispatch-task guard (spawning twice = two acquires =
one leaked permit), and the "refuse to auto-preempt a profiling run" belt-and-
suspenders check that logs an ERROR if any upstream caller has a bug. A long
comment block (lines ~158–172) documents a *removed* mechanism
(`pending_evictions`) and explicitly warns against re-introducing it — exactly
the kind of institutional memory this report exists to preserve.

### 4.2 `node_slots.py` — per-node GPU semaphores *(362 LOC)*

**Purpose.** The in-memory representation of "how many GPUs are free on each
node", gating dispatch so a slow preempt on one node doesn't block another.

**Inside.** `NodeSlots` holds, per node: `_totals` (immutable GPU count),
`_sems` (an `asyncio.BoundedSemaphore` sized at the total), `_used` (a derived
mirror counter for diagnostics — avoids reaching into the private
`Semaphore._value`), and `_acquire_locks` (per-node lock). Methods:
- `acquire(node, n)` — under the per-node lock, take `n` permits with
  **partial-rollback** on cancellation; logs `SLOT-OVERSUB` if `_used > total`.
- `release(node, n)` — `BoundedSemaphore.release()` (caps capacity so an
  over-release can't silently inflate); logs `SLOT-OVERRELEASE` on underflow.
- `reconcile(get_conn)` — at startup, pre-acquire one permit per GPU for each
  RUNNING/PROFILING row (**not** QUEUED-with-assignment — those have no live
  container).
- `detect_drift()` / `recover_from_drift()` — compare to and rebase from the DB
  authoritative count (§3.2).
- `metrics()` — lifetime counters for `/admin/slots`.

**Inside — the three hard invariants** (from SLOT_INVARIANTS.md):
1. **Capacity:** `_used[N] ≤ _totals[N]` always.
2. **Pairing:** every `acquire(N, n)` matched by exactly one `release(N, n)`,
   from exactly one of the nine release paths.
3. **Single dispatch path:** all container starts go through
   `dispatch_with_slot` (calling `enqueue`/worker `dispatch_job` directly is
   forbidden).

**Challenges & hacks.** The per-node lock fixes a real deadlock: two concurrent
`acquire(2)` on a 2-permit node would otherwise each grab 1 and stall.
`BoundedSemaphore` is the structural guard against the oversubscription class of
bug. The whole drift subsystem is the §3.2 safety net.

### 4.3 `job_dispatcher.py` — the single dispatch path *(285 LOC)*

**Purpose.** The one place that turns "this instance should run on this node" into
an actual `/run` POST, with the slot permit held for exactly the right window.

**Inside.** `JobDispatcher`:
- `dispatch_with_slot(instance, node, cfg)` — **the entry point.** Acquires
  `n = sum(cfg.values())` permits; awaits any in-flight `/stop` on the same node
  (so `/run` doesn't race the previous container's CUDA release); re-fetches
  `assigned_node` and bails if the row was reassigned; calls `enqueue()`. On any
  `BaseException` (including `CancelledError`) it **releases the permit** and
  re-raises — this is the dispatch-side release path.
- `enqueue(job_id)` — fetch the worker URL, POST `/run`; raises if
  `assigned_node` was cleared (preempt) or no worker URL exists.
- `stop(job_id, reason)` — fire-and-forget async `/stop`, **deduplicated** by
  `(node, job_id)` via `_inflight_stops` so two preempts don't double-kill.
- `_await_inflight_stops_on(node)` — block (10-s defensive bound) until pending
  `/stop`s on a node drain before dispatching.
- `_remote_request()` — HTTP with retry: `/run` retries 3× (a 409 can happen mid-
  migration); `/stop` is single-attempt best-effort (worker reconcile catches
  orphans).

**Data flow.** Called by `routers/jobs` (create/resume) and `app._dispatch_when_
slot_free`. Calls `node_slots.acquire/release` and the worker HTTP API.

**Challenges & hacks.** The 10-s timeout on `_await_inflight_stops_on` is a
defensive bound (→ §11): if a worker truly hangs, the drift heartbeat repairs the
slot anyway. Routing **everything** through this method is the §3.1 fix.

### 4.4 `profiling.py` — the ProfilingScheduler *(588 LOC)*

**Purpose.** Implements profile-always (§3.6): decide whether a job runs in
profiling or standard mode, claim configs atomically, and find/evict slots for
profiling.

**Inside.** `ProfilingScheduler`:
- `get_valid_configurations()` — intersection of profiling-node and
  production-node GPU configs, deduped and sorted deterministically.
- `get_node_gpu_usage()` — current per-node allocation, optionally reserving
  in-flight unmeasured claims.
- `schedule_job()` — the per-instance decision: inherit in-flight claims or
  atomically claim new cells (`_atomic_claim_remaining` with `ON CONFLICT DO
  NOTHING` for race-safety); set `is_profiling_run` and the assignment.
- `try_preempt_for_profile()` — find the cheapest victim set to evict so an
  unprofiled type can profile.
- `_count_profiled_this_round()`, `get_profiled_configs()`,
  `gc_orphaned_claims()` (15-min lease cleanup for crashed claims).

**Data flow.** Called from `routers/jobs` (create/resume) and `app` (Phase 1a/1c).
Reads/writes `jobs` and `profiling_results`. Worker `handle_complete` writes the
`duration_seconds` that this scheduler later reads.

**Challenges & hacks.** Atomic claim via `ON CONFLICT DO NOTHING` is the
race-safe core; `include_cross_instance_claims=False` at placement time is a
**deliberate trade-off** (the slot semaphore, not a reservation, is the real
arbiter — reserving would deadlock peers). `gc_orphaned_claims` is a safety net
for API-crash-mid-claim.

### 4.5 `optimizer.py` — the GPUspb client *(481 LOC)*

**Purpose.** Translate IJM's DB state into a GPUspb `/optimizer/v5` request and
translate the response back into `Assignment`/`preempt` decisions.

**Inside.** `optimize()` orchestrates: build the `jobs` payload (submission time,
deadline, priority, epochs, and **per-epoch** `ProfilingData`), build the `nodes`
payload (virtual `node_id_gputype` entries for mixed-GPU nodes, `free_nGPUs =
total − assigned`), build `currentScheduling` (QUEUED/RUNNING/PROFILING — see
§3.4), POST, then map optimizer node IDs back to `(real_node, gpu_type)` and
classify each instance as new / kept-same / migrate / drop-preempt. Helpers:
`_format_time` (locale-independent RFC-2822-ish), `_parse_progress` (`"5/20"` →
5), `_build_nodes_payload`, `_build_node_map`, `_opt_node_for_real`.

**Data flow.** Called once per scheduler pass from `_schedule_waiting_jobs`.
Reads `jobs` + `profiling_results`; returns an `OptimizerResult` consumed by
`app`.

**Challenges & hacks.** The profile gate (skip instances that owe a profile), the
per-epoch (not pre-multiplied) duration fix, the QUEUED-in-`currentScheduling`
fix (§3.4), and the capacity-accounting fix (subtract *all* assigned, add back
only standard) are all here. Mixed-GPU virtual node IDs require the reverse map.

### 4.6 Supporting files

| File | LOC | Purpose / Inside |
|---|---|---|
| [`state.py`](backend/src/state.py) | 59 | Global mutable state: the `pool`, `job_runner`, `node_slots`, `dispatch_tasks` (so the reaper can cancel rather than race), `pending_migrates`, and `schedule_lock`. `get_conn()` context manager and `require_runner()` (503 if unset). One source of truth for cross-task coordination without DB polling. |
| [`models.py`](backend/src/models.py) | 139 | Pydantic schemas: `JobCreate`, `Job` (hydrated from `dict_row`, with a pre-validator that drops SQL NULLs so defaults apply), `NodeConfig`, `NodeStatus`, `ScheduleResult`. |
| [`cluster.py`](backend/src/cluster.py) | 68 | `ClusterManager`: loads `nodes_config.json` and `gpu_energy_costs.json` (env-overridable), exposes `get_energy_cost(type, n)`. Read at startup by scheduler/optimizer/routers. |
| [`constants.py`](backend/src/constants.py) | 32 | `STOPPABLE_STATUSES`, `RESUMABLE_STATUSES`, priority bounds, `DEFAULT_PROFILING_CONFIGS_PER_JOB`, CORS origins (empty-filtered to avoid a `[""]` wildcard bug). |
| [`main.py`](backend/src/main.py) | 22 | Entry point: re-exports `app`, bootstraps logging (so scheduler logs actually emit at INFO). |
| [`utils/gpu.py`](backend/src/utils/gpu.py) | 8 | `config_key()` — canonical sorted-JSON string for a GPU config, used for dedup/comparison. |
| [`__init__.py`](backend/src/__init__.py), `utils/__init__.py` | 3 / 7 | Package markers / re-exports. |

### 4.7 Routers — `backend/src/routers/`

| File | LOC | Endpoints |
|---|---|---|
| [`jobs.py`](backend/src/routers/jobs.py) | 415 | `POST /jobs` (create → `schedule_job` → dispatch), `GET /jobs[/{id}]`, `POST /jobs/{id}/stop` (SELECT FOR UPDATE → atomically pre-flip to PREEMPTED → async `/stop?reason=user`), `POST /jobs/{id}/resume`, `DELETE /jobs[/{id}]` (stop + delete unmeasured claims + drift-recover), `GET /jobs/{id}/logs` (proxy with assigned-node → fan-out → local FS fallback, UUID + path-traversal guards). |
| [`nodes.py`](backend/src/routers/nodes.py) | 66 | `GET /nodes`, `GET /gpu-costs`, `GET /configurations`. |
| [`admin.py`](backend/src/routers/admin.py) | 62 | `GET /admin/slots` (per-node total/available/used_mem/used_db/drift + lifetime metrics), `POST /admin/reconcile-slots`, `GET /admin/dispatch-tasks` (in-flight task introspection). |
| [`health.py`](backend/src/routers/health.py) | 44 | `GET /`, `GET /health` (DB ping + runner status, 503 if degraded). |
| [`profiling.py`](backend/src/routers/profiling.py) | 24 | `GET /profiling-results/{job_id}` (ordered fastest-first). |
| [`__init__.py`](backend/src/routers/__init__.py) | 16 | Aggregates all routers into one. |

**Notable hack (→ §11):** `POST /jobs/{id}/stop` **atomically pre-flips** the row
to PREEMPTED *before* calling the worker. This is deliberate, not a band-aid — it
removes the row from the optimizer's preempt-target set immediately and means the
worker must detect the pre-flipped state (`pre_flipped_user_stop`) and skip its
own redundant UPDATE. It's the API and worker cooperating across an async gap.

---

## 5. Worker — `worker/`

Pure-Python asyncio HTTP server, one per GPU node, driving the Docker CLI.
**1,526 LOC across 7 modules.** Must stay importable on Python 3.7 (legacy
image). It owns no
scheduling logic — it executes, measures, and reports state back to Postgres.

### 5.1 `execution.py` — the four-phase job lifecycle *(572 LOC)*

**Purpose.** Run a job in a container, stream its output, measure profiling
epochs, and handle every completion/failure/stop/migration outcome with correct
slot accounting.

**Inside — `_run_job(job_id)` in four phases:**
- **Phase 1 — atomic claim.** A single `UPDATE ... WHERE status = ANY(runnable)
  AND assigned_node = NODE_ID RETURNING id`. The `assigned_node` check is
  load-bearing: the API reaper can clear `assigned_node` for a stuck row while
  this claim is in flight; without the check the worker would commit RUNNING on a
  slot the reaper already released — **the literal 5-on-4-GPUs bug** (§3.1).
  Immediately after the claim it sets `running_jobs[job_id] = None` — a
  placeholder that makes the dispatch **indivisible** from `/stop`'s perspective.
- **Phase 2 — prepare & launch.** Resolve checkpoint/runs dirs, `chmod 0777`,
  claim distinct GPU indices (`_claim_gpu_indices`), apply `IMAGE_TAG_OVERRIDE`
  (matemagician `:latest`→`:legacy`), build the docker command, write a per-run
  log header, `Popen` the container, set `running_jobs[job_id] = process` and
  `dispatch_ready.set()` (unblocking `/stop`).
- **Phase 3 — stream & wait.** `_stream_output` reads stdout line-by-line, writes
  the log, parses `Epoch N/M`, and records monotonic timestamps for profiling.
  Progress writes are **coalesced** (a background writer flushes ≤1×/s) because
  per-line writes over the SSH-tunnelled DB took ~5 s each and made the reader
  fall behind. Writes are guarded by `status IN (RUNNING,PROFILING) AND
  container_name = ours` so a migrated row stops streaming.
- **Phase 4 — completion.** Re-reads the row. If `assigned_node` changed →
  **migrated**, don't touch it (the new owner is authoritative). If status is no
  longer RUNNING/PROFILING → record exit code only (a `/stop` already owns the
  transition + `NOTIFY`). On non-zero exit *while a `/stop` is in flight*
  (`job_id in stopping`) → record exit code only (avoid a FAILED flash in the
  UI). Otherwise mark SUCCEEDED/FAILED, delete unmeasured claims, and emit
  `ijm_slot_freed`. Profiling success defers to `handle_complete`.
- **Exception path** — a pre-Phase-4 failure marks FAILED and **still emits
  `ijm_slot_freed`** (else the API permit leaks forever — §3.1 cause #3).
- **`finally`** — pop `running_jobs`, release GPU indices, `dispatch_ready.set()`
  on every path, clean up task maps.

**Inside — module state.** `running_jobs` (Popen or `None` placeholder),
`_claimed_gpus` + `_gpu_claim_lock`, `dispatch_ready` (Events), `running_tasks`,
`stopping` (reason flag read by Phase 4), `_job_tasks` (for graceful shutdown).

**Challenges & hacks.** This file embodies the worker side of §3.1, §3.2, and the
stop/dispatch race. The GPU-index fallback to "lowest indices + warn" when
accounting is wrong (→ §11) is a **deliberate** choice: never block a launch, but
make the upstream bug operator-visible. The "never wipe checkpoints on a real
run" rule prevents losing a live checkpoint when two dispatches race.

### 5.2 `app.py` — worker HTTP server *(327 LOC)*

**Purpose.** Expose `/run`, `/stop`, `/logs`, `/health`; run startup
reconciliation; drain on shutdown.

**Inside.** `lifespan` runs `reconcile_job_states()` + `pickup_queued_jobs()` on
startup and `await`s all running tasks on shutdown. `run_job` rejects with 409 if
a task is still running or draining (prevents duplicate `_run_job`). `stop_job`
sets the `stopping` flag, then `_stop_job_impl` handles: a **stale-stop guard**
(row points at a different node → skip DB write, still kill any local stray); the
`_persist_stop` inner function (reads prev status, handles `pre_flipped_user_
stop`, emits `ijm_slot_freed` with `USER_STOP`/`AUTO_PREEMPT` reason, **commits
the status flip and the NOTIFY atomically**); a wait on `dispatch_ready` (≤10 s)
so a `/stop` mid-launch doesn't no-op against a not-yet-existent container; a kill
**before** persist (so a silent kill failure can't leave a live container under a
QUEUED row); and `_zombie_release` as the last resort if `kill_container` keeps
failing. `get_job_logs` returns the log with an `X-Log-Mtime` header so the API's
fan-out can pick the freshest copy across a migration.

**Challenges & hacks.** The atomic status-flip + NOTIFY is the contract that
keeps the API's release in lockstep with the actual kill. The 60-s run-task drain
timeout and the 10-s `dispatch_ready` wait are defensive bounds (→ §11).

### 5.3 Supporting files

| File | LOC | Purpose / Inside |
|---|---|---|
| [`docker.py`](worker/docker.py) | 266 | Docker CLI wrapper. `total_gpus()` (env → `nvidia-smi` → 0), `_gpu_flags()` (runtime/cdi/none modes; uses `NVIDIA_VISIBLE_DEVICES` to pin concrete indices, §3.9), `_is_rootless_docker()` + `_host_data_owner()` + `build_run_cmd()` (rootless vs. rootful `--user`, DNS pinning, checkpoint/runs/shared-dataset mounts), `kill_container()` (kill + `rm -f` + poll-to-convergence), `remove_container_if_exists()`. |
| [`reconcile.py`](worker/reconcile.py) | 102 | Startup recovery. `reconcile_job_states()` marks RUNNING/PROFILING rows whose container is gone as FAILED + deletes unmeasured claims + emits `ijm_slot_freed` (a **legacy 2-field** payload → defaults to TERMINAL). `pickup_queued_jobs()` **clears `assigned_node`** and notifies the API (the §3.1 phantom-release fix). |
| [`profiling.py`](worker/profiling.py) | 109 | `compute_duration()` (mean of post-warmup inter-epoch intervals; excludes the warmup epoch) and `handle_complete()` (writes `duration_seconds` filtered by `instance_id`, decides whether the instance `still_owes` more profiles, resets to QUEUED, NOTIFYs — all in one transaction). |
| [`db.py`](worker/db.py) | 107 | `connect()` / `conn()` short-lived connections; `update_job()` and `fetch_job()` with **column whitelists** so the worker can't accidentally write read-only fields. Caller owns the transaction for atomicity. |
| [`constants.py`](worker/constants.py) | 43 | `NODE_ID`, `WORKER_PORT`, `RUNNABLE_STATUSES = {QUEUED}`, container-name convention (`ijm-` + first 8 chars), mount paths. |
| [`Dockerfile`](worker/Dockerfile) / [`Dockerfile.server`](worker/Dockerfile.server) | — | Worker container images. |
| `tests/__init__.py` | 0 | **Empty — a known coverage gap.** The worker is exercised only by `infra/e2e_scenario*.sh`; per CLAUDE.md, adding worker logic without an e2e to back it is a recognized risk. |

---

## 6. Shared — `shared/`

The contract layer imported by **both** backend and worker. Only 76 LOC, but it
pins the cross-process protocol.

| File | LOC | Purpose / Inside |
|---|---|---|
| [`constants.py`](shared/constants.py) | 52 | `JobStatus` (StrEnum: QUEUED/PROFILING/RUNNING/SUCCEEDED/FAILED/PREEMPTED); `SlotFreedReason` (TERMINAL / USER_STOP / AUTO_PREEMPT / ORPHAN_DRAIN — the §3.3 churn-gate); the two NOTIFY channel names (`ijm_schedule`, `ijm_slot_freed`) and the documented `"<node>:<n>:<reason>"` payload format; `DEFAULT_PROFILING_EPOCHS = 3`; `RUNS_DIR` / `OUTPUT_LOG_FILENAME`. The module docstring on `PG_NOTIFY_SLOT_FREED` is itself the spec for the wake-gate behavior and the legacy-payload compatibility. |
| [`profiling_sql.py`](shared/profiling_sql.py) | 24 | `delete_unmeasured_claims(cur, instance_id)` — deletes only `duration_seconds IS NULL` rows for an instance (measured rows are preserved as the type's cost cache). Used by the API (`DELETE /jobs`) and worker (Phase 4, reconcile). Operates on a cursor so the caller controls the transaction. |
| `__init__.py` | 0 | Package marker. |

**Why a shared module exists.** The backend and worker are separate processes
(often on separate machines) that never call each other directly. `shared/`
guarantees they agree on status strings, channel names, payload formats, and the
profile-claim cleanup semantics — drift here would mean silent protocol
mismatches. This is "one source of truth per fact" applied to the wire protocol.

---

## 7. Runtime — `runtime/`

The training container images and scripts. They subclass a common
`BaseTrainer`, support checkpoint resume, and are built from a single Dockerfile
parameterized by a `SCRIPT` build-arg.

### 7.1 `base.py` — the trainer base class *(417 LOC)* — the cross-version centerpiece

**Purpose.** Provide checkpoint save/load, dataset loading, the training loop,
and evaluation, so concrete scripts only define the model/dataset/preprocessing.

**Inside.** The fd-2 cuDNN-noise filter (§3.8); `download_dataset()` (prefer
on-disk, fall back to download — §3.9); `BaseTrainer` (device detection,
DataParallel wrap for multi-GPU, Adam + CrossEntropy, env-driven epochs/batch,
test-batch capped at `min(256, batch)` to avoid OOM on 4 GB P600);
`load_checkpoint()` / `save_checkpoint()` (the §3.7 cross-version machinery); the
`train()` loop that logs `Epoch N/M` (the line the worker parses for progress and
profiling); and the `MNISTTrainer` / `MNISTSequenceTrainer` / `CIFAR10Trainer`
dataset mixins.

**Challenges & hacks.** This is §3.7 and §3.8 in full. The loader is deliberately
*loose* (`strict=False`, optimizer try/except, format/kwarg fallbacks) — CLAUDE.md
explicitly forbids tightening it because `cnn_big` exercises the
A40↔P600 / torch-2.6↔1.5.1 migration path.

### 7.2 Model scripts & images

| File | Purpose |
|---|---|
| [`cnn_big.py`](runtime/cnn_big.py) | Deep, wide CNN on synthetic 128×128×3 tensors. **The placement-choice / 2-GPU benchmark** — deliberately sized so per-step compute amortizes DataParallel sync overhead (P600×2 is 1.68× P600×1; A40×2 is 1.12× A40×1). Used by `e2e_scenario_2gpu.sh` (the 2-GPU standard-placement proof) and `e2e_scenario_2types.sh` (pins + urgent job). Don't change its shape without re-measuring (CLAUDE.md). |
| [`convnet.py`](runtime/convnet.py) | Lightweight 3-layer CNN on CIFAR-10 (no multi-GPU win). |
| [`efficientnet.py`](runtime/efficientnet.py) | MBConv EfficientNet on CIFAR-10 (SiLU unsupported on legacy torch). |
| [`lstm_big.py`](runtime/lstm_big.py) / [`lstm_small.py`](runtime/lstm_small.py) | 3-layer / 1-layer LSTM on MNIST sequences; `flatten_parameters()` to quiet the DataParallel RNN warning. |
| [`sleepy.py`](runtime/sleepy.py) | Sleep-based trivial job — exercises the full pipeline (profile/stop/resume/checkpoint) without GPU load. |
| [`Dockerfile`](runtime/Dockerfile) | Modern image: Python 3.13, torch 2.6, CUDA 12.4. `SCRIPT` build-arg picks the trainer. |
| [`Dockerfile.legacy`](runtime/Dockerfile.legacy) | Legacy image: torch 1.5.1, CUDA 10.1 — matemagician's `IMAGE_TAG_OVERRIDE=legacy` target. |
| [`compat/`](runtime/compat/) | A self-contained torch-1.7.1 LSTM trainer + Dockerfile (no `base.py` dependency) for intermediate-version compat testing. |
| [`README.md`](runtime/README.md) | The model table (2-GPU-win status), build commands, and the cross-version-robustness notes. |

---

## 8. Tests — `backend/tests/`

A real pytest suite (8 files) framed as **regression guards** — most map directly
to a bug in §3.

| File | Guards |
|---|---|
| [`test_node_slots.py`](backend/tests/test_node_slots.py) | Semaphore construction, acquire/release, **multi-GPU atomic acquire** (the deadlock fix), reconcile, drift detection. The §3.1/§3.2 spec. |
| [`test_job_lifecycle.py`](backend/tests/test_job_lifecycle.py) | The full state machine: stop/resume transitions, invalid-transition 409s, rapid stop/resume cycling, the runner contract — via `FakeConn`/`FakeRunner`. |
| [`test_main.py`](backend/tests/test_main.py) | Root/health (503 when degraded), job CRUD, deadline UTC normalization, admin endpoints. |
| [`test_pending_migrates.py`](backend/tests/test_pending_migrates.py) | The migrate-apply `WHERE` clause (`status=QUEUED AND assigned_node IS NULL`); **greps the source** to fail if the status guard is refactored away. The §3.5 spec. |
| [`test_slot_payload.py`](backend/tests/test_slot_payload.py) | Legacy 2-field vs. new 3-field payload parsing, and the wake-gate (AUTO_PREEMPT does **not** wake). The §3.3 spec. |
| [`test_sql_integration.py`](backend/tests/test_sql_integration.py) | Real-Postgres JSONB equality (`= %s::jsonb`); skips gracefully if no DB. |
| [`test_infra.py`](backend/tests/test_infra.py) | Compose validation: Postgres volume mount at `/var/lib/postgresql` (not `.../data`), consistent Python base images. |

---

## 9. Infra & deployment — `infra/`

| File | What it does |
|---|---|
| `docker-compose.yml` / `.server.yml` / `.tunnel.yml` | Compose topologies: full local stack, the remote worker+Postgres "server" half deployed per GPU node, and the SSH-tunnel wiring for distributed runs. |
| `e2e_scenario.sh` | **Scenario 1** (~15 min, `cnn_big`): multi-GPU profiling → horizon-myopic placement on the free cheaper P600 (no preempt) → natural-finish migration P600→A40 (exercising cross-version checkpoint resume) → slot-tracker invariants. Assertions are plain `curl` calls — reproducible by hand. |
| `e2e_scenario_2types.sh` | **Scenario 2** (`cnn_big` + `lstm-small`): prio-5 `cnn_big` pins on A40, prio-1 `lstm-small` patients on P600; one urgent `cnn_big` drop-preempts a *single* patient onto P600×1, then migrates P600→A40 when a pin frees while the evicted patient resumes. |
| `e2e_scenario_unprofiled.sh` | Profile-always path for jobs with no pre-seeded profiling data. |
| `e2e_scenario_2gpu.sh` | 2-GPU DataParallel bundle placement. |
| `smoke_test.sh` | Bring up the stack, wait for API + worker health. |
| `deploy.sh` / `deploy_native.sh` / `tunnel.sh` | Remote deploy (SSH ControlMaster multiplexing; rootless variant avoids a `pkill` self-match SSH-255 bug) and tunnel setup. |
| `generate_chart.py` (494) / `generate_chart_tex.py` (404) | Parse `api.log` into a matplotlib PNG / a deterministic TikZ Gantt chart for the thesis — every bar position comes from a real log event, not hand-tuning. |
| `follow_optimizer.sh` / `snapshot_run.sh` / `ijm` | Tail optimizer decisions live; snapshot a run's logs/json; the CLI entry point. |

---

## 10. Design docs — `documentation/`

- [`SLOT_INVARIANTS.md`](documentation/SLOT_INVARIANTS.md) — **the authoritative
  slot-correctness spec**: the 3 hard invariants, all acquire/release sources,
  the forbidden patterns, the operator handles, and a "How we got here" list of
  the 7 bugs that shaped it. *Note: it says the drift watcher runs "every 5 min",
  but the code default is 15 s (`IJM_DRIFT_HEARTBEAT_S`); the code is
  authoritative.*
- [`e2e-scenarios.md`](documentation/e2e-scenarios.md) — what each scenario
  exercises and the cost-model rationale (priority × tardiness, deadline slack,
  why a high-priority job lands on the *cheaper* node when it has slack).
- [`andreas.md`](documentation/andreas.md) — mapping IJM's API contracts to the
  upstream ANDREAS deliverables.
- `current_prototype_status.puml` / `architecture_diagram.puml` — architecture
  diagrams. `openapi.json` — the generated API schema. `external/` — reference
  PDFs (ANDREAS, GPUspb). `report/` — the LaTeX thesis.

---

## 11. Consolidated hacks & band-aids

Per CLAUDE.md's "no band-aids" principle, here is every workaround in one place,
honestly classified. **Most are deliberate trade-offs** (a real distributed
system needs reconciliation); a few are genuine band-aids compensating for state
that isn't fully persistent.

| # | Location | What it compensates for | Classification & root-fix note |
|---|---|---|---|
| 1 | `app._drift_watcher` (15 s) + `_slot_listener` reconnect | Lost `ijm_slot_freed` events when an SSH tunnel drops | **Justified safety net.** DB is the real source of truth; this reconciles to it. Look here first if a release path is found missing a `NOTIFY`. |
| 2 | `app._reset_stuck_queued_assignments` (30 s reaper) | QUEUED-with-assignment rows orphaned by an API crash post-dispatch | **Band-aid-ish.** Root cause is that dispatch tasks live only in process memory (`state.dispatch_tasks`) and don't survive a restart. A fully persistent task ledger would remove the need; judged not worth the cost. |
| 3 | `node_slots` `BoundedSemaphore` + `SLOT-OVERSUB`/`OVERRELEASE` logs | A latent acquire/release imbalance | **Structural guard**, not a band-aid — caps capacity so a bug can't silently oversubscribe; the ERROR log surfaces it. |
| 4 | `execution._claim_gpu_indices` fallback to lowest indices | A slot-accounting bug delivering more GPUs than free | **Deliberate.** Never block a launch; the WARNING makes the upstream bug operator-visible. |
| 5 | `JobDispatcher._await_inflight_stops_on` (10 s) and `app.py` `/stop` `dispatch_ready` wait (10 s), run-task drain (60 s) | A hung worker | **Defensive bounds.** If exceeded, the drift heartbeat (#1) repairs slots anyway. |
| 6 | `app._preempt_and_release` "refuse to preempt a profiling run" | An upstream caller bug that would evict measurement work | **Belt-and-suspenders.** Logs an ERROR — every automated path should already filter `is_profiling_run`. |
| 7 | `_run_job` exception path emits `ijm_slot_freed` | Pre-Phase-4 failures leaking the API permit forever | **Correct fix** for §3.1 cause #3 — not a band-aid. |
| 8 | `execution.py` "never wipe checkpoints on a real run" | Two racing dispatches nuking a live checkpoint | **Correct fix** — only the isolated `.profiling/` subdir is wiped. |
| 9 | `_stream_output` 1 s progress coalescing | ~5 s/line DB writes over the SSH tunnel starving the reader | **Correct fix** — batches writes so `/stop` drains don't hit the 60 s timeout. |
| 10 | `base.py` fd-2 cuDNN noise filter, `strict=False`, optimizer try/except, format/kwarg fallbacks | Legacy torch 1.5.1 / Python 3.7 / FUSE mounts | **Necessary compatibility shims** for the heterogeneous cluster (§3.7–§3.8). CLAUDE.md forbids tightening them. |
| 11 | `reconcile_job_states` emits a **legacy 2-field** payload (→ TERMINAL) while `_zombie_release` uses `ORPHAN_DRAIN` | — | **Minor inconsistency to be aware of.** Both wake the optimizer (TERMINAL and ORPHAN_DRAIN are both external reasons), so behavior is correct, but the reason tag on the reconcile path is less precise than it could be. |

---

## 12. Appendix: full file inventory

### Backend — `backend/src/` (3,433 LOC)

| File | LOC | One-line role |
|---|---|---|
| `app.py` | 752 | App factory, lifespan, scheduler engine + 5 background tasks |
| `profiling.py` | 588 | ProfilingScheduler — profile-always policy |
| `optimizer.py` | 481 | GPUspb optimizer client (request build + response classify) |
| `routers/jobs.py` | 415 | Job CRUD / stop / resume / delete / logs |
| `node_slots.py` | 362 | Per-node GPU semaphores + drift reconciliation |
| `job_dispatcher.py` | 285 | The single dispatch path (acquire → /run) |
| `models.py` | 139 | Pydantic schemas |
| `cluster.py` | 68 | Cluster + GPU-cost config loader |
| `routers/nodes.py` | 66 | Node / GPU-cost / configuration endpoints |
| `routers/admin.py` | 62 | Slot introspection + manual reconcile |
| `state.py` | 59 | Global mutable state + connection helper |
| `routers/health.py` | 44 | Health checks |
| `constants.py` | 32 | Status sets, priority bounds, profiling config |
| `routers/profiling.py` | 24 | Profiling-results query |
| `main.py` | 22 | Entry point + logging bootstrap |
| `routers/__init__.py` | 16 | Router aggregation |
| `utils/gpu.py` | 8 | `config_key()` canonicalization |
| `utils/__init__.py` | 7 | Re-export |
| `__init__.py` | 3 | Package marker |

### Worker — `worker/` (1,526 LOC across 7 modules)

| File | LOC | One-line role |
|---|---|---|
| `execution.py` | 572 | Four-phase job lifecycle, GPU claim, progress streaming |
| `app.py` | 327 | HTTP server: /run /stop /logs /health, startup reconcile |
| `docker.py` | 266 | Docker CLI wrapper (GPU pinning, rootless/rootful, kill) |
| `profiling.py` | 109 | Profiling completion (duration → DB → re-queue) |
| `db.py` | 107 | Whitelisted DB access |
| `reconcile.py` | 102 | Startup recovery (reconcile + pickup) |
| `constants.py` | 43 | Node config + container-name helpers |
| `tests/__init__.py` | 0 | **Empty — known coverage gap** |
| `Dockerfile`, `Dockerfile.server`, `requirements.txt`, `pyproject.toml`, `pytest.ini` | — | Packaging / tooling |

### Shared — `shared/` (76 LOC)

| File | LOC | One-line role |
|---|---|---|
| `constants.py` | 52 | JobStatus, SlotFreedReason, NOTIFY channels + payload format |
| `profiling_sql.py` | 24 | `delete_unmeasured_claims` (shared by API + worker) |
| `__init__.py` | 0 | Package marker |

### Runtime — `runtime/`

| File | One-line role |
|---|---|
| `base.py` (417) | Trainer base class — checkpoint cross-version resilience (centerpiece) |
| `cnn_big.py` | Synthetic deep CNN — the 2-GPU / placement-choice benchmark |
| `convnet.py`, `efficientnet.py`, `lstm_big.py`, `lstm_small.py`, `sleepy.py` | Concrete trainers |
| `Dockerfile`, `Dockerfile.legacy` | Modern (torch 2.6) / legacy (torch 1.5.1) images |
| `compat/Dockerfile`, `compat/train_lstm.py` | Intermediate torch-1.7.1 compat trainer |
| `requirements.txt`, `README.md` | Deps / model table + build docs |

### Tests — `backend/tests/` (8 files)

`test_job_lifecycle.py`, `test_main.py`, `test_node_slots.py`,
`test_pending_migrates.py`, `test_slot_payload.py`, `test_sql_integration.py`,
`test_infra.py`, `__init__.py`.

### Infra — `infra/` (18 files)

4 compose files, 4 `e2e_scenario*.sh`, `smoke_test.sh`, `deploy.sh`,
`deploy_native.sh`, `tunnel.sh`, `follow_optimizer.sh`, `snapshot_run.sh`,
`generate_chart.py`, `generate_chart_tex.py`, `ijm`.

### Documentation — `documentation/`

`SLOT_INVARIANTS.md`, `e2e-scenarios.md`, `andreas.md`,
`current_prototype_status.puml`, `architecture_diagram.puml`, `openapi.json`,
`external/`, `report/`.

---

*Generated as a point-in-time engineering report. The authoritative source for
slot correctness remains [SLOT_INVARIANTS.md](documentation/SLOT_INVARIANTS.md);
where this report and that doc differ on the drift-watcher interval, the code
(`IJM_DRIFT_HEARTBEAT_S`, default 15 s) wins.*

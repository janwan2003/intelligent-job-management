# IJM End-to-End Scenarios

Scripted exercises of the full distributed cluster. Each script drives the
system through the **public HTTP API only** (`POST /jobs`,
`POST /admin/reconcile-slots`, `GET /jobs`, …) — no out-of-band DB writes or
preseeds — so every scenario is reproducible by hand or from the UI.

**The scripts are the source of truth for the exact commands and
assertions.** This document explains *what each script exercises and why*;
when in doubt read the script. Anything below that contradicts the script is
a bug in this document.

| Script | Role | Proves |
|---|---|---|
| [`infra/e2e_scenario.sh`](../infra/e2e_scenario.sh) | Scenario 1 — single type, under-subscribed | Profile sweep (incl. 2-GPU coverage) · horizon-myopic placement (urgent takes a *free* slow bundle, **no preempt**) · natural-finish cross-node, cross-version migration · coherent slot accounting |
| [`infra/e2e_scenario_2types.sh`](../infra/e2e_scenario_2types.sh) | Scenario 2 — two types, full cluster | Deadline-driven **preemption** (single evict) · migrate-to-meet-deadline · evicted job resumes · clean terminal state |
| [`infra/e2e_scenario_2gpu.sh`](../infra/e2e_scenario_2gpu.sh) | Scenario 2b — forced 2-GPU standard placement (supporting) | Isolated proof that the optimiser picks P600×2 for a standard run when 1-GPU would be tardy |
| [`infra/e2e_scenario_unprofiled.sh`](../infra/e2e_scenario_unprofiled.sh) | Scenario 3 — preempt-for-profile (supporting) | Profile-always policy: a brand-new (unprofiled) type evicts a running job to take a profiling slot |

The thesis chapter ([`documentation/report/Files/e2e.tex`](report/Files/e2e.tex),
`sec:e2e-s1` / `sec:e2e-s2`) walks through Scenario 1 and Scenario 2 with
figures from the `2026-06-25` run snapshots. Scenarios 2b and 3 are not part
of the thesis walkthrough.

## Running

```bash
./infra/e2e_scenario.sh              # Scenario 1 (~15 min), default JOB_TYPE=cnn_big
./infra/e2e_scenario.sh --strict     # soft checks (checkpoint-resume log line, A40 held, <4 SUCCEEDED, zombies, underflow>0) become fatal
./infra/e2e_scenario_2types.sh       # Scenario 2 (~20 min)
JOB_TYPE=lstm-small ./infra/e2e_scenario.sh   # faster sanity run (MNIST, no 2-GPU win)
```

All scripts expect the API at `$API` (default `http://localhost:8000`), the
SSH tunnels to both workers up, and the
`wangrat/ijm-{cnn_big,lstm-small}:latest` images present on the GPU nodes
(Scenario 3 additionally needs `wangrat/ijm-convnet:latest`). The `wangrat`
namespace is the `IMAGE_NS` default. Worker/node wiring is read at runtime
from [`config/nodes_config.tunnel.json`](../config/nodes_config.tunnel.json).

## Cluster topology assumed

| Node (SSH alias) | GPUs | torch / CUDA | Slots |
|---|---|---|---|
| `matemagician` (`polimi`) | 2× QuadroP600 | 1.5.1 / 10.1 (legacy image) | 2 |
| `polimi-gpu` | 2× A40 | 2.6 / 12.4 | 2 |

4 GPU slots total; both nodes are profiling-capable. Checkpoints are written
in the pre-1.6 `torch.save` format and loaded with `strict=False`, so a job
started on one node **resumes on the other regardless of torch version** —
the P600↔A40 hop is the cross-version resume test. `PROFILING_CONFIGS_PER_JOB=2`,
so two instances of a type cover all four `(node, GPU-count)` profile cells.

---

# Scenario 1 — single type, under-subscribed

**`cnn_big` only. 5 jobs: 4 patient + 1 urgent. No preemption.** The cluster
is *under-subscribed* when the urgent job arrives (both P600 slots free), so
the urgent job takes free capacity rather than evicting anyone. This is the
scenario that exercises the GPUspb Random-Greedy placement proxy's
**horizon-myopic** behaviour, and cross-node migration on natural finish.

> **Why `cnn_big`.** Its per-epoch compute is heavy enough to amortise the
> DataParallel sync overhead on *both* GPU classes, so its 2-GPU bundle is
> observably faster than 1-GPU on both A40 (1.12×) and P600 (1.68×) — it is
> the only type whose 2-GPU placement is worth choosing. It also uses purely
> synthetic tensors, so it has no dataset-staging dependency. Override with
> `JOB_TYPE=lstm-small` for a faster run (but lstm-small has no 2-GPU win).

### Jobs submitted (Stage 1 + Stage 2)

All five jobs are the same type (default `cnn_big`, image
`wangrat/ijm-cnn_big:latest`; `JOB_TYPE`/`IMAGE_NS` override both).
Deadlines are relative to each job's own submission time. The *Placement*
column is what the 2026-06-25 reference run (the thesis figures) actually
did — Stage 1 asserts only the sweep completing and ≥2 patients RUNNING,
not the exact split.

| Job | Image | Submitted | Priority | Deadline | Epochs | Placement (2026-06-25 run) |
|---|---|---|---|---|---|---|
| JOB1 | `wangrat/ijm-cnn_big:latest` | t+0 s | 4 | +30 min | 75 | carries the P600 profile sweep, then standard on **A40×1** |
| JOB2 | `wangrat/ijm-cnn_big:latest` | t+5 s | 4 | +30 min | 75 | carries the A40 profile sweep, then standard on **A40×1** |
| JOB3 | `wangrat/ijm-cnn_big:latest` | t+10 s | 2 | +2 h | 50 | standard on **A40×1**; finishes naturally at t≈6.2 min |
| JOB4 | `wangrat/ijm-cnn_big:latest` | t+15 s | 1 | +2 h | 25 | standard on **A40×1**; finishes naturally at t≈3.9 min |
| **URGENT** | `wangrat/ijm-cnn_big:latest` | Stage 2 — after Stage-1 steady state + 60 s settle (t≈7.7 min in the reference run) | 5 | +10 min | 200 | **free QuadroP600×2** (whole `matemagician` idle, J3/J4 already done), then migrates **P600×2 → A40×1 → A40×2** |

The patients' priorities and deadlines are deliberately staggered to break
placement symmetry: with 4 identical (prio, deadline) jobs the cost surface
across the "2 on P600, 2 on A40" permutations is flat and the optimiser
flips assignments round-to-round. The short slack jobs (J4: 25 ep, J3:
50 ep at 5.08 s/epoch on A40×1) finish *before* URGENT is submitted, so at
URGENT's arrival the steady state is J1+J2 on A40×1 each and **both P600
slots free** — the under-subscribed setup Stage 2 needs.

### Stages

- **Stage 0 — sanity + clean slate.** Health-check API + both workers; clear
  the DB (`DELETE /jobs`), wipe on-disk checkpoints/runs on the nodes, and
  `POST /admin/reconcile-slots` to zero the slot tracker.
- **Stage 1 — profile sweep + fill.** Submit the 4 patients 5 s apart. Wait
  until **all 4 `(node, GPU-count)` cells are measured**, no job is still
  `PROFILING`, and ≥2 patients are `RUNNING`. Then assert **≥1 profiled row
  used a 2-GPU bundle** — this is the multi-GPU-during-sweep evidence
  (`(.gpu_config | first) == 2`).
- **Stage 2 — horizon-myopic placement.** After a 60 s settle, submit the
  urgent prio-5 job (deadline +10 min, 200 epochs — a deadline *no* bundle
  can actually meet; it finishes ~11 min late and still counts SUCCEEDED).
  Assert it lands on a **QuadroP600** bundle (the assert accepts ×1 or ×2;
  the reference run gives it P600×2 since the whole node is idle) and that
  **no incumbent was preempted** — a preempt here would be flagged a regression, because
  Random-Greedy only places on free capacity and its horizon-myopic proxy
  reads the urgent job's tardiness as zero. "A40 still held by ≥1 patient"
  is a soft check. See `Appendix~\ref{app:proxy}` in the thesis for the
  proxy math.
- **Stage 3 — removed.** Manual user-stop was intentionally dropped so every
  preempt is attributable to the optimiser. No sticky-`PREEMPTED` assertion
  remains.
- **Stage 4 — natural-finish migration.** As the A40 patients (J2 at
  t≈10.9 min, J1 at t≈13.5 min) finish on their own, the optimiser re-plans
  and **migrates the urgent job P600 → A40** (`polimi-gpu`), then upgrades
  A40×1 → A40×2 as the second slot frees. The migration itself is a **hard
  assert** (fatal if it doesn't happen within 12 min), and since Stage 2
  pinned the origin to P600 it is always the **cross-node, cross-version
  resume** (torch 1.5.1 → 2.6) — twice, in fact, in the reference run. The
  checkpoint actually loading is soft-checked via a
  `resumed from … / loaded checkpoint` log line, escalated to fatal under
  `--strict`.
- **Stage 5 — drain + invariants.** Wait for **all 5 jobs terminal**; assert
  **≥4 SUCCEEDED, 0 FAILED**, no zombie containers on either node, and slot
  tracker clean: `acquire_oversub_count == 0`, `release_underflow_count ≤ 5`,
  drift heartbeat fired (15 s cadence).

### What it proves
2-GPU profiling coverage · horizon-myopic placement (urgent on the *free
slow* bundle, zero preempts) · natural-finish cross-node migration with
cross-version checkpoint resume · coherent slot accounting.

---

# Scenario 2 — two types, full cluster

**`cnn_big` + `lstm-small`. 5 jobs. Deadline-driven preemption + migration.**
Unlike Scenario 1 the cluster is *full* when the urgent job arrives, so the
optimiser must **preempt** to place it: it drop-preempts a *single* prio-1
`lstm-small` and puts URGENT on P600×1 (A40 is held by equal-priority pins).
When a pin finishes on its own, URGENT **migrates** P600→A40 to meet its
deadline, and the evicted patient **resumes** on the freed P600 slot.

### Jobs submitted

Deadlines are relative to each job's own submission time.

| Job | Image | Submitted | Priority | Deadline | Epochs | Role |
|---|---|---|---|---|---|---|
| PIN1 | `wangrat/ijm-cnn_big:latest` | t+0 s | 5 | +10 min | 80 | pins an A40 slot |
| PIN2 | `wangrat/ijm-cnn_big:latest` | t+5 s | 5 | +10 min | 80 | pins the other A40 slot |
| PB1 | `wangrat/ijm-lstm-small:latest` | t+10 s | 1 | +8 h | 200 | fills a P600 slot (eviction target) |
| PB2 | `wangrat/ijm-lstm-small:latest` | t+15 s | 1 | +8 h | 200 | fills the other P600 slot (eviction target) |
| **URGENT** | `wangrat/ijm-cnn_big:latest` | Stage 2 — after Stage-1 steady state (4 active, ≥3 progressed; t≈7.7 min in the reference run) | 5 | +22 min | 40 | **drop-preempts one P600 patient → `matemagician` P600×1, then migrates to A40** |

The two prio-5 `cnn_big` pins hold A40 (equal priority to URGENT, so RG never
evicts them for it; and evicting one would re-dispatch it to a slow P600×1,
blowing its +10-min deadline). The two prio-1 `lstm-small` patients hold P600
and are the only preemptible occupants — the proxy drop-preempts **one** of
them (the cheapest preemptible bundle).

### Stages

- **Stage 0 — sanity + clean slate.** API health, `DELETE /jobs`, wipe node
  state, reconcile slots.
- **Stage 1 — pin then fill.** *1a:* two prio-5 tight-deadline `cnn_big` to
  pin A40. *1b:* two prio-1 loose-deadline `lstm-small` to fill P600. Wait
  for the cluster full (4 active, ≥3 RUNNING with progress) and the profile
  sweep complete. The settle is not instant: in the reference run the sweep
  interleaves both types across both nodes, an `lstm-small` briefly runs
  standard on A40×1 and gets drop-preempted by a prio-5 pin at t≈6.9 min,
  and the layout reaches "A40 = two pins, P600 = two lstm patients" at
  t≈7.6 min.
- **Stage 2 — single-evict preempt.** Submit the urgent `cnn_big` (prio 5,
  deadline +22 min). Assert it lands on **`matemagician` with QuadroP600×1**
  and that **exactly one** `lstm-small` patient was evicted — the other keeps
  its P600 slot alongside URGENT. (A40 stays pinned: RG won't evict the
  equal-priority `cnn_big` pins.)
- **Stage 3 — migrate + resume.** When a pin finishes on its own (reference
  run: PIN-B at t≈9.5 min), assert URGENT **migrates P600→A40**
  (`polimi-gpu`) — cross-node, torch 1.5.1→2.6 — and that the **evicted
  patient resumes** from checkpoint on the freed P600 slot. In the reference
  run URGENT then finishes at t≈13.3 min on A40×1, ~16 min *inside* its
  +22-min deadline.
- **Stage 4 — drain + invariants.** Wait for all 5 jobs terminal; assert
  **URGENT SUCCEEDED** (met its deadline on A40), **0 FAILED**, and the slot
  tracker clean (0 oversub, ≤5 underflow).

### What it proves
Deadline-driven preemption (single evict) · cross-node cross-version
**migration of the urgent job** to meet its deadline · **resume of the
evicted job** from checkpoint with no lost epochs · A40 stays pinned by the
equal-priority jobs · clean terminal (0 FAILED) with coherent slot accounting.

---

# Supporting scenarios

- **Scenario 2b — `e2e_scenario_2gpu.sh`.** A minimal, isolated proof of the
  2-GPU-standard claim: it **requires `cnn_big` and `lstm-small` already
  profiled** (asserts ≥4 cells each, else tells you to run Scenarios 1+2
  first), pins both A40 slots with two prio-5 `lstm-small`
  (`wangrat/ijm-lstm-small:latest`, deadline +45 min, 400 epochs, submitted
  4 s apart), then submits a prio-4 `cnn_big`
  (`wangrat/ijm-cnn_big:latest`, deadline +22 min, 40 epochs) and asserts it
  lands on `matemagician` P600×2. The deadline math forcing that choice
  (the script's figures): A40×1 = 6.3 s/epoch but both A40 slots are
  pinned; P600×1 = 51.1 s/epoch → 40 epochs ≈ 34 min, tardy; P600×2 =
  30.3 s/epoch → ≈ 20 min, fits +22 min. (These absolute s/epoch values
  predate the per-epoch profiler-measurement fix — the 2026-06-25 sweep
  measured 78.4 / 46.7 s/epoch — but the 1.68× P600×2 speedup, which drives
  the choice, is unchanged.) Stops early once placement is proven (does not
  drain — jobs are left running).
- **Scenario 3 — `e2e_scenario_unprofiled.sh`.** Proves the *profile-always*
  policy: a **never-profiled** type (`convnet`, its rows wiped in Stage 0;
  requires ≥2 profile rows each for `lstm-small` and `cnn_big`) is
  submitted onto a full cluster and must proactively evict the lowest-priority
  running job to take a profiling slot, then the victim resumes from
  checkpoint. The cluster is filled with 4 patients submitted 4 s apart:
  `lstm-small` prio 1 (+8 h, 400 ep), `cnn_big` prio 1 (+8 h, 40 ep),
  `lstm-small` prio 4 (+2 h, 400 ep), `cnn_big` prio 4 (+2 h, 40 ep) — the
  two prio-1 jobs are the eviction candidates. Then `convnet`
  (`wangrat/ijm-convnet:latest`, prio 3, +8 h, 5 epochs) is submitted;
  prio 3 is deliberately mid-tier because Phase 1c does not gate on the
  submitter's priority — profiling evicts the lowest-prio running job
  regardless. Asserts `convnet` enters `PROFILING` with `is_profiling_run =
  true` within 60 s, a victim exists (expected prio 1 — warn otherwise),
  the profile completes and is cached, the victim resumes, and all jobs
  terminate with 0 FAILED.

---

# Notes on variability

- **Patient placement** (Scenario 1 Stage 1): the staggered
  priorities/deadlines prevent the flat-cost-surface flapping of 4 identical
  jobs, but which instance carries which sweep and the order slots free in
  can still vary run-to-run — the S1 jobs table records the 2026-06-25
  reference run, and Stage 1 asserts only the sweep complete and ≥2
  patients RUNNING. Invariant: no slot left idle while a schedulable QUEUED
  job exists, and the profile sweep covers every cell including a 2-GPU
  bundle.
- **Cross-node migration** (Scenario 1 Stage 4) is a hard assert: Stage 2
  already pins URGENT's origin to a P600 bundle, so the P600→A40 hop (and
  with it the torch 1.5.1 → 2.6 crossing) must happen within 12 min or the
  run fails. What stays soft is confirming the checkpoint *load* in the
  trainer logs (`resumed from …`) — warn-only unless `--strict`.
- **DataParallel.** For tiny LSTMs 1-GPU still beats 2-GPU (the scatter/gather
  cost isn't amortised), so the profiler honestly records no 2-GPU win for
  `lstm-small`; only `cnn_big` shows a 2-GPU speedup. This is why Scenario 1's
  2-GPU coverage and Scenario 2/2b's 2-GPU *placement* both use `cnn_big`.

# What each scenario proves, by capability

| Capability | S1 | S2 | 2b | S3 |
|---|:--:|:--:|:--:|:--:|
| Profile-always sweep (one config per instance) | ✓ | ✓ | — | ✓ |
| Single- and dual-GPU placement | ✓ | ✓ | ✓ | — |
| Deadline-driven preemption | — | ✓ | ✓ | — |
| Preempt-for-profile (unprofiled type) | — | — | — | ✓ |
| Cross-node, cross-version checkpoint resume | ✓ | ✓ | — | ✓ |
| Coherent slot accounting (0 oversub / underflow) | ✓ | ✓ | — | — |
| Horizon-myopic placement (free bundle, no preempt) | ✓ | — | — | — |

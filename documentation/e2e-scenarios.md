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
| [`infra/e2e_scenario_2types.sh`](../infra/e2e_scenario_2types.sh) | Scenario 2 — two types, full cluster | Deadline-driven **preemption** · 2-GPU bundle chosen for a *standard* run · clean terminal state |
| [`infra/e2e_scenario_2gpu.sh`](../infra/e2e_scenario_2gpu.sh) | Scenario 2b — forced 2-GPU standard placement (supporting) | Isolated proof that the optimiser picks P600×2 for a standard run when 1-GPU would be tardy |
| [`infra/e2e_scenario_unprofiled.sh`](../infra/e2e_scenario_unprofiled.sh) | Scenario 3 — preempt-for-profile (supporting) | Profile-always policy: a brand-new (unprofiled) type evicts a running job to take a profiling slot |

The thesis chapter ([`documentation/report/Files/e2e.tex`](report/Files/e2e.tex),
`sec:e2e-s1` / `sec:e2e-s2`) walks through Scenario 1 and Scenario 2 with
figures from the `2026-06-25` run snapshots. Scenarios 2b and 3 are not part
of the thesis walkthrough.

## Running

```bash
./infra/e2e_scenario.sh              # Scenario 1 (~15 min), default JOB_TYPE=cnn_big
./infra/e2e_scenario.sh --strict     # a non-cross-node migration becomes fatal
./infra/e2e_scenario_2types.sh       # Scenario 2 (~20 min)
JOB_TYPE=lstm-small ./infra/e2e_scenario.sh   # faster sanity run (MNIST, no 2-GPU win)
```

Both expect the API at `$API` (default `http://localhost:8000`), the SSH
tunnels to both workers up, and the `wangrat/ijm-{cnn_big,lstm-small}:latest`
images present on the GPU nodes. Worker/node wiring is read at runtime from
[`config/nodes_config.tunnel.json`](../config/nodes_config.tunnel.json).

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

| Job | Priority | Deadline | Epochs | Expected steady-state |
|---|---|---|---|---|
| JOB1 | 4 | +30 min | 75 | P600 or A40 (tight pair pins the cheap node) |
| JOB2 | 4 | +30 min | 75 | P600 or A40 |
| JOB3 | 2 | +2 h | 50 | overflow to A40 |
| JOB4 | 1 | +2 h | 25 | overflow to A40 |
| **URGENT** | 5 | +10 min | 200 | **free QuadroP600 bundle, then migrates to A40** |

Exact placement of the 4 patients depends on solver tie-breaking; what is
asserted is the profile sweep completing and ≥2 patients RUNNING.

### Stages

- **Stage 0 — sanity + clean slate.** Health-check API + both workers; clear
  the DB (`DELETE /jobs`), wipe on-disk checkpoints/runs on the nodes, and
  `POST /admin/reconcile-slots` to zero the slot tracker.
- **Stage 1 — profile sweep + fill.** Submit the 4 patients 5 s apart. Wait
  until **all 4 `(node, GPU-count)` cells are measured**, no job is still
  `PROFILING`, and ≥2 patients are `RUNNING`. Then assert **≥1 profiled row
  used a 2-GPU bundle** — this is the multi-GPU-during-sweep evidence
  (`(.gpu_config | first) == 2`).
- **Stage 2 — horizon-myopic placement.** Submit the urgent prio-5 job
  (deadline +10 min, 200 epochs). Assert it lands on a **QuadroP600** bundle
  (the *slower, free* node) and that **no incumbent was preempted** — a
  preempt here would be flagged a regression, because Random-Greedy only
  places on free capacity and its horizon-myopic proxy reads the urgent job's
  tardiness as zero. See `Appendix~\ref{app:proxy}` in the thesis for the
  proxy math.
- **Stage 3 — removed.** Manual user-stop was intentionally dropped so every
  preempt is attributable to the optimiser. No sticky-`PREEMPTED` assertion
  remains.
- **Stage 4 — natural-finish migration.** As A40 patients finish on their
  own, the optimiser re-plans and **migrates the urgent job P600 → A40**
  (`polimi-gpu`). Assert the migration; if the origin was P600 this is the
  **cross-node, cross-version resume** (torch 1.5.1 → 2.6) — soft-checked via
  a `resumed from … / loaded checkpoint` log line, escalated to fatal under
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

| Job | Type | Priority | Deadline | Epochs | Role |
|---|---|---|---|---|---|
| PIN1 | `cnn_big` | 5 | +10 min | 80 | pins an A40 slot |
| PIN2 | `cnn_big` | 5 | +10 min | 80 | pins the other A40 slot |
| PB1 | `lstm-small` | 1 | +8 h | 200 | fills a P600 slot (eviction target) |
| PB2 | `lstm-small` | 1 | +8 h | 200 | fills the other P600 slot (eviction target) |
| **URGENT** | `cnn_big` | 5 | +22 min | 40 | **drop-preempts one P600 patient → P600×1, then migrates to A40** |

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
  sweep complete.
- **Stage 2 — single-evict preempt.** Submit the urgent `cnn_big` (prio 5,
  deadline +22 min). Assert it lands on **`matemagician` with QuadroP600×1**
  and that **exactly one** `lstm-small` patient was evicted — the other keeps
  its P600 slot alongside URGENT. (A40 stays pinned: RG won't evict the
  equal-priority `cnn_big` pins.)
- **Stage 3 — migrate + resume.** When a pin finishes on its own, assert
  URGENT **migrates P600→A40** (`polimi-gpu`) — cross-node, torch 1.5.1→2.6 —
  and that the **evicted patient resumes** from checkpoint on the freed P600
  slot.
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
  first), pins both A40 slots with prio-5 `lstm-small`, then submits a prio-4
  `cnn_big` (deadline +22 min) and asserts it lands on `matemagician`
  P600×2. Stops early once placement is proven (does not drain).
- **Scenario 3 — `e2e_scenario_unprofiled.sh`.** Proves the *profile-always*
  policy: a **never-profiled** type (`convnet`, its rows wiped in Stage 0) is
  submitted onto a full cluster and must proactively evict the lowest-priority
  running job to take a profiling slot, then the victim resumes from
  checkpoint. Asserts `convnet` enters `PROFILING` with `is_profiling_run =
  true`, a victim exists, the profile completes, and all jobs terminate with
  0 FAILED.

---

# Notes on variability

- **Patient placement** (Scenario 1 Stage 1) depends on solver tie-breaking;
  the exact P600/A40 split can differ run-to-run. Invariant: no slot left
  idle while a schedulable QUEUED job exists, and the profile sweep covers
  every cell including a 2-GPU bundle.
- **Cross-node migration** (Scenario 1 Stage 4) only fires if the urgent job
  originated on P600. Under light load it may start on A40 and never hop —
  hence the check is a warning unless `--strict`.
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

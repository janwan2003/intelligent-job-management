# IJM End-to-End Scenarios

Two scripted exercises of the full cluster, each mirrored by a manual walk-through. Both scenarios are reproducible by clicking through the UI — there are no out-of-band DB writes or preseeds. The scripts just automate the same `POST /jobs`, `POST /jobs/<id>/stop`, etc. calls.

| Scenario | Script | Validates |
|---|---|---|
| 1 — Single-type | [`infra/e2e_scenario.sh`](../infra/e2e_scenario.sh) | Profile sweep · optimizer preempt for urgent · cross-node resume · user-stop + queued-job takeover |
| 2 — Two-types | [`infra/e2e_scenario_2types.sh`](../infra/e2e_scenario_2types.sh) | Same as above with mixed `lstm-small`+`lstm-big` · *two* back-to-back preempts · user-stop + late-submitted takeover |

## Running

```bash
./infra/e2e_scenario.sh          # ~10 min  · 6 jobs total · EPOCHS=40 default
./infra/e2e_scenario_2types.sh   # ~15 min  · 7 jobs total
EPOCHS=20 ./infra/e2e_scenario.sh        # faster, less time to observe stages
./infra/e2e_scenario.sh --strict          # warn → fail for best-effort checks
```

Both expect: API at `$API` (default `http://localhost:8000`), SSH aliases `polimi` (matemagician) and `polimi-gpu` reachable, `wangrat/ijm-lstm-{small,big}:latest` images present on each GPU node, and the standard rclone-over-SFTP shared `data/` mount up.

## Cluster topology assumed

| Node | GPUs | Cost (¢/hr) |
|---|---|---|
| `matemagician` | 2× QuadroP600 | 0.06 |
| `polimi-gpu` | 2× A40 | 0.55 |

4 slots total. Both nodes are `isForProfiling=true`. Checkpoints written on either node are readable from the other via the shared mount.

---

# Scenario 1 — Single-type

`lstm-small` only. 6 jobs total: 4 patient + 1 urgent (optimizer preempt) + 1 late (user-stop takeover).

## Stage 0 — Setup

```bash
# 1. Sanity: API + both workers healthy
curl http://localhost:8000/health
curl http://localhost:8001/health   # matemagician worker via SSH tunnel
curl http://localhost:8002/health   # polimi-gpu worker via SSH tunnel

# 2. Clear state
curl -X DELETE http://localhost:8000/jobs
ssh polimi 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*'
```

**No profile preseed.** The `ProfilingScheduler` fills `profiling_results` from real measurements as jobs run — first submissions of each type trigger profile runs for every `(node, GPU)` config the policy wants to explore (`PROFILING_CONFIGS_PER_JOB=1`, default). This makes the scenario reproducible from the UI: the only requirement is "click Clear All", everything else flows from submissions.

## Stage 1 — Four patient jobs

Submit four `lstm-small` jobs with priorities/deadlines designed to make the Stage 2 preempt target unambiguous *and* to pin the steady-state node placement:

| Job | Priority | Deadline | Expected node @ steady state | Why |
|---|---|---|---|---|
| JOB1 | 1 | +8 h | **polimi-gpu (1× A40)** | low priority → small `priority × tardiness` cost → optimizer "tolerates" putting it on the expensive node when capacity forces overflow |
| JOB2 | 4 | +1 h | **matemagician (1× P600)** | high priority + plenty of slack → cheap node satisfies the deadline, low total objective |
| JOB3 | 4 | +2 h | **matemagician (1× P600)** | same reasoning |
| JOB4 | 4 | +4 h | **polimi-gpu (1× A40)** | high priority but loose deadline → again forced to A40 by overflow capacity |

**Why this placement is deterministic.** A40 costs ≈ 9× more per hour than P600 (0.55 vs 0.06 ¢/hr). All four jobs have ≥ 58 min slack on either node and runtime < 6 min, so `priority × tardiness = 0` everywhere. The cost term `runtime × node_cost` dominates and the optimizer wants every job on P600 — but P600 only has 2 slots, so 2 jobs must overflow to A40. Among the 4, the optimizer sends the **two with the lowest priority** to A40 (overflow penalty matters less for them in the objective), and keeps the two **higher-priority** jobs on the cheaper P600 — even though A40 is "faster", it's not needed when deadlines are loose. Priority-1 jobs end up on A40, priority-4 on P600.

Submit one every 5 seconds so each falls into its own optimizer round (placement decision is independent of the others — useful for reproducibility):

```bash
DL_TIGHT=$(date -u -d '+1 hours' +%Y-%m-%dT%H:%M:%SZ)
DL_MED=$(date  -u -d '+2 hours' +%Y-%m-%dT%H:%M:%SZ)
DL_LONG=$(date -u -d '+4 hours' +%Y-%m-%dT%H:%M:%SZ)
DL_LOOSE=$(date -u -d '+8 hours' +%Y-%m-%dT%H:%M:%SZ)

submit() { curl -sS -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d "{
    \"job_id\":\"lstm-small\",\"dockerImage\":\"wangrat/ijm-lstm-small:latest\",
    \"command\":[],\"Priority\":$1,\"deadline\":\"$2\",\"epochsTotal\":40}" | jq -r .id; }

JOB1=$(submit 1 "$DL_LOOSE"); sleep 5
JOB2=$(submit 4 "$DL_TIGHT"); sleep 5
JOB3=$(submit 4 "$DL_MED");   sleep 5
JOB4=$(submit 4 "$DL_LONG")
```

### What happens

1. **Profile sweep, cold cache.** No `(lstm-small, gpu_config)` row exists yet. Each first submission grabs an *unprofiled* config — `PROFILING_CONFIGS_PER_JOB=1` per job instance, so JOB1 profiles one cell (e.g. `A40×1`), JOB2 grabs the next (`A40×2`), JOB3 (`QuadroP600×1`), JOB4 (`QuadroP600×2`). Once each finishes (3 epochs, `compute_duration` excludes warmup), its row flips `is_profiling_run=false` and re-queues for a standard run; the optimizer can now plan with real cost data.
2. **Optimizer placement converges** to the table above. With slack ≥ 58 min on every job's deadline and per-job runtime ≤ 6 min on either node, no job ever risks tardiness — so the objective is pure cost minimization, capped by slot availability. The 2× A40 slots go to the highest-priority+tightest-deadline pair (JOB2, JOB3); the 2× P600 slots go to JOB1 + JOB4.

### Wait condition

```bash
# Block until all 4 are SIMULTANEOUSLY status=RUNNING with non-null progress.
# 10-min cap covers the cold profile sweep.
```

Don't just wait for "no QUEUED" — the slow-node patient may still be in profiling while the fast-node ones have already finished, draining slots and leaving no preempt victim for Stage 2.

---

## Stage 2 — Urgent past-deadline job triggers a preempt

```bash
PAST=$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ)
JOB5=$(submit 5 "$PAST")
```

### Expected placement after Stage 2

| Job | New status | New node | Why |
|---|---|---|---|
| JOB5 (prio 5, past deadline) | `RUNNING` | **polimi-gpu (A40)** | past-deadline tardiness scales as `priority × seconds`; A40 finishes ~50 % faster than P600 → minimizes the tardiness term |
| JOB3 (prio 4, +2 h) | `QUEUED` → `RUNNING` on **matemagician (P600)** | displaced from A40 | among the two A40 occupants, JOB3 has more deadline slack than JOB2 → cheaper to move |
| JOB1 (prio 1, +8 h) | `QUEUED` → `RUNNING` on its same P600 slot, **after** JOB3 displaces it | low-prio P600 slot freed to make room for JOB3 — or JOB1 stays put if optimizer finds a better packing |
| JOB2, JOB4 | unchanged | unchanged | tight deadline (JOB2) / no benefit to moving (JOB4) |

### What happens, step by step

1. **Submission fires `NOTIFY ijm_schedule`** ([backend/src/routers/jobs.py](../backend/src/routers/jobs.py)). The notify listener wakes the optimizer within ~50 ms — without it the safety-net watcher would wait up to 60 s.
2. **Optimizer sees `expected_tardiness > 0` for JOB5** (priority 5 × past-deadline-seconds) and treats it as critical. It picks the running A40 job whose preemption costs least under its objective (priority × tardiness + GPU cost + switch penalty). Among the two A40 occupants {JOB2, JOB3}, JOB3 has +2 h slack vs JOB2's +1 h — preempting JOB3 cheaper. *(If the optimizer's tiebreaking lands on JOB2 instead, that's still valid — what's invariant is "the A40 victim is one of {JOB2, JOB3}".)*
3. **Preempt sequence.** API → `dispatcher.stop(victim, reason="auto")` → worker `/stop`. Worker kills the container **first**, then atomically writes `status=QUEUED, assigned_node=NULL`. Kill-first-persist-after means the DB row never points at a still-alive container.
4. **JOB5 dispatches** into the freed A40 slot. The victim's `data/checkpoints/<victim_id>/latest.pt` is preserved.
5. **Cascade.** The displaced A40 victim (JOB3) re-queues. Optimizer next round finds it a slot. With JOB1 (priority=1) still on a P600 slot, JOB1 is the cheaper victim for a second preempt → JOB1 → `QUEUED`, JOB3 → P600.

### Wait conditions

- JOB5 placed (RUNNING/PROFILING) within 30 s of submission — bounds the NOTIFY path.
- Preempt evidence on any previously-RUNNING id within another 30 s. Evidence is *any* of: row → QUEUED+null, or `assigned_node` changed, or `status=FAILED|PREEMPTED`.
- Victim's container is gone from the **origin** node (scoped — the optimizer may already have re-dispatched it elsewhere).

---

## Stage 3 — Resume the preempted job (best-effort cross-node)

The optimizer eventually re-schedules the preempted row. When an A40 frees (or a P600), the dispatcher picks the cheapest node satisfying the constraints. Cross-node migration is exercised when the new node ≠ the origin node — worth verifying because the read path through the shared mount has rcloneSFTP / UID quirks:

- The trainer container runs as host UID (`--user UID:GID` in [worker/docker.py](../worker/docker.py)) so checkpoints written by one node are readable by SFTP-as-the-same-user from another.
- `runtime/base.py`'s `load_checkpoint` does `io.BytesIO(self.checkpoint_path.read_bytes())` then `torch.load(...)` — pre-buffering avoids FUSE mmap rejection.

### Wait conditions

- Preempted row's `status != QUEUED` within 10 min.
- Trainer log line `Resumed from epoch N>=1` — soft check (warn-only) because logs can rotate or the resume might have started before this stage observed the row.

---

## Stage 4 — User-stop + a pre-queued job takes over

This stage tests the **user-driven** preempt path — the one the UI's Stop button uses, distinct from Stage 2's optimizer-driven (auto) preempt:

| Path | Status after | Slot release | Resume |
|---|---|---|---|
| `reason=auto` (optimizer) | `QUEUED`, `assigned_node=NULL` | NOTIFY frees slot, picked up next round | automatic on next scheduler pass |
| `reason=user` (Stop button) | `PREEMPTED`, `assigned_node` preserved | NOTIFY frees slot, but row stays parked | manual `POST /resume` only |

The atomicity guarantee: the API endpoint atomically flips `status → PREEMPTED` **before** dispatching the async container kill. This blocks the optimizer from racing in — its preempt-list query filters by `status IN ('RUNNING','PROFILING')`, so a flipped row is invisible to it.

```bash
# 1. Submit JOB6 (priority=1, loose deadline).  Whether it sits queued or
#    runs immediately depends on the optimizer's instantaneous decision —
#    either way at least one other previously-queued job is the takeover
#    candidate.
JOB6=$(submit 1 "$DL_LONG")

# 2. Snapshot the QUEUED set NOW (before the user-stop).  These are the
#    candidates the test will look for on the freed slot afterwards.
QUEUED_BEFORE=$(curl -s http://localhost:8000/jobs | jq -c '[.[] | select(.status == "QUEUED") | .id]')

# 3. Pick the lowest-priority RUNNING job (excluding JOB6) — user-stop it.
USER_VICTIM=$(curl -s http://localhost:8000/jobs \
    | jq -r --arg j6 "$JOB6" '[.[] | select(.status=="RUNNING" and .id != $j6)] | sort_by(.priority) | .[0].id')
curl -X POST http://localhost:8000/jobs/$USER_VICTIM/stop

# 4. Verify victim → PREEMPTED, container gone from origin node, some
#    previously-queued job is now RUNNING.
```

**No automatic `/resume` here** — the test honors the user-stop contract: the row stays `PREEMPTED` until the operator explicitly clicks Resume in the UI. Stage 5 therefore expects exactly one `PREEMPTED` leftover.

### Wait conditions / verification

- User-victim reaches `status=PREEMPTED` within 15 s. (`assigned_node` is *preserved* — only auto-stop clears it.)
- Victim's container is gone from its origin node, polling up to 60 s (the worker HTTP call is end-to-end async over the SSH tunnel).
- At least one of the ids in `QUEUED_BEFORE` is now `RUNNING` or `PROFILING` within 60 s — i.e. the slot did not strand idle.

---

## Stage 5 — Final state

```bash
# Wait for all 6 jobs to terminate (≤ 1 h cap).
while true; do
  states=$(curl -s http://localhost:8000/jobs | jq -r '.[].status' | sort | uniq -c)
  echo "$states"
  echo "$states" | grep -qE 'RUNNING|QUEUED|PROFILING' || break
  sleep 15
done
```

### Verification

| Check | Expectation |
|---|---|
| Terminal counts | `SUCCEEDED == 5`, `PREEMPTED == 1` (user-victim), `FAILED == 0`, `total == 6` |
| No zombie training containers | `docker ps --filter name=^ijm-` empty on both nodes |
| Profiling cache populated | ≥ 2 rows in `profiling_results` for `lstm-small` |
| GPU actually used per node | trainer log on every node that ran a job contains `Using device: cuda` (silent CPU fallback would otherwise let the test pass while running 5-10× slower) |

---

# Scenario 2 — Two-types

Adds a second type (`lstm-big`), drives **two** back-to-back optimizer preempts (one per type), and the user-stop's replacement comes from a **late submission** rather than the existing queue. 7 jobs total.

Stages 0 mirrors Scenario 1 exactly. Differences below.

## Stage 1 — Four mixed-type patient jobs

| Job | Type | Priority | Deadline | Expected node @ steady state |
|---|---|---|---|---|
| JOB1 | `lstm-small` | 1 | +8 h | **matemagician (1× P600)** |
| JOB2 | `lstm-big` | 1 | +8 h | **matemagician (1× P600)** |
| JOB3 | `lstm-small` | 4 | +1 h | **polimi-gpu (1× A40)** |
| JOB4 | `lstm-big` | 4 | +2 h | **polimi-gpu (1× A40)** |

**Why this placement.** Same logic as Scenario 1 — A40 ~9× pricier than P600, slack on every deadline → cost minimization caps at slot capacity. The two prio=4 jobs with tighter deadlines (JOB3 +1 h, JOB4 +2 h) take the A40 slots; the prio=1 jobs (JOB1, JOB2) take the P600 slots. Each node ends up with one `lstm-small` and one `lstm-big` — so both image variants are in flight on both nodes, which exercises both cross-image execution paths.

Both types must complete their cold profile sweeps before Stage 2. With `PROFILING_CONFIGS_PER_JOB=1`, each of the 4 patient instances profiles one cell; the profile cache ends Stage 1 with at most 4 rows per type (the count fills as the optimizer naturally cycles through the remaining cells in later stages).

Wait condition same as Scenario 1: all 4 simultaneously RUNNING+progressed.

## Stage 2 — Urgent `lstm-small` forces preempt #1

```bash
URGENT_SMALL=$(submit lstm-small 5 "$PAST")
```

### Expected placement after Stage 2

| Job | New status | New node |
|---|---|---|
| `URGENT_SMALL` (prio 5, past) | `RUNNING` | **polimi-gpu (A40)** — fastest, minimizes tardiness |
| Victim | `QUEUED` (auto-preempt) | freed an A40 slot for URGENT_SMALL |

**Expected victim: JOB3 (lstm-small prio=4 +1 h).** Both A40 slots at this point are held by JOB3 and JOB4 (per Stage 1). Among them, JOB3 (lstm-small) is the natural urgent_small replacement — same image, no image-swap cost, same `lstm-small` runtime curve. Optimizer should preempt JOB3 → JOB3 re-queues, eventually lands on a P600 slot (displacing JOB1 or running after a Stage-1 P600 job finishes). *(If JOB4 is preempted instead, that's the same cost class — assertion is "victim is one of the A40 occupants".)*

## Stage 3 — Urgent `lstm-big` forces preempt #2

Wait for the cluster to refill to 4 active first — otherwise Stage 3 might find a free slot and not need to preempt anyone:

```bash
# Wait: [select(status == "RUNNING" or "PROFILING")] | length >= 4
URGENT_BIG=$(submit lstm-big 5 "$PAST")
```

### Expected placement after Stage 3

| Job | New status | New node |
|---|---|---|
| `URGENT_BIG` (prio 5, past) | `RUNNING` | **polimi-gpu (A40)** — same logic as URGENT_SMALL |
| Victim | `QUEUED` (auto-preempt) | freed an A40 slot |

**Expected victim: JOB4 (lstm-big prio=4 +2 h).** Mirror of Stage 2: same image as the urgent (lstm-big), so no migration / image cost, and it's on the A40 slot URGENT_BIG wants. *(If the optimizer picks the lstm-small running on A40 instead, the test still passes — the assertion is "preempt happened, victim ≠ Stage-2 victim".)*

## Stage 4 — Both preempted jobs resume

For each of `PREEMPTED1`, `PREEMPTED2`:
- Wait for `status != QUEUED` and `progress != null` (≤ 10 min).
- Soft-check `Resumed from epoch N>=1` in the trainer log.

Cross-node resume sanity: at least one of the two preempted jobs should end up on a *different* node than where it started. With `--strict` this is mandatory; otherwise it's a warn.

## Stage 4.5 — User-stop + late-submitted takeover

Same atomic user-stop path as Scenario 1's Stage 4, but the replacement job is submitted **after** the stop lands — exercising the "fresh dispatch into fresh capacity" path rather than backfilling from the existing queue:

```bash
# 1. Pick the lowest-priority running job, user-stop it.
USER_VICTIM=$(curl -s http://localhost:8000/jobs \
    | jq -r '[.[] | select(.status=="RUNNING")] | sort_by(.priority) | .[0].id')
USER_VICTIM_TYPE=$(curl -s http://localhost:8000/jobs/$USER_VICTIM | jq -r .job_id)
curl -X POST http://localhost:8000/jobs/$USER_VICTIM/stop

# 2. Wait: victim → PREEMPTED + container gone from origin node (up to 60s).

# 3. Submit JOB7 of the OPPOSITE type from the victim — exercises both
#    images in this scenario.
LATE_TYPE=$([ "$USER_VICTIM_TYPE" = "lstm-small" ] && echo "lstm-big" || echo "lstm-small")
JOB7=$(submit_typed $LATE_TYPE 1 "$DL_LOOSE")
# Wait: JOB7 placed within 60s.

# 4. Leave the user-stopped job in PREEMPTED — honor the sticky contract.
```

## Stage 5 — Final state

| Check | Expectation |
|---|---|
| Terminal counts | `SUCCEEDED == 6`, `PREEMPTED == 1` (user-victim), `FAILED == 0`, `total == 7` |
| No zombies | both nodes clean |
| Profile cache | ≥ 2 rows each for `lstm-small` AND `lstm-big` |
| GPU used | `Using device: cuda` in trainer log on every node that ran a job |

---

# Notes on flakiness / variability

- **Optimizer placement** for the 4 patient jobs depends on solver tiebreaking. The exact node assignment may differ across runs — what's invariant is "no slot left idle when a QUEUED job exists with profiling data" and "higher priority gets the faster node when both are available". If JOB5/urgent ever lands on a node that wasn't expected, that's still valid as long as the preempted job's priority is ≤ JOB5's.
- **Cross-node resume** only fires if the optimizer chooses a different node for the resumed job. With light cluster load this often doesn't happen; that's why the check is a soft warning, escalated only under `--strict`.
- **User-stop atomicity** depends on the API endpoint's `SELECT FOR UPDATE` + status flip landing before the optimizer's next preempt-list build. Under heavy optimizer churn (e.g. cost-driven reshuffling triggered by tight deadlines) the race window can be longer; the script polls for up to 60 s before declaring the kill failed.
- **DataParallel quirk.** When the trainer is wrapped in `nn.DataParallel` (≥ 2 visible GPUs), `LSTM.flatten_parameters()` is called inside `forward()` to avoid the cudnn re-pack tax. Without this, 2-GPU runs of small LSTMs are ~2× slower than 1-GPU; with it the gap shrinks but for tiny models 1-GPU still wins (LSTM doesn't amortize the scatter/gather cost). The profile cache records both honestly — the optimizer then prefers 1-GPU configs for these workloads.

---

# What this proves, by stage

| Component | Single-type | Two-types |
|---|---|---|
| Cold-cache profile sweep, one config per submission | Stage 1 | Stage 1 (both types) |
| NOTIFY-driven optimizer wake-up (~ms, not 60 s) | Stage 2 | Stage 2 + 3 |
| Optimizer cost-vs-priority placement | Stage 1 | Stage 1 |
| Past-deadline urgency triggers preempt | Stage 2 | Stage 2 + 3 |
| Kill-first-persist-after on preempt | Stage 2 | Stage 2 + 3 |
| Checkpoint preserved across preempt | Stage 3 | Stage 4 |
| Cross-node resume via shared mount | Stage 3 (soft) | Stage 4 (soft, both jobs) |
| Atomic user-stop (sticky PREEMPTED, optimizer-safe) | Stage 4 | Stage 4.5 |
| Freed slot fills from existing QUEUED set | Stage 4 | — |
| Freed slot accepts a brand-new late submission | — | Stage 4.5 |
| User-stop is sticky: PREEMPTED leftover at end | Stage 5 | Stage 5 |
| Clean termination, no zombies | Stage 5 | Stage 5 |
| Per-node GPU actually used (no silent CPU fallback) | Stage 5 | Stage 5 |

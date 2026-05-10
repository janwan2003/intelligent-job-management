# IJM End-to-End Test Scenario

A scripted exercise of the full system across two heterogeneous GPU nodes. It validates profiling, greedy + optimizer placement, immediate optimizer wake-up via `NOTIFY`, auto-preemption, cross-node checkpoint resume via the rclone share, atomic-claim semantics, log mtime selection, and clean termination.

## Running the script

The full scenario is automated in [`infra/e2e_scenario.sh`](../infra/e2e_scenario.sh):

```bash
./infra/e2e_scenario.sh                  # default: 20 epochs, 60-min terminal cap
EPOCHS=5 ./infra/e2e_scenario.sh         # faster (~10 min total)
./infra/e2e_scenario.sh --strict         # fail on the best-effort cross-node / log checks too
```

It expects the API at `http://localhost:8000`, both worker nodes reachable as SSH aliases `polimi` (matemagician) and `polimi-gpu`, and the runtime image `wangrat/ijm-lstm-small:latest` already built on both nodes.

The remainder of this doc walks through each stage explaining **why** each thing happens — useful when debugging a script failure or onboarding a new engineer.

## Cluster topology

| Node | GPUs | Cost |
|---|---|---|
| `matemagician` | 2× QuadroP600 | low (0.10) |
| `polimi-gpu` | 2× A40 (~5× faster) | high (0.30) |

Total capacity: **4 single-GPU slots**. Both nodes are configured with `isForProfiling: true`, so the profiler can claim a slot on either. Both nodes mount `~/ijm/data` as the same rclone-over-SFTP share rooted on matemagician, so checkpoints written on either node are readable on the other.

## Setup

```bash
# Wipe all DB rows + on-disk per-job state
curl -X DELETE http://localhost:8000/jobs
ssh polimi 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*'

# Confirm both workers running
ssh polimi     'docker ps --filter name=wangrat-ijm-worker --format "{{.Status}}"'
ssh polimi-gpu 'docker ps --filter name=wangrat-ijm-worker --format "{{.Status}}"'
```

**Why**: a fresh state ensures the `(job_id, gpu_config)` profiling cache starts empty (so we observe profiling actually run) and that no stale root-owned legacy checkpoint poisons the resume path.

---

## Stage 1 — Four "patient" jobs

Submit four `lstm-small` jobs with deadlines 2 hours out and increasing priorities:

```bash
DEADLINE=$(date -u -d '+2 hours' +%Y-%m-%dT%H:%M:%SZ)
for prio in 1 2 3 4; do
  curl -sS -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d "{
    \"job_id\": \"lstm-small\",
    \"dockerImage\": \"wangrat/ijm-lstm-small:latest\",
    \"command\": [],
    \"Priority\": ${prio},
    \"deadline\": \"${DEADLINE}\",
    \"epochsTotal\": 20
  }" | jq -r '.id'
done
```

### What happens, step by step

1. **Job 1 (priority 1) submitted.** No cache hit for `(lstm-small, {QuadroP600:1})` or `(lstm-small, {A40:1})`. The greedy scheduler ([backend/src/profiling.py](backend/src/profiling.py)) **claims one untested config** for profiling — say A40 first. Status → `PROFILING`, `assigned_node=polimi-gpu`, `is_profiling_run=true`, occupying 1 of 2 A40s. The route fires `NOTIFY ijm_schedule` (the recently added wake-up).

2. **Job 2 (priority 2) submitted.** Profiler claims the *other* untested config: QuadroP600 on matemagician (1 of 2). *(Why: fill the profiling matrix once per `(type, gpu_config)` rather than re-profiling on every submission.)*

3. **Jobs 3 & 4 submitted.** No untested configs left. The other A40 and other QuadroP600 are free, but greedy won't dispatch a real run until the cache has `duration_seconds` (not just an in-flight claim) — so jobs 3 & 4 stay `QUEUED, assigned_node=NULL`. The notify wakes the optimizer; it sees the cluster half-occupied with profiling runs and chooses not to act. *(Why: the optimizer doesn't preempt profiling, and the remaining slots are effectively reserved against the in-flight measurements landing.)*

4. **Profiling completes (~30 s on A40, ~90 s on QuadroP600).** Each worker runs `profiling_epochs_no=3`, computes the steady-state mean (warmup excluded), writes `duration_seconds` into `profiling_results`, then resets the row to `QUEUED, assigned_node=NULL, is_profiling_run=false` and emits `NOTIFY ijm_schedule`.

5. **First post-profiling optimizer pass.** Cache is now filled. The optimizer sees 4 QUEUED jobs and **4 free slots** — every job fits, no contention. It picks placement to minimize `Σ priority × max(0, finish_time − deadline)` plus the cost term. With 2-hour slack the deadline term is zero, so cost minimization dominates → **lower-priority work to QuadroP600 (cheap), higher-priority work to A40 (faster, finishes sooner)**.

   Expected steady state — **all 4 jobs running concurrently**:
   - `polimi-gpu` (2× A40): jobs 3 + 4 RUNNING (priorities 3, 4)
   - `matemagician` (2× QuadroP600): jobs 1 + 2 RUNNING (priorities 1, 2)

   *(If priorities were equal or deadlines tighter, the optimizer would shuffle differently — what's invariant is "no slot left idle when a QUEUED job exists with profiling data".)*

### Verification

```bash
curl -sS http://localhost:8000/jobs | jq '.[] | {id: .id[0:8], status, priority, node: .assigned_node, gpu: .assigned_gpu_config}'
```

Expect: 4× `RUNNING`, 2 per node, with priorities 3 & 4 on `polimi-gpu` and 1 & 2 on `matemagician`.

---

## Stage 2 — Urgent past-deadline job

```bash
PAST=$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ)
curl -sS -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d "{
  \"job_id\": \"lstm-small\",
  \"dockerImage\": \"wangrat/ijm-lstm-small:latest\",
  \"command\": [],
  \"Priority\": 10,
  \"deadline\": \"${PAST}\",
  \"epochsTotal\": 20
}"
```

### What happens

1. **Job 5 submitted with deadline already past.** `lstm-small` is profiled, so no profiling needed. Greedy attempts placement, fails (all 4 slots busy), returns `node_id=None`; row stays `QUEUED, assigned_node=NULL`.

2. **`NOTIFY ijm_schedule` fires from the route** ([backend/src/routers/jobs.py:99](backend/src/routers/jobs.py#L99)). Without this fix the job would wait up to **60 s** for the safety-net watcher; with it, `_notify_consumer` ([backend/src/app.py:250](backend/src/app.py#L250)) wakes within ~50 ms.

3. **Optimizer sees `expected_tardiness > 0` for job 5** and treats it as critical. Among the 4 running jobs it picks the one whose preemption costs least: lowest priority, with the urgency-weighted finish time on the freed slot favouring A40. **Likely action: preempt job 1 (priority 1, on QuadroP600) and place job 5 on the freed QuadroP600 slot.** *(A40 would let job 5 finish faster, but preempting a priority-3-or-4 job costs more in `priority × tardiness` than the speed-up gained — the solver picks the lowest-priority displacement. If deadline pressure is severe enough the math can flip; the invariant is "the displaced job is the one whose preemption increases the objective the least".)*

4. **Preemption sequence (validates several recent worker fixes):**
   - API → `JobDispatcher.stop(job_1_id, reason="auto")` → HTTP `POST /stop?reason=auto` to matemagician's worker.
   - Worker `stop_job` ([worker/app.py](worker/app.py)) waits for the dispatch placeholder if Phase 1 is mid-launch, then **kills the container first**, **verifies the kill landed** via `kill_container` (which raises if the container survives), and **only after** persists `status=QUEUED, assigned_node=NULL, container_name=NULL`. *(Pre-fix: silent kill failure left a row pointing at a still-alive container, with `progress` ticking up under `status=QUEUED`.)*
   - API's `_wait_for_preemption` polls until `assigned_node IS NULL` — the post-kill commit. It then assigns job 5 to QuadroP600 in Phase 1b and dispatches.
   - matemagician's worker Phase 1 atomically claims the row with `WHERE status = ANY(RUNNABLE_STATUSES) RETURNING id`. *(Pre-fix: a non-atomic fetch+update could let a stop arriving between them clobber `RUNNING` with `QUEUED` while the container started anyway.)*

5. **Job 5 starts on QuadroP600 from epoch 0.** Job 1's `data/checkpoints/{job_1_id}/latest.pt` is preserved — the recently removed `is_first_run` wipe used to nuke the dir mid-resume when two dispatches raced.

### Verification

```bash
sleep 3
curl -sS http://localhost:8000/jobs | jq '.[] | select(.priority==10 or .priority==1) | {id: .id[0:8], status, priority, node: .assigned_node, progress}'
```

Expect:
- Job 5 (priority 10): `RUNNING` on `matemagician`, progress `0/20` or `1/20`.
- Job 1 (priority 1): `QUEUED, assigned_node=null`, `progress` reflecting the epoch reached before the kill (e.g. `5/20`).

```bash
# Worker is consistent with DB — exactly 2 running ijm-* containers on matemagician
# (job 5 and the still-running job 2), and no leftover from job 1.
ssh polimi 'docker ps --filter name=ijm- --format "{{.Names}} {{.Status}}"'
```

---

## Stage 3 — Resume preempted job, cross-node

When job 5 (or any other RUNNING job) finishes, the optimizer reconsiders. Job 1 has `progress=N/20` recorded; the optimizer factors in remaining work (`epochs_total − N`) and reassigns it.

### What happens

1. **Job 5 reaches `SUCCEEDED`.** Phase 4 commits the terminal status; the in-process `_on_job_completed` fires another `_schedule_waiting_jobs` pass.

2. **Optimizer reassigns job 1.** Suppose at this point an A40 slot has just been freed by job 4 finishing (A40 is faster, so job 4 finishes well before its QuadroP600 sibling). The optimizer parses `progress` to compute remaining epochs and picks the cheapest node satisfying constraints. **If the placement lands on `polimi-gpu` (different node than where the checkpoint was written)**, this exercises cross-node resume.

3. **polimi-gpu's worker dispatches job 1:**
   - Mounts `~/ijm/data/checkpoints/<job_1_id>/` (rclone view of matemagician's local FS) into the container at `/checkpoints`.
   - The container runs as host UID `wangrat:wangrat` (pinned by `--user UID:GID` in [worker/docker.py](worker/docker.py)) with `USER=ijm HOME=/tmp` env vars set so `getpass.getuser()` doesn't crash inside torch.dynamo.
   - The checkpoint file was written by matemagician's container, also as `wangrat:wangrat` thanks to the same `--user` pinning — so SFTP-as-wangrat **can** read it. *(Pre-fix: file was `root:root 0600`, SFTP returned EPERM at open(), `read_bytes()` failed.)*
   - `runtime/base.py:load_checkpoint` does `io.BytesIO(self.checkpoint_path.read_bytes())` then `torch.load(...)` — pre-buffering avoids FUSE mmap rejection.
   - Trainer logs `Resumed from epoch N (best acc: X%)`.

4. **Training continues from epoch N+1 to 20**, atomically saving `latest.pt` each epoch (via `tempfile.mkstemp` → `replace`).

### Verification

```bash
# File ownership: cross-node-readable
ssh polimi 'stat -c "%U:%G %a" ~/ijm/data/checkpoints/<job_1_id>/latest.pt'
# expect: wangrat:wangrat 600

# Watch progress climb starting from the preempted epoch
watch -n 2 "curl -s http://localhost:8000/jobs/<job_1_id> | jq '{status, progress, node: .assigned_node}'"

# Worker log: confirm the resume message
ssh polimi-gpu 'docker logs --tail 50 wangrat-ijm-worker 2>&1' | grep -E "Resumed|Starting training from epoch"
```

---

## Stage 4 — Logs fan-out by mtime

While job 1 is running on its new node, fetch its logs:

```bash
curl -s http://localhost:8000/jobs/<job_1_id>/logs | tail -10
```

### What happens

1. API reads `assigned_node = polimi-gpu`, proxies to that worker's `GET /jobs/<id>/logs`.
2. polimi-gpu's worker returns the file content with `X-Log-Mtime: <epoch>` header.
3. The pre-preemption log on **matemagician** is *longer* (it covers the original 8 epochs); polimi-gpu's log is *shorter* (just resume output) but its mtime is now. The fan-out helper in [backend/src/routers/jobs.py:get_job_logs](backend/src/routers/jobs.py) ranks responses by **mtime descending**, then length — the live log wins. *(Pre-fix: `max(by len)` returned the frozen historical file, making the UI look stuck while progress climbed in the background.)*

### Verification

```bash
# Should show recent epochs, not stuck at the pre-preemption epoch
curl -s http://localhost:8000/jobs/<job_1_id>/logs | grep -E "Epoch [0-9]+/20" | tail -3
```

---

## Stage 5 — Final state & cleanup

Wait for everything to terminate:

```bash
while true; do
  states=$(curl -s http://localhost:8000/jobs | jq -r '.[].status' | sort | uniq -c)
  echo "$states"
  if echo "$states" | grep -q SUCCEEDED && ! echo "$states" | grep -qE 'RUNNING|QUEUED|PROFILING'; then
    break
  fi
  sleep 5
done
```

### Final verification

1. **All 5 jobs `SUCCEEDED`.**

2. **Profiling cache populated for both configs:**
   ```bash
   ssh polimi 'docker exec wangrat-ijm-postgres psql -U postgres -d ijm -c \
     "SELECT job_id, gpu_config, node_id, duration_seconds FROM profiling_results WHERE job_id='\''lstm-small'\'';"'
   ```
   Expect 2 rows: one for each `(QuadroP600:1)` and `(A40:1)`, both with `duration_seconds` populated.

3. **No zombie containers:**
   ```bash
   ssh polimi      'docker ps --filter name=ijm- --format "{{.Names}}" | wc -l'  # → 0
   ssh polimi-gpu  'docker ps --filter name=ijm- --format "{{.Names}}" | wc -l'  # → 0
   ```

4. **No PROFILING/RUNNING DB rows leaked:**
   ```bash
   curl -s http://localhost:8000/jobs | jq '[.[] | select(.status == "RUNNING" or .status == "PROFILING")] | length'
   # → 0
   ```

---

## What this scenario proves

| Component | Validated by |
|---|---|
| Profiling cache reuse across job instances | Stage 1: 5 jobs share one `lstm-small` profile |
| `NOTIFY`-driven optimizer wake-up | Stage 2: urgent job placed within seconds, not 60 s |
| Optimizer cost-vs-priority placement | Stage 1: priority 4 → A40, priority 1 → QuadroP600 |
| Past-deadline urgency triggers preempt | Stage 2: lowest-priority RUNNING job preempted |
| Auto-stop: kill-first, persist-after | Stage 2: row never points at a live container |
| `kill_container` raises on survival | Stage 2: silent kill failure surfaced as 500 |
| Atomic Phase-1 claim | Stage 2: no `QUEUED` row under a running container |
| Dispatch placeholder synchronizes /stop | Stage 2: stop arriving in pre-launch window blocks |
| Checkpoint dir not wiped on dispatch race | Stage 3: progress resumes from `N`, not 0 |
| `--user UID:GID` pinning | Stage 3: cross-node SFTP read of `latest.pt` succeeds |
| `USER`/`HOME` env for pinned UID | Stage 1+: torch.dynamo cache_dir setup doesn't crash |
| `chmod 0777` on per-job dirs | Stage 1+: `tempfile.mkstemp` works as non-root |
| `read_bytes` + `BytesIO` for FUSE | Stage 3: `torch.load` doesn't EPERM on rclone path |
| Logs fan-out by mtime | Stage 4: live log served, not stale frozen one |
| Clean termination, no zombies | Stage 5 |

---

## Notes on flakiness / variability

- **Optimizer placement** in Stage 1 and 2 depends on the GPUspb solver's tiebreaking when multiple placements have similar objective values. The exact node assignment for jobs 1–4 may differ across runs; what's invariant is "higher priority gets faster node when both are available". If you see all 4 on QuadroP600 (e.g. solver decided cost minimization dominates), that's still valid — adjust the deadline tighter to force A40 use.

- **Stage 3 cross-node migration** only fires *if* the optimizer actually reassigns job 1 to polimi-gpu after preemption. With the steady-state placement (priorities 1+2 on QuadroP600, 3+4 on A40), job 4 finishing first frees an A40 slot — that's the realistic path to cross-node resume. To force it deterministically, after job 5 starts on QuadroP600 you can manually `POST /jobs/<job_2_id>/stop` (user stop) so when the optimizer next reschedules job 1 only A40 has open slots.

- **Stage 4 mtime fan-out** only triggers the interesting code path when `assigned_node` is briefly NULL (during a preempt). If `assigned_node` is set, the API does direct-proxy and the mtime fan-out isn't exercised. To force-test it, query `/logs` for a job mid-preempt.

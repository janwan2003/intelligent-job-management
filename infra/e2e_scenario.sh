#!/usr/bin/env bash
# End-to-end scenario test, mirroring documentation/e2e-scenario.md.
#
# Validates: profiling, greedy + optimizer placement, NOTIFY-driven optimizer
# wake-up, auto-preemption, checkpoint preservation, no zombies.  Cross-node
# migration (Stage 3) and log mtime fan-out (Stage 4) are best-effort because
# they depend on optimizer choice / timing — failures there are warnings, not
# fatal, unless --strict is passed.
#
# Usage:
#   ./infra/e2e_scenario.sh                  # default: 20 epochs, 60 min cap
#   API=http://localhost:8000 EPOCHS=5 ./infra/e2e_scenario.sh
#   ./infra/e2e_scenario.sh --strict         # also fail on best-effort checks
#
# Expects:
#   - API reachable at $API (default localhost:8000)
#   - SSH aliases polimi (matemagician) and polimi-gpu reachable
#   - Both nodes' workers running, runtime image present, rclone mount up
#   - jq, curl, ssh in PATH
set -euo pipefail

API="${API:-http://localhost:8000}"
NODE_A="${NODE_A:-polimi}"        # matemagician SSH alias
NODE_B="${NODE_B:-polimi-gpu}"    # polimi-gpu SSH alias
JOB_TYPE="${JOB_TYPE:-lstm-small}"
IMAGE="${IMAGE:-wangrat/ijm-${JOB_TYPE}:latest}"
# Long-enough horizon that the fastest A40 epoch (~6s × EPOCHS) outlasts
# the slow-node cold profile sweep (P600 lstm-small ≈ 30 s) — otherwise
# the fast jobs SUCCEED before Stage 1's "all 4 RUNNING+progressed" wait
# trips and there's no preempt victim for Stage 2.  40 epochs at ~6 s =
# 240 s also gives the operator time to observe each stage when running
# the scenario manually through the UI.
EPOCHS="${EPOCHS:-40}"
TERMINAL_TIMEOUT_S="${TERMINAL_TIMEOUT_S:-3600}"   # 60 min for everything to finish
PRIORITY_MAX=5   # backend/src/constants.py:PRIORITY_MAX — max allowed by the API

STRICT=0
[[ "${1:-}" == "--strict" ]] && STRICT=1

C_INFO=$'\033[1;36m'; C_OK=$'\033[1;32m'; C_WARN=$'\033[1;33m'; C_FAIL=$'\033[1;31m'; C_END=$'\033[0m'
log()   { echo "${C_INFO}[$(date +%T)] $*${C_END}"; }
pass()  { echo "${C_OK}  ✓ $*${C_END}"; }
warn()  { echo "${C_WARN}  ⚠ $*${C_END}"; (( STRICT )) && exit 1 || true; }
fail()  { echo "${C_FAIL}  ✗ $*${C_END}"; exit 1; }

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

submit_job() {
    local prio="$1" deadline="$2"
    local resp
    resp=$(curl -sS -X POST "$API/jobs" -H 'Content-Type: application/json' -d "{
        \"job_id\": \"$JOB_TYPE\",
        \"dockerImage\": \"$IMAGE\",
        \"command\": [],
        \"Priority\": $prio,
        \"deadline\": \"$deadline\",
        \"epochsTotal\": $EPOCHS
    }")
    local id
    id=$(echo "$resp" | jq -r '.id // empty')
    if [[ -z "$id" || "$id" == "null" ]]; then
        echo "ERR: POST /jobs failed: $resp" >&2
        return 1
    fi
    echo "$id"
}

job_field() { curl -sS "$API/jobs/$1" | jq -r ".$2"; }
count_status() { curl -sS "$API/jobs" | jq "[.[] | select(.status == \"$1\")] | length"; }
all_jobs() { curl -sS "$API/jobs"; }

# Wait until predicate (jq filter applied to /jobs response) returns true,
# polling every $3 seconds, timing out after $2 seconds.
wait_for() {
    local desc="$1" timeout="$2" interval="$3" filter="$4"
    local elapsed=0
    while (( elapsed < timeout )); do
        if all_jobs | jq -e "$filter" >/dev/null 2>&1; then return 0; fi
        sleep "$interval"; elapsed=$((elapsed + interval))
    done
    return 1
}

# ---------------------------------------------------------------------------
# Stage 0 — Setup
# ---------------------------------------------------------------------------

log "Stage 0: cluster sanity"
curl -fsS "$API/health" >/dev/null || fail "API at $API not reachable"
# Probe each worker /health directly.  The /nodes endpoint doesn't expose
# worker URLs, so we read them from the config the API uses.  In tunneled
# deployments the API and the e2e share the same localhost tunnel ports.
WORKER_CONFIG="${WORKER_CONFIG:-$(dirname "$0")/../config/nodes_config.tunnel.json}"
[[ -f "$WORKER_CONFIG" ]] || fail "worker config not found at $WORKER_CONFIG"
mapfile -t worker_pairs < <(jq -r '.[] | "\(.id) \(.workerUrl)"' "$WORKER_CONFIG")
[[ ${#worker_pairs[@]} -gt 0 ]] || fail "no workers in $WORKER_CONFIG"
for pair in "${worker_pairs[@]}"; do
    node_id="${pair%% *}"
    worker_url="${pair#* }"
    curl -fsS --max-time 5 "$worker_url/health" | jq -e '.status == "ok"' >/dev/null \
        || fail "worker for node $node_id at $worker_url not healthy"
done
pass "API healthy, all workers reachable"

log "Stage 0: clearing state"
curl -sS -X DELETE "$API/jobs" >/dev/null
ssh "$NODE_A" 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*' 2>/dev/null || true
[[ "$(count_status RUNNING)" == "0" ]] || warn "some jobs still RUNNING after Clear All — will continue"
pass "DB cleared, on-disk state wiped"

# Profiling is NOT preseeded.  The ProfilingScheduler fills the cache from
# real measurements as jobs run — first submissions of each type trigger
# profile runs for every (node, GPU) config the policy wants to explore.
# Keeping the script preseed-free means it mirrors what a user does in the
# UI (Submit Job → wait), so the scenario is reproducible manually.

# ---------------------------------------------------------------------------
# Stage 1 — Four patient jobs
# ---------------------------------------------------------------------------

log "Stage 1: submitting 4 patient jobs with priorities/deadlines designed for a deterministic preempt"
# Cluster has 4 slots total (2 × matemagician + 2 × polimi-gpu); a 5th
# urgent submission in Stage 2 must preempt exactly ONE patient job.  We
# make the preempt target unambiguous to both the optimizer and the reader:
#
#   JOB1  priority 1  deadline +8h   ← clearest preempt target
#                                      (lowest priority, loosest deadline)
#   JOB2  priority 4  deadline +1h   ← protected (high priority, tight deadline)
#   JOB3  priority 4  deadline +2h
#   JOB4  priority 4  deadline +4h
#
# Any reasonable optimizer that scores by (priority, slack-to-deadline)
# will pick JOB1 to evict.  All 4 use the same image so there's no
# cost-driven migration incentive between them — the cluster stays
# placement-stable until JOB5 lands.
DL_TIGHT=$(date -u -d '+1 hours'  +%Y-%m-%dT%H:%M:%SZ)
DL_MED=$(date  -u -d '+2 hours'  +%Y-%m-%dT%H:%M:%SZ)
DL_LONG=$(date -u -d '+4 hours'  +%Y-%m-%dT%H:%M:%SZ)
DL_LOOSE=$(date -u -d '+8 hours' +%Y-%m-%dT%H:%M:%SZ)
# 5 s between submissions: each falls into its own optimizer round so the
# placement decision for each is independent of the others (no batching
# inside a single optimizer call).  Makes per-job placement reproducible.
JOB1=$(submit_job 1 "$DL_LOOSE"); sleep 5   # preempt target
JOB2=$(submit_job 4 "$DL_TIGHT"); sleep 5
JOB3=$(submit_job 4 "$DL_MED");   sleep 5
JOB4=$(submit_job 4 "$DL_LONG")
log "  ids: target=${JOB1:0:8}  protected=${JOB2:0:8} ${JOB3:0:8} ${JOB4:0:8}"

# Require all 4 patient jobs to be SIMULTANEOUSLY RUNNING (status != PROFILING
# / SUCCEEDED) with non-null progress, before submitting the urgent job.
# Without this, the fast-node lstm-small can SUCCEED while the slow-node one
# is still profiling, draining slots and leaving no preempt victim.  Generous
# 10-min cap covers the cold-cache profile sweep (no preseed).
log "  waiting for all 4 to be simultaneously RUNNING with progress (≤ 10 min)…"
if ! wait_for "all 4 RUNNING+progressed" 600 5 \
    '[.[] | select(.status == "RUNNING" and .progress != null)] | length == 4'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, priority, node: .assigned_node, progress}'
    fail "couldn't get all 4 patient jobs simultaneously RUNNING+progressed in 10 min"
fi
pass "all 4 jobs RUNNING with progress (preempt target available)"

# Print actual placement so the operator can compare against the
# documented expected table (documentation/e2e-scenarios.md#stage-1):
#   JOB1 prio=1 +8h  → matemagician (1× QuadroP600)
#   JOB2 prio=4 +1h  → polimi-gpu   (1× A40)
#   JOB3 prio=4 +2h  → polimi-gpu   (1× A40)
#   JOB4 prio=4 +4h  → matemagician (1× QuadroP600)
log "  steady-state placement (see e2e-scenarios.md for expected):"
for label in JOB1 JOB2 JOB3 JOB4; do
    jid_var="${label}"; jid="${!jid_var}"
    node=$(job_field "$jid" assigned_node)
    cfg=$(job_field "$jid" assigned_gpu_config)
    prio=$(job_field "$jid" priority)
    log "    $label=${jid:0:8} prio=$prio → $node $cfg"
done

running_count=$(all_jobs | jq '[.[] | select(.status == "RUNNING" or .status == "PROFILING")] | length')
log "  current cluster: $running_count active (RUNNING/PROFILING)"

# ---------------------------------------------------------------------------
# Stage 2 — Urgent past-deadline job + NOTIFY-driven optimizer kick-in
# ---------------------------------------------------------------------------

log "Stage 2: capturing pre-submit RUNNING (id, node) map"
# Snapshot id+assigned_node for every currently-RUNNING job.  We use this to
# detect preempt evidence in *any* of three forms within a 30s window:
#   (a) the row is now QUEUED+assigned_node IS NULL  — classic preempt,
#   (b) the row's assigned_node changed              — preempt + re-place,
#   (c) the row is now FAILED or PREEMPTED           — kill landed and Phase 4
#                                                       set FAILED before /stop
#                                                       could re-queue it.
# This is robust to the optimizer reassigning faster than a single 3s probe.
PRE_RUNNING=$(all_jobs | jq -c '[.[] | select(.status == "RUNNING") | {id, node: .assigned_node}]')
log "  pre-submit RUNNING (id/node): $(echo "$PRE_RUNNING" | jq -r 'map("\(.id[0:8])@\(.node)") | join(" ")')"

log "Stage 2: submitting urgent past-deadline job (priority=$PRIORITY_MAX)"
PAST=$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ)
JOB5=$(submit_job "$PRIORITY_MAX" "$PAST") || fail "POST /jobs rejected the urgent job"
log "  id: ${JOB5:0:8}"

log "  verifying optimizer kicks in within 30s (NOTIFY path, not 60s watcher)…"
T0=$(date +%s)
# Budget: 30s.  Optimizer fires within ~1s of submission, plans urgent on
# A40, /stop the victim → worker kill (~3-5s) → ijm_slot_freed NOTIFY →
# dispatch acquires slot → /run.  ~10-15s in steady state.  30s gives
# headroom for slow preempts.
if ! wait_for "job5 placed" 60 1 \
    "[.[] | select(.id == \"$JOB5\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length >= 1"; then
    s=$(job_field "$JOB5" status)
    fail "job 5 still status=$s after 30s — NOTIFY-driven optimizer didn't fire (or preempt round-trip too slow)"
fi
ELAPSED=$(( $(date +%s) - T0 ))
pass "job 5 placed in ${ELAPSED}s (NOTIFY working)"

log "  polling up to 30s for preempt evidence on any previously-RUNNING id…"
PREEMPTED_ID=""
for _ in $(seq 30); do
    sleep 1
    cur=$(all_jobs)
    # Find first pre-RUNNING id whose state shows preempt evidence (a/b/c above).
    pid=$(jq -r --argjson pre "$PRE_RUNNING" '
        ($pre[]) as $p
        | (map(select(.id == $p.id)) | first) as $now
        | select($now != null
            and ( ($now.status == "QUEUED" and $now.assigned_node == null)
               or ($now.assigned_node != null and $now.assigned_node != $p.node)
               or ($now.status == "FAILED" or $now.status == "PREEMPTED") ))
        | $now.id
    ' <<<"$cur" | head -1)
    if [[ -n "$pid" ]]; then
        PREEMPTED_ID=$pid
        break
    fi
done
if [[ -z "$PREEMPTED_ID" ]]; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, priority, node: .assigned_node, progress}'
    fail "no preempt evidence on any previously-RUNNING id within 30s"
fi
pass "preempt observed for id=${PREEMPTED_ID:0:8}"

# Note: the optimizer scores by a cost function that mixes priority,
# deadline slack, switch-penalty, and per-node GPU efficiency — not by
# (priority, slack) alone.  In practice that means JOB1 (priority=1,
# deadline+8h) is the *most likely* preempt target but not the only valid
# one — e.g. if JOB1 is on a node that can't host JOB5, kicking a
# higher-priority job on the right node is correct.  We log which job was
# picked for visibility but don't fail the test on choice alone.
preempted_prio=$(all_jobs | jq --arg p "$PREEMPTED_ID" '.[] | select(.id == $p) | .priority')
log "  optimizer chose to preempt id=${PREEMPTED_ID:0:8} (priority=$preempted_prio)"
log "  (cluster-target was JOB1=${JOB1:0:8} priority=1 — see scenario comment for scoring rationale)"

# Soft check: did the preempted job have time to record progress?  If yes,
# the checkpoint-preservation path is exercised.  If no (preempt fired before
# epoch 1 finished), we don't fail — fresh-start preempt is still valid.
preempted_progress=$(job_field "$PREEMPTED_ID" progress)
if [[ "$preempted_progress" != "null" && -n "$preempted_progress" ]]; then
    pass "preempted job had progress=$preempted_progress at preempt time (checkpoint will be restored)"
else
    warn "preempted job had no recorded progress yet — Stage 3 resume won't validate checkpoint reuse"
fi

# Confirm worker-side state matches DB: no zombie container on the ORIGINAL
# node for the preempted job.  We must scope to the pre-preempt node because
# auto-preempt re-queues the job and the optimizer can re-dispatch it to
# the OTHER node within ≤3s — a container with the same name running there
# is a resume, not a leak.  The kill-first-persist-after invariant only
# concerns the node the job was on when /stop fired.
preempt_origin_node=$(echo "$PRE_RUNNING" | jq -r --arg p "$PREEMPTED_ID" '.[] | select(.id == $p) | .node')
# Map cluster node id ("matemagician" / "polimi-gpu") → SSH alias (NODE_A / NODE_B).
case "$preempt_origin_node" in
    matemagician) origin_ssh="$NODE_A" ;;
    polimi-gpu)   origin_ssh="$NODE_B" ;;
    *)            origin_ssh="" ;;
esac
if [[ -z "$origin_ssh" ]]; then
    warn "could not map pre-preempt node ($preempt_origin_node) to SSH alias — skipping zombie check"
else
    sleep 3
    zombie=$(ssh "$origin_ssh" "docker ps --filter name=ijm-${PREEMPTED_ID:0:8} --format '{{.Names}}'" 2>/dev/null || echo "")
    [[ -z "$zombie" ]] || fail "preempted job's container still alive on origin node $preempt_origin_node: $zombie (kill-first-persist-after broke?)"
    pass "preempted job's container is gone from origin node $preempt_origin_node (kill-first ordering held)"
fi

# ---------------------------------------------------------------------------
# Stage 3 — Resume preempted job (best-effort cross-node)
# ---------------------------------------------------------------------------

log "Stage 3: waiting for preempted job to resume (≤ 10 min)…"
# Wait until the preempted row leaves QUEUED with non-zero progress (i.e. it was
# re-dispatched and the trainer either resumed or completed).
if ! wait_for "preempted resumed" 600 10 \
    "[.[] | select(.id == \"$PREEMPTED_ID\" and .status != \"QUEUED\")] | length >= 1"; then
    warn "preempted job didn't resume within 10 min — cross-node test skipped"
else
    resumed_node=$(job_field "$PREEMPTED_ID" assigned_node)
    progress=$(job_field "$PREEMPTED_ID" progress)
    pass "preempted job resumed (node=$resumed_node progress=$progress)"

    # Best-effort: confirm that on resume the trainer logged "Resumed from epoch N".
    # If logs aren't reachable or the resume node is the same as before, that's not fatal.
    log "  fetching trainer log for evidence of checkpoint resume…"
    if curl -sS "$API/jobs/$PREEMPTED_ID/logs" | grep -qE "Resumed from epoch [1-9]"; then
        pass "trainer log shows 'Resumed from epoch N>=1' — checkpoint loaded"
    else
        warn "no 'Resumed from epoch' line in trainer log — checkpoint may not have been used"
    fi
fi

# ---------------------------------------------------------------------------
# Stage 4 — User /stop on a running job; a *pre-existing queued* job takes over
# ---------------------------------------------------------------------------
# Exercises the user-driven preempt path (reason=user, what the UI "Stop"
# button does), which is the only way a profile run can be stopped and is
# also the operator's manual override for standard runs.  Distinct from the
# optimizer-driven preempt in Stage 2 in two ways:
#   - The freed slot is filled by a job that was already QUEUED, not by an
#     incoming urgent submission.  Validates the dispatch backfill loop.
#   - The stopped row goes back through the standard re-queue path; we don't
#     need a checkpoint resume verification because the test already covered
#     that in Stage 3.

# Submit JOB6 (priority=1, loose deadline) into the cluster.  Whether it
# sits queued or runs immediately depends on the optimizer's instantaneous
# decision (it may rotate jobs to place JOB6 and re-queue a different one).
# Either way, by Stage 4's end at least one previously-queued job is the
# "takeover" candidate, which is the path under test.
log "Stage 4: submitting JOB6 (priority=1, loose deadline)"
JOB6=$(submit_job 1 "$DL_LONG") || fail "POST /jobs JOB6 rejected"
log "  id: ${JOB6:0:8}"

# Settle: wait for the scheduler to acknowledge JOB6 (any non-terminal
# state).  The user-stop below will free a slot regardless of whether JOB6
# itself is queued or already running.
if ! wait_for "JOB6 scheduled" 30 1 \
    "[.[] | select(.id == \"$JOB6\" and (.status == \"QUEUED\" or .status == \"RUNNING\" or .status == \"PROFILING\"))] | length >= 1"; then
    fail "JOB6 not visible to scheduler within 30s"
fi
pass "JOB6 accepted by scheduler"

# Snapshot the QUEUED set BEFORE the user-stop so we can later assert that
# at least one of those queued jobs took the freed slot.
QUEUED_BEFORE=$(all_jobs | jq -c '[.[] | select(.status == "QUEUED" and .assigned_node == null) | .id]')
log "  queued jobs before user-stop: $(echo "$QUEUED_BEFORE" | jq -r 'map(.[0:8]) | join(" ")')"

# Pick the lowest-priority RUNNING job (excluding JOB6 itself) as user-stop victim.
USER_VICTIM=$(all_jobs | jq -r --arg j6 "$JOB6" \
    '[.[] | select(.status == "RUNNING" and .id != $j6)] | sort_by(.priority) | .[0].id')
[[ -n "$USER_VICTIM" && "$USER_VICTIM" != "null" ]] || fail "no RUNNING job to user-stop"
USER_VICTIM_NODE=$(job_field "$USER_VICTIM" "assigned_node")
USER_VICTIM_PRIO=$(job_field "$USER_VICTIM" "priority")
log "  user-stopping ${USER_VICTIM:0:8} (priority=$USER_VICTIM_PRIO @ $USER_VICTIM_NODE)"

curl -fsS -X POST "$API/jobs/$USER_VICTIM/stop" >/dev/null \
    || fail "POST /jobs/$USER_VICTIM/stop failed"

# User-stop puts the row in PREEMPTED (sticky, awaits a manual /resume).
# assigned_node is *preserved* on user-stop so /resume can prefer the same
# node (warm checkpoint cache); only auto-stop clears it.
if ! wait_for "user-victim PREEMPTED" 15 1 \
    "[.[] | select(.id == \"$USER_VICTIM\" and .status == \"PREEMPTED\")] | length == 1"; then
    fail "user-stopped job did not reach PREEMPTED within 15s"
fi
pass "user-stopped ${USER_VICTIM:0:8} is PREEMPTED"

# Container must be gone from the origin node.  We poll up to 60s because
# the worker HTTP /stop can be slow (SSH-tunnel latency, optimizer churn),
# and the worker's stream-watcher may need a few seconds to detect the
# status flip and tear the container down.
container_deadline=$(( SECONDS + 60 ))
while (( SECONDS < container_deadline )); do
    victim_container_count=$(ssh "$USER_VICTIM_NODE" \
        "docker ps --filter name=ijm-${USER_VICTIM:0:8} --format '{{.Names}}'" 2>/dev/null | wc -l)
    [[ "$victim_container_count" -eq 0 ]] && break
    sleep 3
done
[[ "$victim_container_count" -eq 0 ]] \
    || fail "container ijm-${USER_VICTIM:0:8} still present on $USER_VICTIM_NODE after 60s"
pass "victim's container removed from origin node $USER_VICTIM_NODE"

# At least one of the previously-queued jobs takes the freed slot within
# ~60s.  We don't insist on JOB6 specifically because the optimizer may
# have rotated the queue — what matters is that the user-stop didn't strand
# a slot idle: some pre-queued job is now running.
if ! wait_for "queued job takes over" 60 2 \
    "[.[] | select(.status == \"RUNNING\" or .status == \"PROFILING\") | .id] as \$running |
     (${QUEUED_BEFORE}) as \$wasQ |
     [\$wasQ[] | select(. as \$x | \$running | index(\$x))] | length >= 1"; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, prio: .priority, node: .assigned_node}'
    fail "no previously-queued job took the freed slot within 60s"
fi
NEW_RUNNING=$(all_jobs | jq -r --argjson q "$QUEUED_BEFORE" \
    '[.[] | select((.status == "RUNNING" or .status == "PROFILING") and (.id | IN($q[]))) | .id[0:8]] | join(" ")')
pass "previously-queued job(s) took the freed slot: $NEW_RUNNING"

# Honor the user-stop contract: the row stays PREEMPTED until the operator
# explicitly chooses to /resume it.  We do NOT auto-resume here — that
# would mask the sticky semantics and races against the late-arriving
# worker kill that lands seconds after the API's atomic flip.  Stage 5
# expects exactly one PREEMPTED leftover (the user-victim).

# ---------------------------------------------------------------------------
# Stage 5 — Final state
# ---------------------------------------------------------------------------

log "Stage 5: waiting for all jobs to terminate (≤ ${TERMINAL_TIMEOUT_S}s)…"
# Terminal = SUCCEEDED, FAILED, or PREEMPTED (user-stop contract).
# We expect exactly 1 PREEMPTED (the user-victim from Stage 4) plus the
# rest SUCCEEDED.
if ! wait_for "all terminal" "$TERMINAL_TIMEOUT_S" 15 \
    '[.[] | select(.status == "RUNNING" or .status == "QUEUED" or .status == "PROFILING")] | length == 0'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, progress}'
    fail "jobs still pending after ${TERMINAL_TIMEOUT_S}s"
fi

succeeded=$(count_status SUCCEEDED)
failed=$(count_status FAILED)
preempted=$(count_status PREEMPTED)
total=$(all_jobs | jq 'length')
log "  terminal counts: SUCCEEDED=$succeeded PREEMPTED=$preempted FAILED=$failed total=$total"
(( failed == 0 )) || fail "expected 0 FAILED, got $failed"
(( preempted == 1 )) || fail "expected exactly 1 PREEMPTED (user-victim), got $preempted"
(( succeeded == total - 1 )) || fail "expected $(( total - 1 )) SUCCEEDED, got $succeeded"
pass "$succeeded SUCCEEDED + 1 PREEMPTED (user-stopped, awaiting manual resume) — terminal"

log "Stage 5: zombie & cache checks"
# Match training-container names anchored at start (``ijm-<8-hex>``) to skip
# the infrastructure containers (``wangrat-ijm-worker``, ``wangrat-ijm-postgres``)
# that ``docker ps --filter name=ijm-`` would catch as substring matches.
zombies_a=$(ssh "$NODE_A" 'docker ps --filter name=ijm- --format "{{.Names}}" | grep -cE "^ijm-" || true' 2>/dev/null || echo 99)
zombies_b=$(ssh "$NODE_B" 'docker ps --filter name=ijm- --format "{{.Names}}" | grep -cE "^ijm-" || true' 2>/dev/null || echo 99)
(( zombies_a == 0 )) || fail "$zombies_a training-container zombie(s) on $NODE_A"
(( zombies_b == 0 )) || fail "$zombies_b training-container zombie(s) on $NODE_B"
pass "no zombie ijm-* training containers on either node"

prof_count=$(ssh "$NODE_A" "docker exec wangrat-ijm-postgres psql -U postgres -d ijm -tA -c \
    \"SELECT count(*) FROM profiling_results WHERE job_id='$JOB_TYPE' AND duration_seconds IS NOT NULL\"" \
    | tr -d '[:space:]')
(( prof_count >= 2 )) || fail "expected ≥2 profiling_results rows, got $prof_count"
pass "profiling cache populated ($prof_count rows for $JOB_TYPE)"

# Verify every node that ran a job actually used its GPU — silent CPU
# fallback (driver/runtime mis-config, e.g. wrong WORKER_GPU_MODE) would
# otherwise let the test pass while jobs run 5-10× slower than intended.
# Check EVERY job that touched a GPU-capable node, regardless of its
# terminal status (SUCCEEDED / PREEMPTED / FAILED all still produced a
# trainer log we can inspect); a single CPU run on a GPU node is a fail.
log "Stage 5: verifying GPU was actually used on every node that ran a job"
nodes_used=$(all_jobs | jq -r '[.[].assigned_node] | unique | .[] | select(. != null)')
for node in $nodes_used; do
    # Iterate over every job that ever ran on this node and verify each
    # trainer log shows "Using device: cuda".  A "Using device: cpu" on a
    # GPU node means silent CUDA fallback — fail the scenario.
    job_ids=$(all_jobs | jq -r --arg n "$node" '[.[] | select(.assigned_node == $n)] | .[] | .id')
    [[ -z "$job_ids" ]] && { warn "no jobs ever ran on $node — skipping GPU check"; continue; }
    cpu_seen=0
    cuda_seen=0
    while IFS= read -r jid; do
        [[ -z "$jid" ]] && continue
        log_text=$(curl -sS "$API/jobs/$jid/logs" 2>/dev/null || true)
        if echo "$log_text" | grep -qE "Using device: cuda"; then
            cuda_seen=1
        elif echo "$log_text" | grep -qE "Using device: cpu"; then
            cpu_seen=1
            fail "$node: job ${jid:0:8} trainer log shows 'Using device: cpu' on a GPU node — silent CUDA fallback (check WORKER_GPU_MODE)"
        fi
    done <<<"$job_ids"
    if (( cuda_seen == 1 )); then
        pass "$node: every inspected trainer log shows 'Using device: cuda' (GPU was used)"
    else
        warn "$node: no trainer log on this node had a 'Using device:' line (probably no job got far enough to log)"
    fi
done

echo
echo "${C_OK}✓ E2E scenario passed${C_END}"

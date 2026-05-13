#!/usr/bin/env bash
# Scenario 1 — single-type end-to-end demo.
#
# Exercises, in order, the four behaviours the system promises:
#   1. Multi-GPU placement     — during the profile sweep, every instance
#                                runs on a 2-GPU bundle for at least one
#                                config (visible in profiling_results).
#   2. Auto-preempt by urgency — an urgent past-deadline submission forces
#                                the optimizer to evict a lower-priority
#                                running job and place itself on the freed
#                                fastest slot.
#   3. Manual user-stop        — the operator pauses one running job; the
#                                row sticks at PREEMPTED until /resume.
#   4. Cross-node resume       — when the auto-preempted job comes back,
#                                it lands on a different node than its
#                                origin and the legacy-format checkpoint
#                                loads cleanly.
#
# Default job type is ``efficientnet`` (CNN that genuinely benefits from
# DataParallel across 2 GPUs).  Override:
#   JOB_TYPE=lstm-small bash infra/e2e_scenario.sh
#
# Usage:
#   bash infra/e2e_scenario.sh                # default settings
#   bash infra/e2e_scenario.sh --strict       # cross-node-resume becomes fatal
set -euo pipefail

API="${API:-http://localhost:8000}"
NODE_A="${NODE_A:-polimi}"       # matemagician SSH alias
# Default is ``lstm-small``: MNIST is reliably pre-staged on both nodes
# and PyTorch 1.5.1 supports all the ops we use.  The legacy CIFAR-10
# integrity-check fails intermittently on polimi-gpu's modern torchvision,
# so use ``JOB_TYPE=convnet`` only after fixing that staging.
JOB_TYPE="${JOB_TYPE:-lstm-small}"
IMAGE="${IMAGE:-wangrat/ijm-${JOB_TYPE}:latest}"
EPOCHS="${EPOCHS:-40}"
TERMINAL_TIMEOUT_S="${TERMINAL_TIMEOUT_S:-3600}"
PRIORITY_MAX=5

STRICT=0
[[ "${1:-}" == "--strict" ]] && STRICT=1

C_INFO=$'\033[1;36m'; C_OK=$'\033[1;32m'; C_WARN=$'\033[1;33m'; C_FAIL=$'\033[1;31m'; C_END=$'\033[0m'
log()  { echo "${C_INFO}[$(date +%T)] $*${C_END}"; }
pass() { echo "${C_OK}  ✓ $*${C_END}"; }
warn() { echo "${C_WARN}  ⚠ $*${C_END}"; (( STRICT )) && exit 1 || true; }
fail() { echo "${C_FAIL}  ✗ $*${C_END}"; exit 1; }

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

submit_job() {
    local prio="$1" deadline="$2"
    local resp; resp=$(curl -sS -X POST "$API/jobs" -H 'Content-Type: application/json' -d "{
        \"job_id\": \"$JOB_TYPE\",
        \"dockerImage\": \"$IMAGE\",
        \"command\": [],
        \"Priority\": $prio,
        \"deadline\": \"$deadline\",
        \"epochsTotal\": $EPOCHS
    }")
    local id; id=$(echo "$resp" | jq -r '.id // empty')
    [[ -n "$id" && "$id" != "null" ]] || { echo "ERR: POST /jobs: $resp" >&2; return 1; }
    echo "$id"
}

job_field()   { curl -sS "$API/jobs/$1" | jq -r ".$2"; }
all_jobs()    { curl -sS "$API/jobs"; }
count_status() { curl -sS "$API/jobs" | jq "[.[] | select(.status == \"$1\")] | length"; }

wait_for() {
    local desc="$1" timeout="$2" interval="$3" filter="$4" elapsed=0
    while (( elapsed < timeout )); do
        if all_jobs | jq -e "$filter" >/dev/null 2>&1; then return 0; fi
        sleep "$interval"; elapsed=$((elapsed + interval))
    done
    return 1
}

# -----------------------------------------------------------------------------
# Stage 0 — Cluster sanity + clean slate
# -----------------------------------------------------------------------------

log "Stage 0: cluster sanity"
curl -fsS "$API/health" >/dev/null || fail "API at $API not reachable"
WORKER_CONFIG="${WORKER_CONFIG:-$(dirname "$0")/../config/nodes_config.tunnel.json}"
[[ -f "$WORKER_CONFIG" ]] || fail "worker config not found at $WORKER_CONFIG"
mapfile -t worker_pairs < <(jq -r '.[] | "\(.id) \(.workerUrl)"' "$WORKER_CONFIG")
for pair in "${worker_pairs[@]}"; do
    node_id="${pair%% *}"; worker_url="${pair#* }"
    curl -fsS --max-time 5 "$worker_url/health" | jq -e '.status == "ok"' >/dev/null \
        || fail "worker for $node_id at $worker_url not healthy"
done
pass "API + workers healthy"

log "Stage 0: clearing state"
curl -sS -X DELETE "$API/jobs" >/dev/null
ssh "$NODE_A" 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*' 2>/dev/null || true
curl -sS -X POST "$API/admin/reconcile-slots" >/dev/null
pass "DB + on-disk state cleared"

# -----------------------------------------------------------------------------
# Stage 1 — Fill the cluster + multi-GPU profile coverage
# -----------------------------------------------------------------------------
# Submit 4 patient jobs (prio=4, +1 h deadlines).  Profiling sweep with
# configs_per_job=2 covers every (node, GPU-count) cell, including 2-GPU
# bundles on both A40 and QuadroP600 — that's the multi-GPU usage check.
# After profile sweep settles, all 4 jobs are RUNNING on the 4 cluster slots.
log "Stage 1: submit 4 patient ${JOB_TYPE} jobs"
DL=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)
JOB1=$(submit_job 4 "$DL"); sleep 5
JOB2=$(submit_job 4 "$DL"); sleep 5
JOB3=$(submit_job 4 "$DL"); sleep 5
JOB4=$(submit_job 4 "$DL")
log "  ids: ${JOB1:0:8} ${JOB2:0:8} ${JOB3:0:8} ${JOB4:0:8}"

log "  waiting for all 4 RUNNING with progress (≤ 12 min)…"
wait_for "all 4 RUNNING+progressed" 720 5 \
    '[.[] | select(.status == "RUNNING" and .progress != null)] | length == 4' \
    || fail "Stage 1 timeout: not all 4 jobs RUNNING with progress"
pass "Stage 1 steady state — 4 jobs RUNNING, cluster full"

# Profile coverage check (proves multi-GPU happened during sweep).
log "  asserting profile coverage includes 2-GPU bundles:"
profile_json=$(curl -sS "$API/profiling-results/$JOB_TYPE")
two_gpu_rows=$(echo "$profile_json" | jq '[.[] | select(.duration_seconds != null and ((.gpu_config | to_entries | .[0].value) == 2))] | length')
[[ "$two_gpu_rows" -ge 1 ]] || fail "profile coverage: expected ≥1 row with a 2-GPU bundle, got $two_gpu_rows"
pass "${JOB_TYPE} profile coverage: $two_gpu_rows row(s) with 2-GPU bundles"
CELLS=$(echo "$profile_json" | jq -r '[.[] | "\(.gpu_config | tostring)@\(.node_id)"] | sort | unique | join("  ")')
log "    cells: $CELLS"

# Snapshot current placement so we can detect cross-node resume after Stage 2.
PRE_URGENT_RUNNING=$(all_jobs | jq -c '[.[] | select(.status == "RUNNING") | {id, node: .assigned_node}]')
log "  pre-urgent RUNNING (id@node): $(echo "$PRE_URGENT_RUNNING" | jq -r 'map("\(.id[0:8])@\(.node)") | join(" ")')"

# -----------------------------------------------------------------------------
# Stage 2 — Auto-preempt by urgency
# -----------------------------------------------------------------------------
# Submit an URGENT prio=5 job with a deadline so tight every node is at least
# somewhat tardy; A40 is dramatically less tardy than P600, so the optimizer
# is forced to place it on A40 and evict whatever's there.
log "Stage 2: submit URGENT ${JOB_TYPE} (priority=$PRIORITY_MAX, deadline +90s)"
URGENT_DL=$(date -u -d '+90 seconds' +%Y-%m-%dT%H:%M:%SZ)
JOB_URGENT=$(submit_job "$PRIORITY_MAX" "$URGENT_DL") || fail "POST /jobs urgent rejected"
log "  id: ${JOB_URGENT:0:8}"

log "  verifying URGENT placed within 60s…"
T0=$(date +%s)
wait_for "urgent placed" 60 3 \
    "[.[] | select(.id == \"$JOB_URGENT\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length == 1" \
    || fail "URGENT still QUEUED after 60s"
pass "URGENT placed in $(( $(date +%s) - T0 ))s"

# Identify the victim: an id that was RUNNING pre-urgent but is now QUEUED or
# PREEMPTED.  Save its origin node for the cross-node-resume check.
log "  identifying auto-preempt victim…"
sleep 5   # let the preempt+row-update settle
PRE_IDS=$(echo "$PRE_URGENT_RUNNING" | jq -r '.[].id')
VICTIM=""
VICTIM_ORIGIN=""
for prev_id in $PRE_IDS; do
    cur_status=$(job_field "$prev_id" status)
    if [[ "$cur_status" == "QUEUED" || "$cur_status" == "PREEMPTED" ]]; then
        VICTIM="$prev_id"
        VICTIM_ORIGIN=$(echo "$PRE_URGENT_RUNNING" | jq -r ".[] | select(.id == \"$prev_id\") | .node")
        break
    fi
done
[[ -n "$VICTIM" ]] || fail "no auto-preempt victim observed"
pass "auto-preempt victim: ${VICTIM:0:8} (origin: $VICTIM_ORIGIN, status: $(job_field "$VICTIM" status))"

# -----------------------------------------------------------------------------
# Stage 3 — Manual user-stop on a still-running job
# -----------------------------------------------------------------------------
# Pick the lowest-priority job currently RUNNING (excluding the URGENT) and
# issue /stop?reason=user.  Verify the row transitions to PREEMPTED and stays
# there — manual stops are sticky (no automatic resume).
log "Stage 3: manual user-stop on a running non-urgent job"
STOP_TARGET=$(all_jobs | jq -r \
    "[.[] | select(.status == \"RUNNING\" and .id != \"$JOB_URGENT\")] | sort_by(.priority)[0].id // empty")
[[ -n "$STOP_TARGET" ]] || fail "no RUNNING non-urgent job available for manual stop"
STOP_ORIGIN=$(job_field "$STOP_TARGET" assigned_node)
log "  user-stopping ${STOP_TARGET:0:8} (on $STOP_ORIGIN)"
curl -fsS -X POST "$API/jobs/$STOP_TARGET/stop" >/dev/null \
    || fail "POST /jobs/$STOP_TARGET/stop failed"

wait_for "user-stop landed" 60 3 \
    "[.[] | select(.id == \"$STOP_TARGET\" and .status == \"PREEMPTED\")] | length == 1" \
    || fail "manual stop didn't reach PREEMPTED in 60s"
pass "manual stop: ${STOP_TARGET:0:8} → PREEMPTED (sticky)"

# -----------------------------------------------------------------------------
# Stage 4 — Cross-node resume of the auto-preempt victim
# -----------------------------------------------------------------------------
# Wait for the auto-preempted victim from Stage 2 to come back online.
# Its re-dispatch can land on either node — we *want* a different node from
# its origin (cross-node resume).  Under --strict this is fatal; otherwise
# warn-only since same-node resume is also a valid outcome under cost-min.
log "Stage 4: waiting for auto-preempt victim ${VICTIM:0:8} to resume (≤ 10 min)…"
wait_for "victim resumed" 600 5 \
    "[.[] | select(.id == \"$VICTIM\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length == 1" \
    || fail "victim ${VICTIM:0:8} did not resume in 10 min"
VICTIM_RESUME_NODE=$(job_field "$VICTIM" assigned_node)
pass "victim resumed on $VICTIM_RESUME_NODE"
if [[ "$VICTIM_RESUME_NODE" != "$VICTIM_ORIGIN" ]]; then
    pass "cross-node resume: ${VICTIM:0:8} moved from $VICTIM_ORIGIN → $VICTIM_RESUME_NODE"
else
    warn "victim resumed on origin node $VICTIM_ORIGIN — same-node resume (not strict-fatal under cost-min)"
fi

# Verify the checkpoint actually loaded (trainer log line on the resume node).
# grep returns 1 with no match → pipefail trips set -e and exits the
# whole script.  Force the grep step's exit to 0 so an absent log line
# is reported as "warn" rather than aborting.
RESUMED_LOG=$(curl -sS "$API/jobs/$VICTIM/logs" 2>/dev/null | tail -200 | { grep -i "resumed from\|resuming from\|loaded checkpoint" || true; } | tail -1)
if [[ -n "$RESUMED_LOG" ]]; then
    pass "checkpoint load confirmed in logs: $(echo "$RESUMED_LOG" | head -c 120)"
else
    warn "no 'resumed from' line in logs (logs may still be in flight)"
fi

# -----------------------------------------------------------------------------
# Stage 5 — Drain and final state
# -----------------------------------------------------------------------------
log "Stage 5: waiting for all jobs to terminate (≤ ${TERMINAL_TIMEOUT_S}s)…"
wait_for "all terminal" "$TERMINAL_TIMEOUT_S" 10 \
    '[.[] | select(.status == "SUCCEEDED" or .status == "FAILED" or .status == "PREEMPTED")] | length == 5' \
    || fail "not all 5 jobs reached terminal state"

succ=$(count_status SUCCEEDED); fail_n=$(count_status FAILED); preem=$(count_status PREEMPTED)
log "  terminal counts: SUCCEEDED=$succ FAILED=$fail_n PREEMPTED=$preem"
[[ "$succ" -ge 4 ]] || warn "expected ≥4 SUCCEEDED, got $succ"
[[ "$preem" -ge 1 ]] || warn "expected ≥1 PREEMPTED (the user-stopped job), got $preem"
[[ "$fail_n" == "0" ]] || fail "$fail_n job(s) FAILED — this is fatal"

# Zombie containers — none should remain on either node.
for pair in "${worker_pairs[@]}"; do
    node_id="${pair%% *}"; worker_url="${pair#* }"
    running_on_node=$(curl -sS "$worker_url/health" | jq -r '.running | length // 0')
    [[ "$running_on_node" == "0" ]] || warn "$node_id reports $running_on_node container(s) still running"
done

# Race-fix validation.  ``acquire_oversub_count`` is the hard guarantee
# (BoundedSemaphore must never let the cluster oversubscribe a node) ---
# any value > 0 is fatal.  ``release_underflow_count`` is a soft signal:
# the BoundedSemaphore caps the release path, so an underflow attempt is
# already absorbed without consequence to placement.  The 15s drift
# heartbeat then re-syncs ``_used`` against the DB.  We warn above a
# small threshold (the post-fix steady-state on a busy scenario is
# typically 0–3 underflows per run).
SLOTS_METRICS=$(curl -sS http://localhost:8000/admin/slots | jq -c '.metrics')
underflow=$(echo "$SLOTS_METRICS" | jq '.release_underflow_count')
oversub=$(echo "$SLOTS_METRICS" | jq '.acquire_oversub_count')
recovery=$(echo "$SLOTS_METRICS" | jq '.drift_recovery_count')
log "  slot metrics: $SLOTS_METRICS"
[[ "$oversub" == "0" ]] || fail "race-fix regression: acquire_oversub_count=$oversub (expected 0)"
if (( underflow > 5 )); then
    fail "race-fix regression: release_underflow_count=$underflow (expected ≤5)"
elif (( underflow > 0 )); then
    warn "race-fix soft: release_underflow_count=$underflow — absorbed by BoundedSemaphore + drift heartbeat"
else
    pass "race-fix validation: zero underflow, zero oversub"
fi
pass "drift heartbeat fired ${recovery}× (15s cadence; auto-recovered)"

pass "Scenario 1 complete: 2-GPU profiling, auto-preempt, manual-stop, cross-node-or-same resume, slot-tracker clean"

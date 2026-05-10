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
EPOCHS="${EPOCHS:-20}"
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
ssh -o ConnectTimeout=5 "$NODE_A" "docker ps --filter name=wangrat-ijm-worker --format '{{.Status}}' | grep -q Up" \
    || fail "worker on $NODE_A not running"
ssh -o ConnectTimeout=5 "$NODE_B" "docker ps --filter name=wangrat-ijm-worker --format '{{.Status}}' | grep -q Up" \
    || fail "worker on $NODE_B not running"
pass "API healthy, both workers up"

log "Stage 0: clearing state"
curl -sS -X DELETE "$API/jobs" >/dev/null
ssh "$NODE_A" 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*' 2>/dev/null || true
[[ "$(count_status RUNNING)" == "0" ]] || warn "some jobs still RUNNING after Clear All — will continue"
pass "DB cleared, on-disk state wiped"

# ---------------------------------------------------------------------------
# Stage 1 — Four patient jobs
# ---------------------------------------------------------------------------

log "Stage 1: submitting 4 patient jobs (deadline +2h)"
DEADLINE=$(date -u -d '+2 hours' +%Y-%m-%dT%H:%M:%SZ)
JOB1=$(submit_job 1 "$DEADLINE")
JOB2=$(submit_job 2 "$DEADLINE")
JOB3=$(submit_job 3 "$DEADLINE")
JOB4=$(submit_job 4 "$DEADLINE")
log "  ids: 1=${JOB1:0:8} 2=${JOB2:0:8} 3=${JOB3:0:8} 4=${JOB4:0:8}"

# Wait for the cluster to fill: every job has been placed at least once
# (status is no longer QUEUED — it's PROFILING/RUNNING or already terminal
# for fast A40 jobs that complete before slow QuadroP600 profiling finishes).
# Profiling is up to ~90s on QuadroP600, so allow generous time.
log "  waiting for all 4 to leave QUEUED (≤ 5 min)…"
if ! wait_for "no QUEUED" 300 5 '[.[] | select(.status == "QUEUED")] | length == 0'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, priority, node: .assigned_node}'
    fail "some jobs still QUEUED after 5 min — profiling stuck or scheduler not placing"
fi
pass "all 4 jobs placed (none in QUEUED)"

# Stage 2 needs at least one currently-RUNNING job to preempt.  If they
# all finished already (very short EPOCHS), skip the urgent-job stages.
running_count=$(all_jobs | jq '[.[] | select(.status == "RUNNING" or .status == "PROFILING")] | length')
log "  current cluster: $running_count active (RUNNING/PROFILING)"

# Sanity: 2 per node
nodes=$(all_jobs | jq -r '[.[] | select(.status == "RUNNING") | .assigned_node] | sort | unique | length')
[[ "$nodes" == "2" ]] || warn "expected jobs spread across 2 nodes, got $nodes distinct"

# ---------------------------------------------------------------------------
# Stage 2 — Urgent past-deadline job + NOTIFY-driven optimizer kick-in
# ---------------------------------------------------------------------------

log "Stage 2: capturing pre-submit RUNNING set"
# Snapshot the IDs that are RUNNING before we submit job 5 so we can detect
# which one(s) get preempted regardless of their current ``progress`` value.
RUNNING_BEFORE=$(all_jobs | jq -r '[.[] | select(.status == "RUNNING") | .id] | join(" ")')
log "  pre-submit RUNNING ids: $(echo "$RUNNING_BEFORE" | sed 's/[a-f0-9-]\{36\}/&\n/g' | awk '{print substr($0,1,8)}' | tr '\n' ' ')"

log "Stage 2: submitting urgent past-deadline job (priority=$PRIORITY_MAX)"
PAST=$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ)
JOB5=$(submit_job "$PRIORITY_MAX" "$PAST") || fail "POST /jobs rejected the urgent job"
log "  id: ${JOB5:0:8}"

log "  verifying optimizer kicks in within 20s (NOTIFY path, not 60s watcher)…"
T0=$(date +%s)
if ! wait_for "job5 placed" 20 1 \
    "[.[] | select(.id == \"$JOB5\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length >= 1"; then
    s=$(job_field "$JOB5" status)
    fail "job 5 still status=$s after 20s — NOTIFY-driven optimizer didn't fire (or preempt round-trip too slow)"
fi
ELAPSED=$(( $(date +%s) - T0 ))
pass "job 5 placed in ${ELAPSED}s (NOTIFY working)"

log "  verifying preemption: 1+ of the previously-RUNNING jobs is now QUEUED+unassigned"
preempted_ids=$(all_jobs | jq -r --argjson running_before "$(echo "$RUNNING_BEFORE" | jq -R 'split(" ")')" \
    '[.[] | select(.status == "QUEUED" and .assigned_node == null and (.id as $i | $running_before | index($i)))] | .[].id')
if [[ -z "$preempted_ids" ]]; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, priority, node: .assigned_node, progress}'
    fail "no previously-RUNNING job is now QUEUED+unassigned — preemption didn't happen"
fi
PREEMPTED_ID=$(echo "$preempted_ids" | head -1)
preempted_n=$(echo "$preempted_ids" | wc -l)
pass "$preempted_n job(s) preempted (id=${PREEMPTED_ID:0:8})"

# Soft check: did the preempted job have time to record progress?  If yes,
# the checkpoint-preservation path is exercised.  If no (preempt fired before
# epoch 1 finished), we don't fail — fresh-start preempt is still valid.
preempted_progress=$(job_field "$PREEMPTED_ID" progress)
if [[ "$preempted_progress" != "null" && -n "$preempted_progress" ]]; then
    pass "preempted job had progress=$preempted_progress at preempt time (checkpoint will be restored)"
else
    warn "preempted job had no recorded progress yet — Stage 3 resume won't validate checkpoint reuse"
fi

# Confirm worker-side state matches DB: no zombie container for the preempted job
sleep 3
zombie=$(ssh "$NODE_A" "docker ps --filter name=ijm-${PREEMPTED_ID:0:8} --format '{{.Names}}'" 2>/dev/null || echo "")
zombie+=$(ssh "$NODE_B" "docker ps --filter name=ijm-${PREEMPTED_ID:0:8} --format '{{.Names}}'" 2>/dev/null || echo "")
[[ -z "$zombie" ]] || fail "preempted job's container still alive: $zombie (kill-first-persist-after broke?)"
pass "preempted job's container is dead (kill-first ordering held)"

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
# Stage 4 — Final state
# ---------------------------------------------------------------------------

log "Stage 5: waiting for all 5 jobs to terminate (≤ ${TERMINAL_TIMEOUT_S}s)…"
if ! wait_for "all SUCCEEDED" "$TERMINAL_TIMEOUT_S" 15 \
    '[.[] | select(.status == "RUNNING" or .status == "QUEUED" or .status == "PROFILING")] | length == 0'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, progress}'
    fail "jobs still pending after ${TERMINAL_TIMEOUT_S}s"
fi

succeeded=$(count_status SUCCEEDED)
failed=$(count_status FAILED)
total=$(all_jobs | jq 'length')
log "  terminal counts: SUCCEEDED=$succeeded FAILED=$failed total=$total"
(( succeeded == 5 )) || fail "expected 5 SUCCEEDED, got $succeeded (FAILED=$failed)"
pass "all 5 jobs SUCCEEDED"

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

echo
echo "${C_OK}✓ E2E scenario passed${C_END}"

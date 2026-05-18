#!/usr/bin/env bash
# Scenario 3 — preempt-for-profile with a brand-new (unprofiled) job type.
#
# Demonstrates the system's "profiling always wins" policy: when a job is
# submitted whose type has NEVER been profiled, and every cluster slot is
# busy, the profiling scheduler proactively evicts the lowest-priority
# RUNNING job to free a slot for the profile run.  After the profile
# completes the evicted job is restored from its checkpoint.
#
# Job types:
#   - lstm-small / cnn_big (already profiled — used as cluster fillers)
#   - convnet              (NEVER profiled — drives the preempt-for-profile path)
#
# Expects: lstm-small + cnn_big profile rows present; convnet profile rows
# absent.  This script DELETES any pre-existing convnet profile rows in
# Stage 0 to make the scenario rerunnable.
#
# Usage:
#   bash infra/e2e_scenario_unprofiled.sh
set -euo pipefail

API="${API:-http://localhost:8000}"
NODE_A="${NODE_A:-polimi}"
NODE_B="${NODE_B:-polimi-gpu}"
EPOCHS_SMALL="${EPOCHS_SMALL:-400}"
EPOCHS_BIG="${EPOCHS_BIG:-40}"
TERMINAL_TIMEOUT_S="${TERMINAL_TIMEOUT_S:-3600}"

C_INFO=$'\033[1;36m'; C_OK=$'\033[1;32m'; C_WARN=$'\033[1;33m'; C_FAIL=$'\033[1;31m'; C_END=$'\033[0m'
log()  { echo "${C_INFO}[$(date +%T)] $*${C_END}"; }
pass() { echo "${C_OK}  ✓ $*${C_END}"; }
warn() { echo "${C_WARN}  ⚠ $*${C_END}"; }
fail() { echo "${C_FAIL}  ✗ $*${C_END}"; exit 1; }

submit_job() {
    local jt="$1" prio="$2" deadline="$3" epochs="$4"
    local resp
    resp=$(curl -sS -X POST "$API/jobs" -H 'Content-Type: application/json' -d "{
        \"job_id\": \"$jt\",
        \"dockerImage\": \"wangrat/ijm-$jt:latest\",
        \"command\": [],
        \"Priority\": $prio,
        \"deadline\": \"$deadline\",
        \"epochsTotal\": $epochs
    }")
    local id; id=$(echo "$resp" | jq -r '.id // empty')
    [[ -n "$id" && "$id" != "null" ]] || { echo "ERR: POST /jobs($jt) failed: $resp" >&2; return 1; }
    echo "$id"
}

job_field()    { curl -sS "$API/jobs/$1" | jq -r ".$2"; }
all_jobs()     { curl -sS "$API/jobs"; }
count_status() { curl -sS "$API/jobs" | jq "[.[] | select(.status == \"$1\")] | length"; }

wait_for() {
    local desc="$1" timeout="$2" interval="$3" filter="$4" elapsed=0
    while (( elapsed < timeout )); do
        if all_jobs | jq -e "$filter" >/dev/null 2>&1; then return 0; fi
        sleep "$interval"; elapsed=$((elapsed + interval))
    done
    return 1
}

# ---------------------------------------------------------------------------
# Stage 0 — Setup; ensure convnet is unprofiled, other types are profiled
# ---------------------------------------------------------------------------

log "Stage 0: cluster sanity + clean slate"
curl -fsS "$API/health" >/dev/null || fail "API at $API not reachable"
curl -sS -X DELETE "$API/jobs" >/dev/null
ssh "$NODE_A" 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*' 2>/dev/null || true
curl -sS -X POST "$API/admin/reconcile-slots" >/dev/null
ssh "$NODE_A" 'docker exec wangrat-ijm-postgres psql -U postgres -d ijm -c "DELETE FROM profiling_results WHERE job_id='"'"'convnet'"'"';"' >/dev/null
pass "state cleared, convnet profile rows wiped"

# Sanity: lstm-small and cnn_big must already be profiled (the previous
# scenarios populate this).  Otherwise Stage 1's filler jobs would also
# trigger profile runs and there'd be no clean "cluster full of RUNNING
# standard jobs" state to test the preempt-for-profile path.
for t in lstm-small cnn_big; do
    got=$(curl -sS "$API/profiling-results/$t" | jq '[.[] | select(.duration_seconds != null)] | length')
    (( got >= 2 )) || fail "$t has only $got profile rows — run scenarios 1+2 first"
done
got_conv=$(curl -sS "$API/profiling-results/convnet" | jq 'length')
(( got_conv == 0 )) || fail "convnet still has $got_conv profile rows — Stage 0 wipe failed"
pass "lstm-small + cnn_big profiled; convnet unprofiled (correct precondition)"

# ---------------------------------------------------------------------------
# Stage 1 — Fill the cluster with 4 patient pre-profiled jobs
# ---------------------------------------------------------------------------
#
# Mix small + big so both nodes (A40 + P600) are utilised:
#   JOB1  lstm-small  priority 1  ← lowest priority → preempt target
#   JOB2  cnn_big     priority 1  ← lowest priority → preempt target
#   JOB3  lstm-small  priority 4  ← protected
#   JOB4  cnn_big     priority 4  ← protected
#
log "Stage 1: 4 patient pre-profiled jobs to fill the cluster"
DL_LOOSE=$(date -u -d '+8 hours'  +%Y-%m-%dT%H:%M:%SZ)
DL_MED=$(date  -u -d '+2 hours'   +%Y-%m-%dT%H:%M:%SZ)
JOB1=$(submit_job lstm-small 1 "$DL_LOOSE" "$EPOCHS_SMALL"); sleep 4
JOB2=$(submit_job cnn_big    1 "$DL_LOOSE" "$EPOCHS_BIG");   sleep 4
JOB3=$(submit_job lstm-small 4 "$DL_MED"   "$EPOCHS_SMALL"); sleep 4
JOB4=$(submit_job cnn_big    4 "$DL_MED"   "$EPOCHS_BIG")
log "  ids: ${JOB1:0:8} ${JOB2:0:8} ${JOB3:0:8} ${JOB4:0:8}"

# Pre-profiled, so no profiling sweep happens — all 4 go straight to RUNNING.
log "  waiting for cluster to be FULL (4 RUNNING, no QUEUED) — ≤ 4 min…"
if ! wait_for "cluster full" 240 5 \
    '[.[] | select(.status == "RUNNING") and (.is_profiling_run | not)] | length == 4
     and ([.[] | select(.status == "QUEUED")] | length == 0)'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], type: .job_id, status, node: .assigned_node, profile: .is_profiling_run}'
    fail "cluster never reached 4 RUNNING standard jobs"
fi
pass "cluster full: 4 RUNNING standard jobs, no profile sweep needed"

# Snapshot which job has the LOWEST priority on a profiling-capable node
# — that's the expected eviction victim.  Phase 1c picks the cheapest set
# of victims (lowest priority, least progress).  JOB1 (prio=1, lstm-small)
# and JOB2 (prio=1, cnn_big) are both candidates; whichever is on the node
# whose total capacity fits the smallest convnet profile config (A40×1 →
# polimi-gpu) is the actual victim.
PRE_RUNNING=$(all_jobs | jq -c '[.[] | select(.status == "RUNNING") | {id, type: .job_id, node: .assigned_node, priority}]')
log "  pre-submit running set:"
echo "$PRE_RUNNING" | jq -r '.[] | "    \(.id[0:8]) \(.type) prio=\(.priority) @ \(.node)"'

# ---------------------------------------------------------------------------
# Stage 2 — Submit unprofiled convnet → preempt-for-profile fires
# ---------------------------------------------------------------------------

log "Stage 2: submit UNPROFILED convnet (priority=3, loose deadline)"
# Priority 3 sits between the two patient tiers (1 and 4).  Phase 1c does
# NOT gate on the submitter's priority — it always evicts the lowest-prio
# running job on a node that fits the target profile config.  So
# priority=3 vs 1 vs 5 doesn't matter here; we keep it modest to make the
# point that profiling is sacred regardless of urgency.
CONV=$(submit_job convnet 3 "$DL_LOOSE" 5)
log "  id: ${CONV:0:8} (epochs=5: profile takes one epoch, standard run takes 4 more)"

T0=$(date +%s)
log "  waiting for Phase 1c preempt + convnet to enter PROFILING (≤ 60s)…"
if ! wait_for "convnet profiling" 60 1 \
    "[.[] | select(.id == \"$CONV\" and .status == \"PROFILING\" and .is_profiling_run == true)] | length == 1"; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], type: .job_id, status, node: .assigned_node, profile: .is_profiling_run}'
    log "  /tmp/api.log tail:"; tail -50 /tmp/api.log
    fail "convnet did NOT enter PROFILING within 60s — Phase 1c preempt failed"
fi
PREEMPT_LATENCY=$(( $(date +%s) - T0 ))
pass "convnet ${CONV:0:8} entered PROFILING in ${PREEMPT_LATENCY}s"

# Identify the evicted victim: a job that was in PRE_RUNNING but is no
# longer on its original node (status=QUEUED with cleared assigned_node,
# or migrated, or PREEMPTED).
VICTIM=""
for _ in $(seq 30); do
    cur=$(all_jobs)
    vid=$(jq -r --argjson pre "$PRE_RUNNING" '
        ($pre[]) as $p
        | (map(select(.id == $p.id)) | first) as $now
        | select($now != null
            and ( ($now.status == "QUEUED" and $now.assigned_node == null)
               or ($now.assigned_node != null and $now.assigned_node != $p.node)
               or ($now.status == "PREEMPTED" or $now.status == "FAILED") ))
        | $now.id' <<<"$cur" | head -1)
    [[ -n "$vid" ]] && { VICTIM=$vid; break; }
    sleep 1
done
[[ -n "$VICTIM" ]] || fail "no eviction observed — Phase 1c took no victim"
victim_type=$(echo "$PRE_RUNNING" | jq -r --arg p "$VICTIM" '.[] | select(.id == $p) | .type')
victim_node=$(echo "$PRE_RUNNING" | jq -r --arg p "$VICTIM" '.[] | select(.id == $p) | .node')
victim_prio=$(echo "$PRE_RUNNING" | jq -r --arg p "$VICTIM" '.[] | select(.id == $p) | .priority')
pass "victim: ${VICTIM:0:8} ($victim_type prio=$victim_prio @ $victim_node)"
(( victim_prio == 1 )) || warn "victim priority=$victim_prio (expected 1 — lowest in the running set)"

# ---------------------------------------------------------------------------
# Stage 3 — Profile completes, victim resumes, convnet standard run begins
# ---------------------------------------------------------------------------

log "Stage 3: wait for convnet profile to finish (≤ 90s; 1 epoch on A40 ≈ 6s)"
if ! wait_for "convnet profile done" 90 2 \
    "[.[] | select(.id == \"$CONV\" and .status != \"PROFILING\")] | length == 1"; then
    fail "convnet profile run did not complete within 90s"
fi
conv_status=$(job_field "$CONV" status)
pass "convnet profile complete, now status=$conv_status"

# Profile record landed
conv_profiled=$(curl -sS "$API/profiling-results/convnet" | jq '[.[] | select(.duration_seconds != null)] | length')
(( conv_profiled >= 1 )) || fail "convnet profile durations not recorded (got $conv_profiled rows)"
pass "convnet now has $conv_profiled profile measurement(s) cached"

# Victim must resume (status != QUEUED, progress != null)
log "Stage 3: wait for victim ${VICTIM:0:8} to resume (≤ 5 min)…"
if ! wait_for "victim resumed" 300 5 \
    "[.[] | select(.id == \"$VICTIM\" and .status != \"QUEUED\" and (.progress != null))] | length == 1"; then
    warn "victim ${VICTIM:0:8} did not resume in 5 min — convnet may still be holding the slot"
else
    final_node=$(job_field "$VICTIM" assigned_node)
    pass "victim resumed on $final_node"
fi

# ---------------------------------------------------------------------------
# Stage 4 — All terminate cleanly
# ---------------------------------------------------------------------------

log "Stage 4: wait for all jobs to terminate (≤ ${TERMINAL_TIMEOUT_S}s)…"
if ! wait_for "all terminal" "$TERMINAL_TIMEOUT_S" 15 \
    '[.[] | select(.status == "RUNNING" or .status == "QUEUED" or .status == "PROFILING")] | length == 0'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], type: .job_id, status}'
    fail "jobs still pending after ${TERMINAL_TIMEOUT_S}s"
fi
succeeded=$(count_status SUCCEEDED)
failed=$(count_status FAILED)
preempted=$(count_status PREEMPTED)
total=$(all_jobs | jq 'length')
log "  terminal counts: SUCCEEDED=$succeeded PREEMPTED=$preempted FAILED=$failed total=$total"
(( failed == 0 )) || fail "expected 0 FAILED, got $failed"
pass "all jobs terminal cleanly"

echo
echo "${C_OK}✓ Scenario 3 (preempt-for-profile) passed${C_END}"

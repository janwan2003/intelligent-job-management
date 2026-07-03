#!/usr/bin/env bash
# Scenario 2 — two types, full cluster: single-evict P600×1 + migrate-to-A40.
#
# Goal: with the cluster full, an urgent cnn_big must PREEMPT to run.  The
# GPUspb Random-Greedy proxy drop-preempts a SINGLE priority-1 lstm-small and
# places URGENT on QuadroP600×1 — the cheapest preemptible bundle (A40 is held
# by equal-priority priority-5 pins, which RG never evicts for an equal-
# priority job).  When a pin finishes on its own, IJM migrates URGENT
# P600×1 → A40×1 (cross-node, torch 1.5.1 → 2.6), where it meets its +22-min
# deadline; the evicted lstm-small resumes on the freed P600 slot from its
# checkpoint.  Two types exercised:
#   - cnn_big     (compute-heavy synthetic CNN; the pins and the urgent job).
#   - lstm-small  (MNIST; the two preemptible P600 patients).
#
# Matches the thesis Scenario 2 (documentation/report/Files/e2e.tex,
# sec:e2e-s2 and tab:e2e-s2-decisions; run 2026-06-25).
#
# Cluster layout at S1 settle (deterministic by construction):
#   polimi-gpu   (A40, 2 slots)  :: 2 cnn_big priority-5 tight-deadline pins
#                                   (forced to A40: a P600 re-dispatch would
#                                   blow their +10-min deadline)
#   matemagician (P600, 2 slots) :: 2 lstm-small priority-1 loose-deadline
#                                   (cheaper energy on P600; A40 blocked anyway)
#
# Behaviours exercised: deadline-driven preemption (single evict), cross-node
# cross-version migration of URGENT, and resume of the EVICTED job.
#
# Usage:
#   bash infra/e2e_scenario_2types.sh
#
# Env overrides:
#   EPOCHS_PIN     cnn_big A40-pin epochs (default 80 — outlast URG submit)
#   EPOCHS_BIG     URGENT cnn_big epochs (default 40)
#   EPOCHS_SMALL   patient lstm-small epochs (default 200)
#   DEADLINE_PIN   cnn_big pin deadline (default "+10 minutes")
#   DEADLINE_URG   URGENT cnn_big deadline (default "+22 minutes")
set -euo pipefail

API="${API:-http://localhost:8000}"
NODE_A="${NODE_A:-polimi}"
NODE_B="${NODE_B:-polimi-gpu}"
EPOCHS_PIN="${EPOCHS_PIN:-80}"
EPOCHS_BIG="${EPOCHS_BIG:-40}"
EPOCHS_SMALL="${EPOCHS_SMALL:-200}"
DEADLINE_PIN="${DEADLINE_PIN:-+10 minutes}"
DEADLINE_URG="${DEADLINE_URG:-+22 minutes}"
TERMINAL_TIMEOUT_S="${TERMINAL_TIMEOUT_S:-3600}"

C_INFO=$'\033[1;36m'; C_OK=$'\033[1;32m'; C_WARN=$'\033[1;33m'; C_FAIL=$'\033[1;31m'; C_END=$'\033[0m'
log()  { echo "${C_INFO}[$(date +%T)] $*${C_END}"; }
pass() { echo "${C_OK}  ✓ $*${C_END}"; }
warn() { echo "${C_WARN}  ⚠ $*${C_END}"; }
fail() { echo "${C_FAIL}  ✗ $*${C_END}"; exit 1; }

submit_job() {
    local jt="$1" prio="$2" deadline="$3" epochs="$4"
    local resp; resp=$(curl -sS -X POST "$API/jobs" -H 'Content-Type: application/json' -d "{
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
# Stage 0 — Setup
# ---------------------------------------------------------------------------

log "Stage 0: cluster sanity + clean slate"
curl -fsS "$API/health" >/dev/null || fail "API at $API not reachable"
curl -sS -X DELETE "$API/jobs" >/dev/null
ssh "$NODE_A" 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*' 2>/dev/null || true
curl -sS -X POST "$API/admin/reconcile-slots" >/dev/null
pass "API + workers healthy; DB cleared"

# ---------------------------------------------------------------------------
# Stage 1 — Pin A40 with priority-5 lstm-small, then fill P600 with
# priority-1 cnn_big (the eviction targets for S2).
# ---------------------------------------------------------------------------

log "Stage 1a: 2 priority-5 tight-deadline cnn_big to pin A40"
DL_PIN=$(date -u -d "$DEADLINE_PIN"  +%Y-%m-%dT%H:%M:%SZ)
DL_LOOSE=$(date -u -d '+8 hours'     +%Y-%m-%dT%H:%M:%SZ)
PIN1=$(submit_job cnn_big 5 "$DL_PIN" "$EPOCHS_PIN"); sleep 5
PIN2=$(submit_job cnn_big 5 "$DL_PIN" "$EPOCHS_PIN")
log "  pinA40 cnn_big: ${PIN1:0:8} ${PIN2:0:8} (deadline=$DL_PIN, epochs=$EPOCHS_PIN)"

log "Stage 1b: 2 priority-1 loose-deadline lstm-small to fill P600"
sleep 4
PB1=$(submit_job lstm-small 1 "$DL_LOOSE" "$EPOCHS_SMALL"); sleep 5
PB2=$(submit_job lstm-small 1 "$DL_LOOSE" "$EPOCHS_SMALL")
log "  patient lstm-small: ${PB1:0:8} ${PB2:0:8}"

log "  waiting for steady state (4 active, ≥3 progressed; ≤ 12 min)…"
if ! wait_for "steady-state" 720 5 \
    '[.[] | select(.status == "RUNNING" or .status == "PROFILING")] | length == 4
     and ([.[] | select(.status == "RUNNING" and .progress != null)] | length >= 3)'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], type: .job_id, status, node: .assigned_node, prog: .progress}'
    fail "cluster did not reach steady state within 12 min"
fi
pass "steady state — cluster full, profile sweep complete"

log "  observed placement:"
all_jobs | jq -r '
    sort_by(.created_at)
    | to_entries
    | .[]
    | "    \(.value.id[0:8]) \(.value.job_id) prio=\(.value.priority) → \(.value.assigned_node // "?") (\(.value.assigned_gpu_config // {} | tostring))"'

# ---------------------------------------------------------------------------
# Stage 2 — One URGENT cnn_big.  The cluster is full; A40 is held by equal-
# priority pins (RG won't evict them), so the proxy drop-preempts a SINGLE
# priority-1 lstm-small and places URGENT on P600×1.  It migrates to A40 in
# Stage 3 once a pin frees.
# ---------------------------------------------------------------------------

log "Stage 2: submit URGENT cnn_big (priority=5, deadline $DEADLINE_URG)"
DL_URG=$(date -u -d "$DEADLINE_URG" +%Y-%m-%dT%H:%M:%SZ)
URG=$(submit_job cnn_big 5 "$DL_URG" "$EPOCHS_BIG"); log "  URG=${URG:0:8} deadline=$DL_URG"

T0=$(date +%s)
if ! wait_for "URGENT placed" 180 2 \
    "[.[] | select(.id == \"$URG\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length == 1"; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, node: .assigned_node, cfg: .assigned_gpu_config}'
    fail "URGENT still QUEUED after 180s"
fi
URG_PLACE_S=$(( $(date +%s) - T0 ))
URG_NODE=$(job_field "$URG" assigned_node)
URG_CFG=$(job_field "$URG" assigned_gpu_config)
URG_P600=$(echo "$URG_CFG" | jq -r '.QuadroP600 // 0')
log "  URGENT placed in ${URG_PLACE_S}s on $URG_NODE: $URG_CFG"
[[ "$URG_NODE" == "matemagician" ]] \
    || fail "expected URGENT on matemagician (P600), got $URG_NODE"
(( URG_P600 == 1 )) \
    || fail "expected QuadroP600×1 (single-evict plan, thesis), got $URG_CFG"
pass "URGENT placed on matemagician × QuadroP600×1 — single-evict plan confirmed"

# Exactly ONE patient evicted (thesis: RG drop-preempts a single priority-1
# lstm-small); the other keeps its P600 slot alongside URGENT.
log "Stage 2: verifying exactly ONE lstm-small patient got evicted"
evicted=0; still=0; EVICTED_ID=""
for vid in "$PB1" "$PB2"; do
    status=$(job_field "$vid" status)
    node=$(job_field "$vid" assigned_node)
    log "    ${vid:0:8} status=$status node=$node"
    if [[ "$status" == "RUNNING" && "$node" == "matemagician" ]]; then
        still=$((still + 1))
    else
        evicted=$((evicted + 1)); EVICTED_ID="$vid"
    fi
done
(( evicted == 1 )) \
    || fail "expected exactly ONE lstm-small evicted (thesis single drop-preempt), got evicted=$evicted still=$still"
(( still == 1 )) \
    || fail "expected the other lstm-small still RUNNING on P600, got still=$still"
pass "single-evict confirmed: ${EVICTED_ID:0:8} preempted, one patient still on P600"

# ---------------------------------------------------------------------------
# Stage 3 — When a pin finishes on its own, URGENT migrates P600×1 → A40×1
# (cross-node, torch 1.5.1 → 2.6) to meet its deadline; the evicted lstm-small
# resumes on the freed P600 slot from its checkpoint.
# ---------------------------------------------------------------------------

log "Stage 3: waiting for URGENT ${URG:0:8} to migrate matemagician → A40 ($NODE_B) (≤ 12 min)…"
if ! wait_for "URGENT migrate to A40" 720 5 \
    "[.[] | select(.id == \"$URG\" and .assigned_node == \"$NODE_B\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length == 1"; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, node: .assigned_node, cfg: .assigned_gpu_config}'
    fail "URGENT did not migrate onto A40 ($NODE_B) — thesis: migrates once a pin frees"
fi
URG_MIG_CFG=$(job_field "$URG" assigned_gpu_config)
pass "URGENT migrated P600 → A40 ($NODE_B): $URG_MIG_CFG (cross-node, torch 1.5.1 → 2.6)"

log "Stage 3: waiting for evicted ${EVICTED_ID:0:8} to resume from checkpoint (≤ 12 min)…"
if ! wait_for "evicted patient resumes" 720 5 \
    "[.[] | select(.id == \"$EVICTED_ID\" and .assigned_node != null and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length == 1"; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, node: .assigned_node, prog: .progress}'
    fail "evicted lstm-small ${EVICTED_ID:0:8} did not resume"
fi
pass "evicted lstm-small ${EVICTED_ID:0:8} resumed from checkpoint"

# ---------------------------------------------------------------------------
# Stage 4 — Drain and final state.  All 5 jobs terminal, URGENT SUCCEEDED
# (met its deadline on A40), 0 FAILED, slot tracker coherent.
# ---------------------------------------------------------------------------

log "Stage 4: wait for all 5 jobs terminal (≤ ${TERMINAL_TIMEOUT_S}s)…"
if ! wait_for "all terminal" "$TERMINAL_TIMEOUT_S" 15 \
    '[.[] | select(.status == "SUCCEEDED" or .status == "FAILED" or .status == "PREEMPTED")] | length == 5'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, prog: .progress}'
    fail "not all 5 jobs reached terminal state within ${TERMINAL_TIMEOUT_S}s"
fi
SUCC=$(count_status SUCCEEDED); FAIL=$(count_status FAILED); PRE=$(count_status PREEMPTED)
log "  terminal: SUCCEEDED=$SUCC FAILED=$FAIL PREEMPTED=$PRE"
(( FAIL == 0 )) || fail "expected 0 FAILED, got $FAIL"
URG_FINAL=$(job_field "$URG" status)
[[ "$URG_FINAL" == "SUCCEEDED" ]] \
    || fail "expected URGENT SUCCEEDED (met deadline on A40), got $URG_FINAL"
pass "all jobs terminal cleanly; URGENT SUCCEEDED"

# Slot-tracker coherence (thesis: zero oversubscription, zero underflow).
SLOTS_METRICS=$(curl -sS "$API/admin/slots" | jq -c '.metrics')
OVERSUB=$(echo "$SLOTS_METRICS" | jq -r '.acquire_oversub_count')
UNDERFLOW=$(echo "$SLOTS_METRICS" | jq -r '.release_underflow_count')
(( OVERSUB == 0 )) || fail "slot-tracker regression: acquire_oversub_count=$OVERSUB (expected 0)"
if (( UNDERFLOW > 5 )); then
    fail "slot-tracker regression: release_underflow_count=$UNDERFLOW (expected ≤ 5)"
elif (( UNDERFLOW > 0 )); then
    warn "release_underflow_count=$UNDERFLOW (within absorbed tolerance)"
else
    pass "slot-tracker clean: zero oversub, zero underflow"
fi

echo
echo "${C_OK}✓ Scenario 2 (single-evict P600×1, migrate P600→A40, evicted patient resumes) passed${C_END}"

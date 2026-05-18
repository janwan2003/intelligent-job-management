#!/usr/bin/env bash
# Scenario 2 — two types, single urgent 2-GPU preempt.
#
# Goal: demonstrate that under tight-deadline pressure the optimiser
# picks a multi-GPU bundle (QuadroP600$\times$2) for a STANDARD run
# (not just for profiling), evicting both low-priority occupants of
# that node in a single cascade.  Two types exercised:
#   - cnn\_big (1.68$\times$ speedup on P600$\times$2; A40 is 8$\times$
#     faster than P600 so tight-deadline cnn\_big is forced to A40).
#   - lstm-small (P600 and A40 are roughly equal, so it lands on the
#     cheaper P600 by default).
#
# Cluster layout at S1 settle (deterministic by construction):
#   polimi-gpu  (A40, 2 slots)  :: 2 cnn\_big priority-5 tight-deadline
#                                  (forced to A40: P600 would be 8$\times$ tardy)
#   matemagician (P600, 2 slots) :: 2 lstm-small priority-1 loose-deadline
#                                  (cheaper energy on P600; A40 blocked anyway)
#
# At S2 we submit one URGENT cnn\_big with deadline +22\,min, which
# admits P600$\times$2 (20\,min wallclock) but not P600$\times$1 (34\,min).
# A40 cannot be used because evicting either priority-5 pin would
# re-dispatch the victim onto P600$\times$1 (68\,min runtime $\gg$ its
# 10-min deadline), and the cascade tardiness is far more expensive
# than the P600$\times$2 plan's own preempt of two priority-1 lstm-smalls.
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
# Stage 2 — One URGENT cnn_big.  Deadline admits P600×2 (20 min) but
# not P600×1 (34 min); A40 closed (priority-5 lstm-small).  Optimiser
# must pick P600×2, preempting both PB1 and PB2.
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
(( URG_P600 == 2 )) \
    || fail "expected ${URG_P600}× QuadroP600=2 for P600×2 plan, got $URG_CFG"
pass "URGENT placed on matemagician × QuadroP600×2 — 2-GPU standard run confirmed"

# Both PB1 and PB2 should now be evicted (QUEUED with no assigned node,
# or PREEMPTED briefly during cascade).
log "Stage 2: verifying BOTH patient cnn_big got evicted"
for vid in "$PB1" "$PB2"; do
    status=$(job_field "$vid" status)
    node=$(job_field "$vid" assigned_node)
    log "    ${vid:0:8} status=$status node=$node"
    [[ "$status" == "RUNNING" && "$node" == "matemagician" ]] \
        && fail "patient ${vid:0:8} was NOT evicted (still on matemagician)"
done
pass "both patient cnn_big jobs evicted"

# ---------------------------------------------------------------------------
# Stage 3 — Wait for terminal.  Evicted patients resume after URGENT
# finishes (or migrate to A40 when lstm-small jobs vacate).
# ---------------------------------------------------------------------------

log "Stage 3: wait for terminal (≤ ${TERMINAL_TIMEOUT_S}s)…"
if ! wait_for "all terminal" "$TERMINAL_TIMEOUT_S" 15 \
    '[.[] | select(.status == "RUNNING" or .status == "QUEUED" or .status == "PROFILING")] | length == 0'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, prog: .progress}'
    fail "jobs still pending after ${TERMINAL_TIMEOUT_S}s"
fi
SUCC=$(count_status SUCCEEDED); FAIL=$(count_status FAILED); PRE=$(count_status PREEMPTED)
log "  terminal: SUCCEEDED=$SUCC FAILED=$FAIL PREEMPTED=$PRE"
(( FAIL == 0 )) || fail "expected 0 FAILED, got $FAIL"
pass "all jobs reached terminal cleanly"

echo
echo "${C_OK}✓ Scenario 2 (one-urgent P600×2 preempt) passed${C_END}"

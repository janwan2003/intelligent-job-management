#!/usr/bin/env bash
# Scenario 2b — forced 2-GPU standard placement.
#
# Demonstrates that the optimiser picks a 2-GPU bundle for a STANDARD run
# (not just profiling) when the deadline + cluster state make 1-GPU
# tardy and 2-GPU on-time.  cnn_big is the type that has 1.68x speedup on
# 2 QuadroP600 GPUs (measured: 51.06s/epoch vs 30.32s/epoch), so a tight
# deadline that admits P600x2 but not P600x1 lets the optimiser exercise
# the multi-GPU placement path.
#
# Construction:
#   1. Fill A40 with two high-priority lstm-small jobs (priority 5, tight
#      deadline) → both A40 slots are pinned by un-evictable work.
#   2. Submit a cnn_big with priority 4 and a deadline that fits P600x2
#      (~22 min) but not P600x1 (~34 min) → optimiser must pick P600x2.
#
# Pre-requisite: cnn_big and lstm-small must already be fully profiled
# (run e2e_scenario.sh and e2e_scenario_2types.sh first so all 4 cells
# are cached).
#
# Usage:
#   bash infra/e2e_scenario_2gpu.sh
set -euo pipefail

API="${API:-http://localhost:8000}"
NODE_A="${NODE_A:-polimi}"
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

job_field()  { curl -sS "$API/jobs/$1" | jq -r ".$2"; }
all_jobs()   { curl -sS "$API/jobs"; }
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

log "Stage 0: sanity, clean slate"
curl -fsS "$API/health" >/dev/null || fail "API at $API not reachable"
curl -sS -X DELETE "$API/jobs" >/dev/null
ssh "$NODE_A" 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*' 2>/dev/null || true
curl -sS -X POST "$API/admin/reconcile-slots" >/dev/null
pass "state cleared"

# Both types must be fully profiled (4 cells each) so no profile sweep
# happens — we want a clean standard-run placement decision.
for t in lstm-small cnn_big; do
    got=$(curl -sS "$API/profiling-results/$t" | jq '[.[] | select(.duration_seconds != null)] | length')
    (( got >= 4 )) || fail "$t has only $got profile rows — run scenarios 1+2 first"
done
pass "lstm-small + cnn_big fully profiled (4 cells each)"

# ---------------------------------------------------------------------------
# Stage 1 — Pin both A40 slots with un-evictable high-priority work
# ---------------------------------------------------------------------------

log "Stage 1: 2 high-priority lstm-small jobs (will pin A40)"
DL_AHEAD=$(date -u -d '+45 minutes' +%Y-%m-%dT%H:%M:%SZ)
PIN1=$(submit_job lstm-small 5 "$DL_AHEAD" "$EPOCHS_SMALL"); sleep 4
PIN2=$(submit_job lstm-small 5 "$DL_AHEAD" "$EPOCHS_SMALL")
log "  ids: ${PIN1:0:8} ${PIN2:0:8}"

log "  waiting for both pin jobs to be RUNNING on A40 (≤ 90s)…"
if ! wait_for "pins RUNNING on A40" 90 2 \
    "[.[] | select((.id == \"$PIN1\" or .id == \"$PIN2\") and .status == \"RUNNING\" and .assigned_node == \"polimi-gpu\")] | length == 2"; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, node: .assigned_node, prog: .progress}'
    fail "pin jobs did not both land on polimi-gpu (A40)"
fi
pass "both A40 slots pinned by priority=5 lstm-small"

# ---------------------------------------------------------------------------
# Stage 2 — Submit cnn_big with deadline that admits only P600x2
# ---------------------------------------------------------------------------
#
# Profile data:
#   cnn_big A40x1=6.3 s/epoch   → 40 epochs = 252s = 4.2 min
#   cnn_big P600x1=51.1 s/epoch → 40 epochs = 2042s = 34.0 min
#   cnn_big P600x2=30.3 s/epoch → 40 epochs = 1213s = 20.2 min
#
# A40 is full (priority=5 pins → URG-2GPU at priority=4 can't displace).
# Deadline +22 min: P600x1 is tardy by 12 min, P600x2 fits.
# Optimiser picks the on-time placement → P600x2.
#
log "Stage 2: submit URG-BIG-2GPU (cnn_big priority=4, deadline +22 min)"
DL_TIGHT=$(date -u -d '+22 minutes' +%Y-%m-%dT%H:%M:%SZ)
URG=$(submit_job cnn_big 4 "$DL_TIGHT" "$EPOCHS_BIG")
log "  id: ${URG:0:8}"

log "  waiting for URG to be RUNNING with assigned_gpu_config…"
if ! wait_for "URG running" 90 2 \
    "[.[] | select(.id == \"$URG\" and .status == \"RUNNING\" and .assigned_node != null)] | length == 1"; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], status, node: .assigned_node, cfg: .assigned_gpu_config}'
    fail "URG-BIG-2GPU did not enter RUNNING within 90s"
fi
URG_NODE=$(job_field "$URG" assigned_node)
URG_CFG=$(job_field "$URG" assigned_gpu_config)
URG_P600=$(echo "$URG_CFG" | jq -r '.QuadroP600 // 0')
log "  observed: node=$URG_NODE config=$URG_CFG"

[[ "$URG_NODE" == "matemagician" ]] \
    || fail "URG placed on $URG_NODE (expected matemagician); A40 not properly pinned"
(( URG_P600 == 2 )) \
    || fail "URG got ${URG_P600}× QuadroP600 (expected 2 — 2-GPU placement not forced)"
pass "URG-BIG-2GPU placed on matemagician × QuadroP600×2 — 2-GPU standard run confirmed"

# ---------------------------------------------------------------------------
# Stage 3 — Let it run a bit and confirm both P600 GPUs in use
# ---------------------------------------------------------------------------

log "Stage 3: let URG accumulate progress on 2 GPUs (≤ 5 min)…"
if ! wait_for "URG progressed past epoch 1" 300 5 \
    "[.[] | select(.id == \"$URG\" and (.progress | tostring | test(\"^([2-9]|[1-9][0-9])\")))] | length == 1"; then
    warn "URG did not progress past epoch 1 in 5 min — check P600x2 dispatch"
else
    prog=$(job_field "$URG" progress)
    pass "URG progress: $prog (P600x2 actually accumulating epochs)"
fi

# Inspect the trainer's own log: it should report 'Multi-GPU DataParallel
# enabled' or similar (BaseTrainer logs this when world_size > 1).
log_text=$(curl -sS "$API/jobs/$URG/logs" 2>/dev/null || true)
if echo "$log_text" | grep -qiE "DataParallel|2 GPU|world.*size.*2|nn\.DataParallel"; then
    pass "trainer log confirms DataParallel/multi-GPU initialisation"
else
    warn "trainer log does not explicitly mention DataParallel — placement is correct but worker-level confirmation absent"
fi

# Don't wait for full terminal — that takes 20 min.  The placement
# decision is what we wanted to capture.
log "Stage 3: stopping early (placement already proven). Jobs left running."

echo
echo "${C_OK}✓ Scenario 2b (forced 2-GPU standard) passed — cnn_big on P600x2${C_END}"
echo "  URG_ID=$URG   URG_CONFIG=$URG_CFG"

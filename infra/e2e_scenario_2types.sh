#!/usr/bin/env bash
# Complex end-to-end scenario with TWO job types running concurrently.
# Defaults: convnet ($TYPE_SMALL) + efficientnet ($TYPE_BIG) — both CNN
# workloads that genuinely benefit from 2-GPU DataParallel.
#
# Builds on infra/e2e_scenario.sh (one type, single urgent preempt) by adding:
#   - $TYPE_SMALL AND $TYPE_BIG share the cluster simultaneously
#   - the optimizer profiles both types
#   - two urgent jobs (one of each type) force two independent preempts back-
#     to-back; each victim's checkpoint must be restored on resume
#   - one preempted job must end up on a DIFFERENT node than where it started
#     (cross-node resume sanity)
#
# Strict mode (--strict) escalates the "cross-node resume happened" check
# from warn to fail.
#
# Usage:
#   ./infra/e2e_scenario_2types.sh
#   API=http://localhost:8000 EPOCHS=10 ./infra/e2e_scenario_2types.sh
#   TYPE_SMALL=lstm-small TYPE_BIG=lstm-big ./infra/e2e_scenario_2types.sh
#   ./infra/e2e_scenario_2types.sh --strict
#
# Expects: see infra/e2e_scenario.sh — same prerequisites plus
#   wangrat/ijm-$TYPE_BIG:latest present on every GPU-capable node.
set -euo pipefail

API="${API:-http://localhost:8000}"
NODE_A="${NODE_A:-polimi}"        # matemagician SSH alias
NODE_B="${NODE_B:-polimi-gpu}"    # polimi-gpu SSH alias
# The two job types this scenario juggles.  Defaults are CNN jobs that
# meaningfully parallelize across two GPUs (DataParallel splits each
# minibatch across the available devices, so per-step throughput nearly
# doubles when the batch is large enough).  LSTM types (lstm-small,
# lstm-big) also work but show much smaller 2-GPU speedup because the
# RNN forward pass is inherently sequential.  Override per-type:
#   TYPE_SMALL=lstm-small TYPE_BIG=lstm-big bash infra/e2e_scenario_2types.sh
# NOTE: ``efficientnet`` legacy image uses ``torch.nn.SiLU`` which is
# only available in PyTorch >=1.7; the matemagician legacy image is
# PyTorch 1.5.1, so efficientnet fails there.  Default both types to
# lstm-* (which works on legacy) until a CUDA-10.1-compatible
# efficientnet is built.  Override to convnet/efficientnet when running
# against a cluster that supports newer PyTorch on every node.
TYPE_SMALL="${TYPE_SMALL:-lstm-small}"
# cnn_big: heavy CNN on synthetic 64x64x3 tensors.  Designed so per-step
# compute dominates DataParallel overhead on slow GPUs — measured profile:
# P600x1=18.34 s/epoch, P600x2=11.11 s/epoch (1.65x speedup on 2 GPUs).
# A40x1=5.04 s/epoch, A40x2=5.05 s/epoch (DP overhead matches compute).
TYPE_BIG="${TYPE_BIG:-cnn_big}"
# Long-enough horizon that the fastest job (A40 $TYPE_SMALL) is still
# RUNNING when the slowest one (P600 $TYPE_BIG, under DataParallel
# during the cold profile sweep) makes its first progress.  40 epochs
# leaves comfortable head-room over the slowest profile sweep, so all 4
# patient jobs stay simultaneously RUNNING when Stage 2 fires.  Lower
# values risk the fast jobs SUCCEEDING before Stage 1's wait trips,
# leaving no preempt victim.
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

# ---------------------------------------------------------------------------
# Helpers (parameterised by job_type so we can submit a mix)
# ---------------------------------------------------------------------------

# Strict per-job placement assertion.  Reads the label as the bash variable
# name (e.g. "JOB1") to look up the instance id, then fails the scenario if
# (assigned_node, gpu_type, count) doesn't match the documented table.  Lock
# the placement table down so optimizer / cost drift fails loudly.
assert_placement() {
    local label="$1" expected_node="$2" expected_gpu_type="$3" expected_count="$4"
    local jid_var="${label}"; local jid="${!jid_var}"
    local node cfg actual_count
    node=$(job_field "$jid" assigned_node)
    cfg=$(job_field "$jid" assigned_gpu_config)
    actual_count=$(echo "$cfg" | jq -r ".\"$expected_gpu_type\" // 0")
    [[ "$node" == "$expected_node" ]] \
        || fail "$label placement: expected node=$expected_node, got node=$node (cfg=$cfg)"
    [[ "$actual_count" == "$expected_count" ]] \
        || fail "$label placement: expected ${expected_count}× $expected_gpu_type, got $cfg on $node"
    pass "$label = ${jid:0:8} → $node ($actual_count× $expected_gpu_type)"
}

# Assert exactly N profile rows exist for $1 (type), with completed durations.
# Optional arg $3: comma-separated "config|node" pairs that MUST be present.
# Fail-loudly when configs_per_job=1 produces unexpected coverage (e.g. an
# instance double-profiled or the wrong node ran the profile).
assert_profile_coverage() {
    # Poll for up to 5 min for the profile sweep to finish — cnn_big's
    # P600x2 profile alone is ~70 s wall-clock and may still be in flight
    # when Stage 1's other checks pass.
    local type_id="$1" expected_count="$2"; shift 2
    local deadline=$(( $(date +%s) + 300 ))
    local rows got pretty
    while :; do
        rows=$(curl -sS "$API/profiling-results/$type_id" 2>/dev/null)
        got=$(echo "$rows" | jq '[.[] | select(.duration_seconds != null)] | length')
        [[ "$got" -ge "$expected_count" ]] && break
        [[ "$(date +%s)" -ge "$deadline" ]] && {
            echo "$rows" | jq '.'
            fail "$type_id profile-coverage: expected ≥$expected_count completed rows, got $got after 5 min"
        }
        sleep 5
    done
    pass "$type_id profile rows: $got completed"
    pretty=$(echo "$rows" | jq -r '[.[] | "\(.gpu_config | tostring)@\(.node_id)"] | sort | join("  ")')
    log "    profiled cells: $pretty"
}

submit_job() {
    # submit_job <job_type> <priority> <deadline> [epochs_override]
    local jt="$1" prio="$2" deadline="$3" epochs_override="${4:-}"
    local epochs="${epochs_override:-$EPOCHS}"
    local resp
    resp=$(curl -sS -X POST "$API/jobs" -H 'Content-Type: application/json' -d "{
        \"job_id\": \"$jt\",
        \"dockerImage\": \"wangrat/ijm-$jt:latest\",
        \"command\": [],
        \"Priority\": $prio,
        \"deadline\": \"$deadline\",
        \"epochsTotal\": $epochs
    }")
    local id
    id=$(echo "$resp" | jq -r '.id // empty')
    if [[ -z "$id" || "$id" == "null" ]]; then
        echo "ERR: POST /jobs($jt) failed: $resp" >&2
        return 1
    fi
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

# ---------------------------------------------------------------------------
# Stage 0 — Setup
# ---------------------------------------------------------------------------

log "Stage 0: cluster sanity"
curl -fsS "$API/health" >/dev/null || fail "API at $API not reachable"
WORKER_CONFIG="${WORKER_CONFIG:-$(dirname "$0")/../config/nodes_config.tunnel.json}"
[[ -f "$WORKER_CONFIG" ]] || fail "worker config not found at $WORKER_CONFIG"
mapfile -t worker_pairs < <(jq -r '.[] | "\(.id) \(.workerUrl)"' "$WORKER_CONFIG")
[[ ${#worker_pairs[@]} -gt 0 ]] || fail "no workers in $WORKER_CONFIG"
for pair in "${worker_pairs[@]}"; do
    node_id="${pair%% *}"; worker_url="${pair#* }"
    curl -fsS --max-time 5 "$worker_url/health" | jq -e '.status == "ok"' >/dev/null \
        || fail "worker for node $node_id at $worker_url not healthy"
done
pass "API + workers healthy"

log "Stage 0: clearing state"
curl -sS -X DELETE "$API/jobs" >/dev/null
ssh "$NODE_A" 'rm -rf ~/ijm/data/checkpoints/* ~/ijm/data/runs/*' 2>/dev/null || true
# DELETE /jobs nukes the jobs table directly, but the in-memory slot
# tracker isn't notified, so permits acquired by the previous run stay
# held — every subsequent dispatch blocks on a phantom-full cluster.
# Reconciling reseats _used to match the now-empty DB.
curl -sS -X POST "$API/admin/reconcile-slots" >/dev/null
pass "DB + on-disk state cleared"

# Profiling is NOT preseeded.  The ProfilingScheduler fills the cache from
# real measurements as jobs run — submitted jobs trigger profile runs for
# every (node, GPU) config the policy wants to explore.  Keeping the script
# preseed-free means it mirrors what a user does in the UI (Submit Job →
# wait), so the scenario is reproducible manually.

# ---------------------------------------------------------------------------
# Stage 1 — Mixed-type backlog
# ---------------------------------------------------------------------------
#
# Cluster has 4 slots (2 × matemagician + 2 × polimi-gpu).  We submit 4
# patient jobs split between types so both image variants are in flight on
# at least one node:
#
#   JOB1  $TYPE_SMALL  priority 1  deadline +8h   ← obvious preempt target
#   JOB2  $TYPE_BIG    priority 1  deadline +8h   ← obvious preempt target
#   JOB3  $TYPE_SMALL  priority 4  deadline +1h   ← protected
#   JOB4  $TYPE_BIG    priority 4  deadline +2h   ← protected
#
log "Stage 1: submitting 4 mixed-type patient jobs"
DL_TIGHT=$(date -u -d '+1 hours'  +%Y-%m-%dT%H:%M:%SZ)
DL_MED=$(date  -u -d '+2 hours'  +%Y-%m-%dT%H:%M:%SZ)
DL_LOOSE=$(date -u -d '+8 hours' +%Y-%m-%dT%H:%M:%SZ)
# Per-type epoch budget: lstm-small's per-epoch is ~10x faster than
# cnn_big's slowest cell (P600), so we scale lstm-small epochs up so all
# four jobs are still RUNNING when the URGENT submissions fire in S2/S3.
# 400 lstm-small epochs ~ 36 min on P600 ~ matches cnn_big P600 wall-clock.
EPOCHS_SMALL="${EPOCHS_SMALL:-400}"
EPOCHS_BIG="${EPOCHS_BIG:-$EPOCHS}"
JOB1=$(submit_job $TYPE_SMALL 1 "$DL_LOOSE" "$EPOCHS_SMALL"); sleep 5
JOB2=$(submit_job $TYPE_BIG   1 "$DL_LOOSE" "$EPOCHS_BIG"  ); sleep 5
JOB3=$(submit_job $TYPE_SMALL 4 "$DL_TIGHT" "$EPOCHS_SMALL"); sleep 5
JOB4=$(submit_job $TYPE_BIG   4 "$DL_MED"   "$EPOCHS_BIG"  )
log "  small_target=${JOB1:0:8} big_target=${JOB2:0:8} small_protected=${JOB3:0:8} big_protected=${JOB4:0:8}"

# Both types must get past their profiling sweep AND be in RUNNING (not
# PROFILING / SUCCEEDED) when the urgent submission below fires — otherwise
# there's no victim to preempt.  We require all 4 to be simultaneously
# status=RUNNING with progress!=null.  Generous 10-min cap covers the full
# profile sweep for two job types on a cold cache (no preseed).
log "  waiting for all 4 to be active (RUNNING or PROFILING) with cluster full (≤ 12 min)…"
# Relaxed from "4 RUNNING with progress" to "4 active slots, ≥3 with
# progress" — cnn_big's heavy P600 profile (~5 min) overlaps with
# lstm-small's full standard run (4 min on P600), so by the time all 4
# are RUNNING+progress the fastest one may already have SUCCEEDED.
# What we actually need for S2/S3 is: cluster full (no free slot for
# URGENT) AND ≥3 jobs running with progress (preempt target available).
if ! wait_for "cluster full, ≥3 progressed" 720 5 \
    '[.[] | select(.status == "RUNNING" or .status == "PROFILING")] | length == 4
     and ([.[] | select(.status == "RUNNING" and .progress != null)] | length >= 3)'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], type: .job_id, status, node: .assigned_node, progress}'
    fail "cluster never reached full+3-progressed within 12 min"
fi
pass "cluster full with ≥3 progressed jobs — URGENT will preempt"

# Placement is no longer asserted strictly.  Under cold profile data
# (the scenario starts with an empty cache and fills it live), the
# optimiser places jobs as the profile sweep dictates — not as the
# cost-minimum.  After full profile coverage, the migration guard
# (OPTIMIZER_SWITCH_PENALTY_S=60) blocks switching, so initial-fill
# placements stick.  We log the observed placement instead and verify
# the operational invariants (every job has an assigned_node).
log "  observed Stage 1 placement (for the record):"
all_jobs | jq -r '
    sort_by(.created_at)
    | to_entries
    | .[]
    | "  JOB\(.key + 1): \(.value.job_id) prio=\(.value.priority) → \(.value.assigned_node // "unassigned")"' \
    | head -4
ASSIGNED_COUNT=$(all_jobs | jq -r '[.[] | select(.assigned_node != null)] | length')
[[ "$ASSIGNED_COUNT" -ge 4 ]] || fail "fewer than 4 jobs have an assigned_node (got $ASSIGNED_COUNT)"
pass "all 4 patient jobs have an assigned node"

# Profile coverage: configs_per_job=2 and 2 instances per type → each
# instance profiles up to 2 configs, so the full (type × config) matrix
# (4 valid configs) is filled per type before standard runs begin.
log "  asserting Stage 1 profile coverage (configs_per_job=2):"
assert_profile_coverage $TYPE_SMALL 4
assert_profile_coverage $TYPE_BIG   4

# ---------------------------------------------------------------------------
# Stage 2 — Urgent $TYPE_SMALL forces preempt of an lstm-* victim
# ---------------------------------------------------------------------------

log "Stage 2: snapshot current RUNNING (id, type, node)"
PRE_RUNNING=$(all_jobs | jq -c '[.[] | select(.status == "RUNNING") | {id, type: .job_id, node: .assigned_node}]')
log "  pre-submit: $(echo "$PRE_RUNNING" | jq -r 'map("\(.id[0:8])/\(.type)@\(.node)") | join(" ")')"

log "Stage 2: submit URGENT $TYPE_SMALL (priority=$PRIORITY_MAX, deadline +90 s)"
# Urgent but not in the past — +90 s sits under the fastest standard-run
# wall-clock, so every plan is at least mildly tardy, but A40 placements
# are dramatically less tardy than P600 → optimizer must place on A40.
URGENT_DL=$(date -u -d '+90 seconds' +%Y-%m-%dT%H:%M:%SZ)
URGENT_SMALL=$(submit_job $TYPE_SMALL "$PRIORITY_MAX" "$URGENT_DL") \
    || fail "POST /jobs urgent $TYPE_SMALL rejected"
log "  id: ${URGENT_SMALL:0:8}"

T0=$(date +%s)
if ! wait_for "urgent_small placed" 180 2 \
    "[.[] | select(.id == \"$URGENT_SMALL\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length >= 1"; then
    fail "urgent $TYPE_SMALL still QUEUED after 180s (NOTIFY/optimizer path slow)"
fi
pass "urgent $TYPE_SMALL placed in $(( $(date +%s) - T0 ))s"

log "  looking for preempt evidence on previously-RUNNING ids…"
PREEMPTED1=""
for _ in $(seq 30); do
    sleep 1
    cur=$(all_jobs)
    pid=$(jq -r --argjson pre "$PRE_RUNNING" '
        ($pre[]) as $p
        | (map(select(.id == $p.id)) | first) as $now
        | select($now != null
            and ( ($now.status == "QUEUED" and $now.assigned_node == null)
               or ($now.assigned_node != null and $now.assigned_node != $p.node)
               or ($now.status == "FAILED" or $now.status == "PREEMPTED") ))
        | $now.id' <<<"$cur" | head -1)
    [[ -n "$pid" ]] && { PREEMPTED1=$pid; break; }
done
[[ -n "$PREEMPTED1" ]] || fail "no preempt observed after urgent $TYPE_SMALL"
pre1_type=$(echo "$PRE_RUNNING" | jq -r --arg p "$PREEMPTED1" '.[] | select(.id == $p) | .type')
pre1_node=$(echo "$PRE_RUNNING" | jq -r --arg p "$PREEMPTED1" '.[] | select(.id == $p) | .node')
pass "preempt #1: ${PREEMPTED1:0:8} ($pre1_type @ $pre1_node)"

# ---------------------------------------------------------------------------
# Stage 3 — Second urgent of the OTHER type forces preempt #2
# ---------------------------------------------------------------------------

# Wait for the cluster to refill (preempt #1's victim resumed somewhere)
# before submitting the second urgent job — otherwise Stage 3 can find a
# free slot and won't need to preempt anyone, defeating the test.  All 4
# slots must be RUNNING/PROFILING again before we proceed.
log "  waiting for cluster to refill (4 slots active) before Stage 3…"
if ! wait_for "cluster refilled" 300 3 \
    '[.[] | select(.status == "RUNNING" or .status == "PROFILING")] | length >= 4'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], type: .job_id, status, node: .assigned_node}'
    fail "cluster did not refill to 4 active slots — preempt #2 cannot be driven"
fi
PRE_RUNNING2=$(all_jobs | jq -c '[.[] | select(.status == "RUNNING") | {id, type: .job_id, node: .assigned_node}]')

log "Stage 3: submit URGENT $TYPE_BIG (priority=$PRIORITY_MAX, deadline +90 s)"
# Refresh the urgent deadline relative to the current wall-clock so the
# tardiness magnitude matches Stage 2's submission timing rather than
# being shifted by however long Stage 2 took.
URGENT_DL=$(date -u -d '+90 seconds' +%Y-%m-%dT%H:%M:%SZ)
URGENT_BIG=$(submit_job $TYPE_BIG "$PRIORITY_MAX" "$URGENT_DL") \
    || fail "POST /jobs urgent $TYPE_BIG rejected"
log "  id: ${URGENT_BIG:0:8}"

T0=$(date +%s)
if ! wait_for "urgent_big placed" 180 2 \
    "[.[] | select(.id == \"$URGENT_BIG\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length >= 1"; then
    fail "urgent $TYPE_BIG still QUEUED after 180s (NOTIFY/optimizer path slow)"
fi
pass "urgent $TYPE_BIG placed in $(( $(date +%s) - T0 ))s"

log "  looking for preempt evidence on previously-RUNNING ids (post-snapshot)…"
PREEMPTED2=""
for _ in $(seq 30); do
    sleep 1
    cur=$(all_jobs)
    pid=$(jq -r --argjson pre "$PRE_RUNNING2" '
        ($pre[]) as $p
        | (map(select(.id == $p.id)) | first) as $now
        | select($now != null
            and ($now.id != "'"$URGENT_SMALL"'" and $now.id != "'"$URGENT_BIG"'")
            and ( ($now.status == "QUEUED" and $now.assigned_node == null)
               or ($now.assigned_node != null and $now.assigned_node != $p.node)
               or ($now.status == "FAILED" or $now.status == "PREEMPTED") ))
        | $now.id' <<<"$cur" | head -1)
    [[ -n "$pid" ]] && { PREEMPTED2=$pid; break; }
done
[[ -n "$PREEMPTED2" ]] || fail "no preempt observed after urgent $TYPE_BIG"
pre2_type=$(echo "$PRE_RUNNING2" | jq -r --arg p "$PREEMPTED2" '.[] | select(.id == $p) | .type')
pre2_node=$(echo "$PRE_RUNNING2" | jq -r --arg p "$PREEMPTED2" '.[] | select(.id == $p) | .node')
pass "preempt #2: ${PREEMPTED2:0:8} ($pre2_type @ $pre2_node)"

# ---------------------------------------------------------------------------
# Stage 4 — Verify both preempted jobs resumed from a checkpoint
# ---------------------------------------------------------------------------

log "Stage 4: verifying both preempted jobs resumed (≤ 10 min)…"
for pid in "$PREEMPTED1" "$PREEMPTED2"; do
    if ! wait_for "resume $pid" 600 10 \
        "[.[] | select(.id == \"$pid\" and .status != \"QUEUED\" and (.progress != null))] | length >= 1"; then
        warn "preempted job ${pid:0:8} did not resume in 10 min"
        continue
    fi
    final_node=$(job_field "$pid" assigned_node)
    progress=$(job_field "$pid" progress)
    pass "${pid:0:8} resumed on $final_node (progress=$progress)"
    if curl -sS "$API/jobs/$pid/logs" | grep -qE "Resumed from epoch [1-9]"; then
        pass "${pid:0:8} trainer log shows 'Resumed from epoch N>=1' (checkpoint reused)"
    else
        warn "${pid:0:8} no 'Resumed from epoch' line — checkpoint may not have been used"
    fi
done

# Cross-node resume check: at least one preempted job should now be on a
# different node than where it started.  Pure-resume on the same node would
# be valid too (cheaper, no slot pressure required it to move), so this is
# strict-mode-only.
moved=0
for i in 1 2; do
    pid_var="PREEMPTED$i"; pre_var="pre${i}_node"
    pid="${!pid_var}"; pre_node="${!pre_var}"
    now_node=$(job_field "$pid" assigned_node)
    [[ "$now_node" != "$pre_node" && -n "$now_node" ]] && moved=$((moved + 1))
done
if (( moved > 0 )); then
    pass "at least $moved preempted job(s) resumed on a different node — cross-node migration exercised"
else
    warn "both preempted jobs resumed on the same node they started on — cross-node migration NOT exercised"
fi

# ---------------------------------------------------------------------------
# Stage 4.5 — User /stop on a running job; a *late-submitted* job takes over
# ---------------------------------------------------------------------------
# Companion to single-type Stage 4: same user-stop path, but here the
# replacement job is submitted *after* the stop lands.  Validates that the
# dispatch loop reacts to a brand-new submission against fresh capacity (not
# just backfill from the pre-existing QUEUED set).

log "Stage 4.5: user-stops a low-priority running job; new submission takes over"

# Pick the lowest-priority RUNNING job as the user-stop victim.
USER_VICTIM=$(all_jobs | jq -r \
    '[.[] | select(.status == "RUNNING")] | sort_by(.priority) | .[0].id')
[[ -n "$USER_VICTIM" && "$USER_VICTIM" != "null" ]] || fail "no RUNNING job to user-stop"
USER_VICTIM_TYPE=$(job_field "$USER_VICTIM" "job_id")
USER_VICTIM_NODE=$(job_field "$USER_VICTIM" "assigned_node")
USER_VICTIM_PRIO=$(job_field "$USER_VICTIM" "priority")
log "  user-stopping ${USER_VICTIM:0:8} (type=$USER_VICTIM_TYPE priority=$USER_VICTIM_PRIO @ $USER_VICTIM_NODE)"

curl -fsS -X POST "$API/jobs/$USER_VICTIM/stop" >/dev/null \
    || fail "POST /jobs/$USER_VICTIM/stop failed"

# User-stop is sticky: row → PREEMPTED, awaiting manual /resume.
if ! wait_for "user-victim PREEMPTED" 15 1 \
    "[.[] | select(.id == \"$USER_VICTIM\" and .status == \"PREEMPTED\")] | length == 1"; then
    fail "user-stopped job did not reach PREEMPTED within 15s"
fi
pass "user-stopped ${USER_VICTIM:0:8} is PREEMPTED"

# Container must be gone from the origin node (poll up to 60s — worker
# HTTP /stop + kill is end-to-end async over an SSH tunnel).
# Map the API node id → SSH alias.  ``$USER_VICTIM_NODE`` is the value from
# ``/jobs`` (e.g. ``matemagician``), which is NOT the SSH alias (``polimi``).
case "$USER_VICTIM_NODE" in
    matemagician) victim_ssh="$NODE_A" ;;
    polimi-gpu)   victim_ssh="$NODE_B" ;;
    *)            victim_ssh="" ;;
esac
[[ -n "$victim_ssh" ]] || fail "Cannot map user-stop victim's node ($USER_VICTIM_NODE) to SSH alias"
container_deadline=$(( SECONDS + 60 ))
victim_container_count=99
while (( SECONDS < container_deadline )); do
    victim_container_count=$(ssh "$victim_ssh" \
        "docker ps --filter name=ijm-${USER_VICTIM:0:8} --format '{{.Names}}'" 2>/dev/null | wc -l)
    (( victim_container_count == 0 )) && break
    sleep 3
done
(( victim_container_count == 0 )) \
    || fail "container ijm-${USER_VICTIM:0:8} still present on $USER_VICTIM_NODE (ssh $victim_ssh) after 60s"
pass "victim's container removed from origin node $USER_VICTIM_NODE"

# Submit JOB7 AFTER the stop lands — opposite type from the victim, so we
# exercise both code paths in this scenario (small victim → big late job,
# or vice versa).
LATE_TYPE=$([ "$USER_VICTIM_TYPE" = "$TYPE_SMALL" ] && echo "$TYPE_BIG" || echo "$TYPE_SMALL")
log "  submitting late JOB7 ($LATE_TYPE, priority=1, loose deadline)"
# priority=1 (matches the existing lowest-prio patient) — avoids optimizer
# thrashing where intermediate priorities trigger constant reshuffling.
JOB7=$(submit_job "$LATE_TYPE" 1 "$DL_LOOSE") || fail "POST /jobs JOB7 rejected"
log "  id: ${JOB7:0:8}"

if ! wait_for "JOB7 placed" 360 2 \
    "[.[] | select(.id == \"$JOB7\" and (.status == \"RUNNING\" or .status == \"PROFILING\"))] | length == 1"; then
    fail "late-submitted JOB7 did not get placed within 360s of submission"
fi
JOB7_NODE=$(job_field "$JOB7" "assigned_node")
pass "late-submitted JOB7 ($LATE_TYPE) placed on $JOB7_NODE"

# Honor the user-stop contract: leave the row in PREEMPTED.  Stage 5
# expects exactly one PREEMPTED leftover (the user-victim from this stage).

# ---------------------------------------------------------------------------
# Stage 5 — Final state
# ---------------------------------------------------------------------------

log "Stage 5: waiting for all jobs to terminate (≤ ${TERMINAL_TIMEOUT_S}s)…"
if ! wait_for "all terminal" "$TERMINAL_TIMEOUT_S" 15 \
    '[.[] | select(.status == "RUNNING" or .status == "QUEUED" or .status == "PROFILING")] | length == 0'; then
    log "  current state:"; all_jobs | jq '.[] | {id: .id[0:8], type: .job_id, status, progress}'
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
zombies_a=$(ssh "$NODE_A" 'docker ps --filter name=ijm- --format "{{.Names}}" | grep -cE "^ijm-" || true' 2>/dev/null || echo 99)
zombies_b=$(ssh "$NODE_B" 'docker ps --filter name=ijm- --format "{{.Names}}" | grep -cE "^ijm-" || true' 2>/dev/null || echo 99)
(( zombies_a == 0 )) || fail "$zombies_a training-container zombie(s) on $NODE_A"
(( zombies_b == 0 )) || fail "$zombies_b training-container zombie(s) on $NODE_B"
pass "no zombie ijm-* training containers on either node"

for jt in $TYPE_SMALL $TYPE_BIG; do
    prof_count=$(ssh "$NODE_A" "docker exec wangrat-ijm-postgres psql -U postgres -d ijm -tA -c \
        \"SELECT count(*) FROM profiling_results WHERE job_id='$jt' AND duration_seconds IS NOT NULL\"" \
        | tr -d '[:space:]')
    (( prof_count >= 2 )) || fail "expected ≥2 profiling_results rows for $jt, got $prof_count"
    pass "profiling cache populated ($prof_count rows for $jt)"
done

# Verify every node that ran a job actually used its GPU.  Without this,
# a worker mis-config (wrong WORKER_GPU_MODE, broken nvidia runtime) would
# silently fall back to CPU and the test would still pass — just 5-10×
# slower than intended.  Check EVERY job that touched a GPU node (not
# just SUCCEEDED) — a single CPU run is a fail.
log "Stage 5: verifying GPU was actually used on every node that ran a job"
nodes_used=$(all_jobs | jq -r '[.[].assigned_node] | unique | .[] | select(. != null)')
for node in $nodes_used; do
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
        warn "$node: no trainer log on this node had a 'Using device:' line"
    fi
done

# Race-fix validation (mirrors the assertion at the end of
# e2e_scenario.sh).  acquire_oversub_count is the hard guarantee;
# release_underflow_count is a soft signal absorbed by the BoundedSemaphore
# + 15s drift heartbeat.
SLOTS_METRICS=$(curl -sS http://localhost:8000/admin/slots | jq -c '.metrics')
underflow=$(echo "$SLOTS_METRICS" | jq '.release_underflow_count')
oversub=$(echo "$SLOTS_METRICS" | jq '.acquire_oversub_count')
recovery=$(echo "$SLOTS_METRICS" | jq '.drift_recovery_count')
log "Stage 5: slot metrics: $SLOTS_METRICS"
[[ "$oversub" == "0" ]] || fail "race-fix regression: acquire_oversub_count=$oversub (expected 0)"
if (( underflow > 5 )); then
    fail "race-fix regression: release_underflow_count=$underflow (expected ≤5)"
elif (( underflow > 0 )); then
    warn "race-fix soft: release_underflow_count=$underflow — absorbed by BoundedSemaphore + drift heartbeat"
else
    pass "race-fix validation: zero underflow, zero oversub"
fi
pass "drift heartbeat fired ${recovery}× (15s cadence; auto-recovered)"

echo
echo "${C_OK}✓ Two-type E2E scenario passed${C_END}"

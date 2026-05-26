"""Client for the GPUspb external optimizer service.

Translates IJM's active jobs, profiling results, and cluster state into the
format expected by POST /optimizer/v5.  Sends ``currentScheduling`` so the
optimizer can decide whether to preempt running jobs for more urgent ones.
"""

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import httpx
import psycopg
from shared.constants import JobStatus

from src.cluster import cluster
from src.constants import DEFAULT_EPOCHS_TOTAL, DEFAULT_JOB_PRIORITY, PRIORITY_MAX, PRIORITY_MIN
from src.models import NodeConfig

logger = logging.getLogger(__name__)

OPTIMIZER_URL: str | None = os.getenv("OPTIMIZER_URL")

# Gate the per-round DIAG INFO logs (OPT GATE/REQ/RESP/PARSE/CLASSIFY) so they
# don't flood the API log in steady state.  Set OPTIMIZER_VERBOSE=1 (or any
# non-zero int) to re-enable them when debugging scheduler decisions.
_OPT_DIAG: bool = bool(int(os.environ.get("OPTIMIZER_VERBOSE", "0") or "0"))

_PROGRESS_RE = re.compile(r"^(\d+)/(\d+)$")


@dataclass
class Assignment:
    """Optimizer's decision for one job."""

    instance_id: str
    node_id: str
    gpu_config: dict[str, int]


@dataclass
class OptimizerResult:
    """Full result from the optimizer, including preemption decisions."""

    assignments: list[Assignment] = field(default_factory=list)
    preempt: list[str] = field(default_factory=list)  # instance_ids to stop
    estimated_cost: float = 0.0


def _format_time(dt: datetime) -> str:
    # GPUspb wants "Mon 04 May 2026, 12.34.56".  RFC 2822 mandates English
    # weekday/month names, so email.utils.format_datetime is locale-independent
    # by spec — unlike strftime, which goes through the C library's LC_TIME.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    weekday, day, month, year, hms, _tz = format_datetime(dt).replace(",", "").split()
    h, m, s = hms.split(":")
    return f"{weekday} {day} {month} {year}, {h}.{m}.{s}"


def _parse_progress(progress: str | None) -> int:
    """Parse '5/20' → 5 (current epoch). Returns 0 if not parseable."""
    if not progress:
        return 0
    m = _PROGRESS_RE.match(progress)
    return int(m.group(1)) if m else 0


def _build_nodes_payload(
    node_gpu_usage: dict[str, dict[str, int]],
) -> dict[str, dict[str, Any]]:
    """Build the optimizer 'nodes' dict from cluster config and current usage.

    ``free_nGPUs`` reflects actual availability (total minus running jobs).
    The optimizer uses this together with ``currentScheduling`` to decide
    assignments — ``free_nGPUs`` must already subtract currentScheduling usage.
    """
    nodes: dict[str, dict[str, Any]] = {}
    for raw in cluster.nodes:
        node = NodeConfig.model_validate(raw)
        used = node_gpu_usage.get(node.id, {})
        for res in node.resources:
            free = max(0, res.gpu_count - used.get(res.gpu_type, 0))
            entry_id = f"{node.id}_{res.gpu_type}" if len(node.resources) > 1 else node.id
            nodes[entry_id] = {
                "GPUtype": res.gpu_type,
                "free_nGPUs": free,
                "total_nGPUs": res.gpu_count,
                "cost": node.cost,
            }
    return nodes


def _build_node_map(nodes_payload: dict[str, dict[str, Any]]) -> dict[str, tuple[str, str]]:
    """Build reverse mapping: optimizer node ID → (real node ID, gpu_type)."""
    node_map: dict[str, tuple[str, str]] = {}
    real_ids = {raw.get("id") for raw in cluster.nodes}
    for entry_id, entry in nodes_payload.items():
        real_id = entry_id.rsplit("_", 1)[0] if "_" in entry_id else entry_id
        if real_id in real_ids:
            node_map[entry_id] = (real_id, entry["GPUtype"])
    return node_map


def _opt_node_for_real(real_node_id: str, gpu_type: str, nodes_payload: dict[str, dict[str, Any]]) -> str | None:
    """Map a real node_id + gpu_type back to the optimizer's node entry ID."""
    # Direct match
    if real_node_id in nodes_payload and nodes_payload[real_node_id].get("GPUtype") == gpu_type:
        return real_node_id
    # Virtual entry for mixed nodes
    virtual = f"{real_node_id}_{gpu_type}"
    if virtual in nodes_payload:
        return virtual
    return None


async def optimize(
    conn: psycopg.AsyncConnection[Any],
    node_gpu_usage: dict[str, dict[str, int]],
) -> OptimizerResult:
    """Call the optimizer with ALL active jobs and currentScheduling.

    Returns assignments (what should run) and preempt list (what should stop).
    On error, returns an empty result; the next scheduler round will retry.
    """
    if not OPTIMIZER_URL:
        return OptimizerResult()

    # Fetch ALL active jobs (QUEUED + RUNNING + PROFILING on non-profiling nodes)
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT id, job_id, status, priority, deadline, created_at, "
            "epochs_total, assigned_node, assigned_gpu_config, progress "
            "FROM jobs WHERE status IN (%s, %s, %s) AND is_profiling_run = FALSE "
            "ORDER BY created_at ASC",
            (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.PROFILING),
        )
        active_jobs = await cur.fetchall()

    if not active_jobs:
        return OptimizerResult()

    # Batch-fetch profiling results
    type_ids = list({row[1] for row in active_jobs})
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT job_id, gpu_config, duration_seconds FROM profiling_results "
            "WHERE job_id = ANY(%s) AND duration_seconds IS NOT NULL",
            (type_ids,),
        )
        all_profiling = await cur.fetchall()

    profiling_by_type: dict[str, list[tuple[dict[str, int], float]]] = {}
    for type_id, gpu_config, duration in all_profiling:
        profiling_by_type.setdefault(type_id, []).append((gpu_config, duration))

    # Per-instance profile-owing gate.  An instance is "profile-owing" if it
    # hasn't completed its ``configs_per_job`` profile budget AND the type
    # still has unprofiled configs the instance could claim.  Profile-owing
    # instances are NEVER placed by the optimizer — they wait in QUEUED for
    # the profiling scheduler / Phase 1c to grant them a profile slot.
    #
    # Without this filter, once any one config for a type has a profile row
    # the optimizer would happily standard-place the remaining same-type
    # submissions on the profiled config, defeating "profile must come first"
    # for every instance.
    from src.profiling import scheduler  # local import to avoid cycle
    from src.utils.gpu import config_key

    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT instance_id, COUNT(*) FROM profiling_results WHERE instance_id = ANY(%s) GROUP BY instance_id",
            ([row[0] for row in active_jobs],),
        )
        profiled_count_by_instance: dict[str, int] = {row[0]: row[1] for row in await cur.fetchall()}

    # All configs the system would consider profiling; subtract per-type
    # profiled set to learn which types still have unprofiled candidates.
    all_configs = scheduler.get_valid_configurations()
    all_config_keys = {config_key(c) for c in all_configs}
    profiled_keys_by_type: dict[str, set[str]] = {}
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT DISTINCT job_id, gpu_config FROM profiling_results WHERE job_id = ANY(%s)",
            (type_ids,),
        )
        for tid, cfg in await cur.fetchall():
            profiled_keys_by_type.setdefault(tid, set()).add(config_key(cfg))
    type_has_unprofiled: dict[str, bool] = {
        tid: bool(all_config_keys - profiled_keys_by_type.get(tid, set())) for tid in type_ids
    }

    nodes_payload = _build_nodes_payload(node_gpu_usage)
    if not nodes_payload:
        return OptimizerResult()

    # Build jobs payload + currentScheduling
    jobs_payload: dict[str, dict[str, Any]] = {}
    current_scheduling: dict[str, dict[str, Any]] = {}
    # Per-(opt-node) running tally used to enforce the invariant
    # sum(currentScheduling per node) <= total_nGPUs.
    current_scheduling_used: dict[str, int] = {}

    for row in active_jobs:
        instance_id = row[0]
        job_type_id = row[1]
        status = row[2]
        priority, deadline, created_at = row[3], row[4], row[5]
        epochs_total = row[6]
        assigned_node, assigned_gpu_config, progress = row[7], row[8], row[9]

        profiling_rows = profiling_by_type.get(job_type_id)
        if not profiling_rows:
            continue  # No profiling data yet — skip until profiling completes

        # Per-instance profile gate: skip instances that still owe a profile
        # and could still claim one (type has unprofiled configs left).
        owes_profile = profiled_count_by_instance.get(instance_id, 0) < scheduler.configs_per_job
        if owes_profile and type_has_unprofiled.get(job_type_id, False):
            if _OPT_DIAG:
                logger.info(
                    "OPT GATE skip job=%s owes_profile=%s type_has_unprofiled=%s",
                    instance_id[:8],
                    owes_profile,
                    type_has_unprofiled.get(job_type_id, False),
                )
            continue

        total_epochs = epochs_total or DEFAULT_EPOCHS_TOTAL

        # Use progress regardless of status: a job that was preempted-then-
        # requeued is QUEUED with a real `progress` value and a checkpoint at
        # epoch N — treating it as fresh would inflate the optimizer's cost
        # and deadline projections for resumed jobs.
        current_epoch = _parse_progress(progress)
        remaining_epochs = max(1, total_epochs - current_epoch)

        # Build ProfilingData with PER-EPOCH execution times.
        # ``duration`` is the mean per-epoch time (warmup excluded), recorded
        # by the worker after a profiling run.  We send it as-is; the GPUspb
        # wrapper multiplies by ``Epochs``:
        #   run_webService.py:213-215  sel_time = ProfilingData[gpu][n]
        #                              sel_time *= int(job_data["Epochs"])
        # Pre-multiplying here would double-count epochs and inflate the
        # optimizer's execution_time / tardiness by remaining_epochs× (~20×
        # for a freshly-submitted 20-epoch job), wrecking placement decisions.
        profiling_data: dict[str, dict[str, float]] = {}
        for gpu_config, duration in profiling_rows:
            for gpu_type, num_gpus in gpu_config.items():
                profiling_data.setdefault(gpu_type, {})[str(num_gpus)] = duration

        dl = deadline or created_at.replace(tzinfo=UTC) + timedelta(days=365)
        jobs_payload[instance_id] = {
            "SubmissionTime": _format_time(created_at),
            "Deadline": _format_time(dl),
            "Priority": max(PRIORITY_MIN, min(PRIORITY_MAX, priority or DEFAULT_JOB_PRIORITY)),
            "Epochs": str(remaining_epochs),
            "ProfilingData": profiling_data,
        }

        # currentScheduling represents jobs the API has placed (assigned_node
        # + assigned_gpu_config set) and that aren't profile-owing.  The
        # optimizer's update_nGPUs() adds these GPUs back to its capacity to
        # allow relocation — it then trusts the resulting nGPUs as the per-
        # node cap.  So sum(currentScheduling per node) + free_nGPUs MUST
        # equal the real total_nGPUs; otherwise the optimizer's view of
        # capacity drifts and we get over- or under-subscription.
        #
        # Status filter includes QUEUED so just-dispatched jobs (semaphore
        # acquired, /run sent, worker hasn't flipped status→RUNNING yet) are
        # treated as occupying their assigned slot.  Without this, the ~5 s
        # window between API-dispatch and worker-RUNNING leaves the row
        # subtracted from free_nGPUs (via get_node_gpu_usage) but not added
        # back here — a phantom 1-GPU shortage that makes the optimizer drop
        # a *different* co-located peer to "free room" that already exists.
        # Stale QUEUED-with-assignment from a failed dispatch is age-gated by
        # _STUCK_DISPATCH_THRESHOLD_S (30 s), but the reaper that acts on it
        # only runs from _schedule_waiting_jobs (NOTIFY-driven) or from the
        # 60-min _queue_watcher heartbeat — so the true upper bound is
        # 30 s **plus** the time to the next scheduler wake, which can be up
        # to ~1 h in a fully idle cluster.  Practically this is still much
        # better than the alternative (the always-firing 5 s spurious
        # cascade), since reserving an unused slot only delays unrelated
        # placements; the row itself is fine.  If this bound becomes
        # problematic, the right fix is a periodic reaper, not reverting
        # this filter.
        #
        # We track per-node usage as we build currentScheduling and skip
        # entries that would push the sum past total_nGPUs.  Excess jobs stay
        # in jobs_payload (without a currentScheduling entry) so the optimizer
        # treats them as fresh placements rather than fixed.
        if (
            assigned_node
            and assigned_gpu_config
            and status
            in (
                JobStatus.RUNNING,
                JobStatus.PROFILING,
                JobStatus.QUEUED,
            )
        ):
            for gpu_type, n_gpus in assigned_gpu_config.items():
                opt_node = _opt_node_for_real(assigned_node, gpu_type, nodes_payload)
                if opt_node is None:
                    break
                total = nodes_payload[opt_node]["total_nGPUs"]
                already = current_scheduling_used.get(opt_node, 0)
                if already + n_gpus > total:
                    # Would over-cap this node; drop from currentScheduling.
                    break
                current_scheduling[instance_id] = {
                    "node": opt_node,
                    "GPUtype": gpu_type,
                    "nGPUs": n_gpus,
                }
                current_scheduling_used[opt_node] = already + n_gpus
                break  # Only first GPU type for currentScheduling entry

    if not jobs_payload:
        return OptimizerResult()

    # `free_nGPUs` (built from node_gpu_usage) already subtracts ALL assigned
    # jobs — standard AND profiling.  After update_nGPUs adds back only the
    # standard jobs in currentScheduling (profiling jobs are intentionally not
    # included so the optimizer treats their slots as fixed), the optimizer's
    # effective capacity = total - profiling_used, leaving room for new
    # standard placements while reserving profiling-occupied slots.
    # The cap loop above guarantees sum(currentScheduling per node) ≤ total,
    # so the invariant holds even if the DB has stale over-cap state.

    request_body: dict[str, Any] = {
        "jobs": jobs_payload,
        "nodes": nodes_payload,
        "GPUcosts": cluster.gpu_energy_costs,
        "currentTime": _format_time(datetime.now(UTC)),
        "method": os.getenv("OPTIMIZER_METHOD", "RG"),
    }
    if current_scheduling:
        request_body["currentScheduling"] = current_scheduling
    # Per-job assignment reasoning gets written to the optimizer's per-call
    # log file at ``/opt/code-optimizer/build/logs/log_<ts>.log`` inside the
    # ``ijm-optimizer`` container.  Levels: 0=cost only (default), 1=high-
    # level, 2=per-job, 3=heuristic internals.  Set OPTIMIZER_VERBOSE to
    # raise the level when debugging churn or unexpected drops.
    opt_verbose = int(os.getenv("OPTIMIZER_VERBOSE", "0"))
    if opt_verbose:
        request_body["verbose"] = opt_verbose

    # DIAG: per-round payload trace.  Gated by OPTIMIZER_VERBOSE so steady-
    # state runs stay quiet; lists each node's free GPUs by type, each job's
    # (id[:8], prio, epochs, profiling-keys), and which jobs claimed a
    # currentScheduling slot vs being treated as fresh.
    if _OPT_DIAG:
        _diag_nodes = {
            n: {k: v for k, v in cfg.items() if k in ("GPUtype", "nGPUs", "total_nGPUs")}
            for n, cfg in nodes_payload.items()
        }
        _diag_jobs = [
            (jid[:8], jp.get("Priority"), jp.get("Epochs"), list(jp.get("ProfilingData", {}).keys()))
            for jid, jp in jobs_payload.items()
        ]
        _diag_cursched = [(jid[:8], v["node"], v["GPUtype"], v["nGPUs"]) for jid, v in current_scheduling.items()]
        logger.info(
            "OPT REQ nodes=%s | currentSched=%s | jobs=%s",
            _diag_nodes,
            _diag_cursched,
            _diag_jobs,
        )

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{OPTIMIZER_URL}/optimizer/v5", json=request_body)
            resp.raise_for_status()
            result = resp.json()
    except Exception:
        logger.exception("Optimizer call failed")
        return OptimizerResult()

    # DIAG: raw response from the C++ optimizer, BEFORE any filtering/parsing
    # decisions on our side.  Tells us which jobs the heuristic placed where
    # and which it left out entirely.  Use this to distinguish "optimizer
    # didn't return a placement" from "we dropped the placement during parse".
    if _OPT_DIAG:
        logger.info("OPT RESP cost=%.4f jobs=%s", result.get("estimated_cost", 0.0), result.get("jobs", {}))

    # Parse response
    node_map = _build_node_map(nodes_payload)
    assigned_ids: set[str] = set()
    assignments: list[Assignment] = []

    for opt_job_id, opt_assignment in result.get("jobs", {}).items():
        opt_node = opt_assignment.get("node", "")
        n_gpus = opt_assignment.get("nGPUs", 0)
        tardiness = float(opt_assignment.get("expected_tardiness", 0) or 0)
        if tardiness > 0:
            logger.warning("Job %s will miss deadline by %.1f hours", opt_job_id[:8], tardiness)
        if not opt_node or n_gpus <= 0 or opt_node not in node_map:
            # DIAG: distinguish "no placement returned" (opt_node empty) from
            # "placement returned for a node we don't recognise".  The latter
            # would be a node_map / nodes_payload key mismatch — a parser bug
            # silently dropping valid placements.
            if _OPT_DIAG:
                logger.info(
                    "OPT PARSE drop job=%s opt_node=%r n_gpus=%s in_node_map=%s",
                    opt_job_id[:8],
                    opt_node,
                    n_gpus,
                    opt_node in node_map,
                )
            continue
        real_node_id, gpu_type = node_map[opt_node]
        assignments.append(Assignment(instance_id=opt_job_id, node_id=real_node_id, gpu_config={gpu_type: n_gpus}))
        assigned_ids.add(opt_job_id)

    # Jobs to preempt: anything in currentScheduling that the optimizer either
    # dropped entirely OR moved to a different (node, gpu_type, nGPUs).  After
    # preemption, app.py re-queues them with assigned_node=NULL so Phase 1b's
    # `WHERE assigned_node IS NULL` clause can apply the optimizer's new
    # placement.
    assignment_by_id = {a.instance_id: a for a in assignments}

    preempt: list[str] = []
    keep_assignments: list[Assignment] = []
    # DIAG: per-assignment classification — "kept-same" (no-op), "migrate"
    # (preempt+place), or "new" (was in QUEUED).  Sums let us cross-check
    # the "Optimizer: N assignment(s)" headline against what actually got
    # acted on, and against the raw OPT RESP count above.
    _diag_kept_same: list[str] = []
    _diag_migrate: list[str] = []
    _diag_new: list[str] = []
    for a in assignments:
        existing = current_scheduling.get(a.instance_id)
        if existing is None:
            keep_assignments.append(a)
            _diag_new.append(a.instance_id[:8])
            continue
        cur_real = existing["node"].rsplit("_", 1)[0] if "_" in existing["node"] else existing["node"]
        new_gpu_type = next(iter(a.gpu_config))
        new_n = a.gpu_config[new_gpu_type]
        same = cur_real == a.node_id and existing["GPUtype"] == new_gpu_type and existing["nGPUs"] == new_n
        if same:
            # Same exact slot — keep the row where it is.  Phase 1b's UPDATE
            # WHERE assigned_node IS NULL won't match anyway (row already has
            # a node), so dropping the assignment is the only correct action.
            _diag_kept_same.append(a.instance_id[:8])
            continue
        # Migration: preempt + let Phase 1b reapply with new placement.
        preempt.append(a.instance_id)
        keep_assignments.append(a)
        _diag_migrate.append(a.instance_id[:8])
    # Anything in currentScheduling that the optimizer didn't include at all → preempt.
    _diag_drop_preempt: list[str] = []
    for job_id in current_scheduling:
        if job_id not in assignment_by_id:
            preempt.append(job_id)
            _diag_drop_preempt.append(job_id[:8])
    if _OPT_DIAG:
        logger.info(
            "OPT CLASSIFY new=%s migrate=%s kept-same=%s drop-preempt=%s",
            _diag_new,
            _diag_migrate,
            _diag_kept_same,
            _diag_drop_preempt,
        )

    assignments = keep_assignments

    logger.info(
        "Optimizer: %d assignment(s), %d preemption(s), cost=%.2f",
        len(assignments),
        len(preempt),
        result.get("estimated_cost", 0),
    )
    if preempt:
        logger.info("Optimizer wants to preempt: %s", [jid[:8] for jid in preempt])

    return OptimizerResult(assignments=assignments, preempt=preempt, estimated_cost=result.get("estimated_cost", 0))

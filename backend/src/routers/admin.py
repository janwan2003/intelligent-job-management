"""Operator-facing admin endpoints — slot accounting, dispatch tasks, manual reconcile."""

from typing import Any

from fastapi import APIRouter, HTTPException

import src.state as state

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/slots")
async def slot_state() -> dict[str, Any]:
    """Snapshot of NodeSlots: in-memory ``_used`` vs DB-authoritative count.

    A drift report (``per_node[node].drift = true``) means the in-memory
    semaphore disagrees with what the DB says is RUNNING/PROFILING.  The
    drift watcher reconciles automatically every 15 s (IJM_DRIFT_HEARTBEAT_S); this endpoint is
    for on-demand inspection.
    """
    if state.node_slots is None:
        raise HTTPException(503, "NodeSlots not initialised")
    drift = await state.node_slots.detect_drift(state.get_conn)
    out: dict[str, dict[str, Any]] = {}
    for node_id in state.node_slots._sems:  # noqa: SLF001 — admin/debug introspection
        mem_used, db_used = drift.get(node_id, (None, None))
        out[node_id] = {
            "total": state.node_slots.total(node_id),
            "available": state.node_slots.available(node_id),
            "used_mem": state.node_slots._used.get(node_id, 0),  # noqa: SLF001
            "used_db": db_used if db_used is not None else state.node_slots._used.get(node_id, 0),  # noqa: SLF001
            "drift": node_id in drift,
        }
    return {"per_node": out, "metrics": state.node_slots.metrics()}


@router.post("/reconcile-slots", status_code=200)
async def reconcile_slots() -> dict[str, Any]:
    """Force NodeSlots to match DB-authoritative state.  Idempotent."""
    if state.node_slots is None:
        raise HTTPException(503, "NodeSlots not initialised")
    deltas = await state.node_slots.recover_from_drift(state.get_conn)
    return {"status": "reconciled", "deltas": deltas}


@router.get("/dispatch-tasks")
async def dispatch_tasks() -> list[dict[str, Any]]:
    """List in-flight ``_dispatch_when_slot_free`` tasks.

    A task that's been alive for >30 s is the reaper's responsibility (it
    cancels stuck tasks).  An unbounded growth here would indicate the
    cancellation path is broken.
    """
    return [
        {
            "instance_id": jid[:8],
            "name": t.get_name(),
            "done": t.done(),
            "cancelled": t.cancelled() if t.done() else False,
        }
        for jid, t in state.dispatch_tasks.items()
    ]

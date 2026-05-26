"""Shared constants for the IJM project.

Imported by the backend to ensure consistent status values and defaults.
"""

from enum import StrEnum


class JobStatus(StrEnum):
    """Job lifecycle states."""

    QUEUED = "QUEUED"
    PROFILING = "PROFILING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PREEMPTED = "PREEMPTED"


# Defaults shared across modules
DEFAULT_PROFILING_EPOCHS = 3

# Data directories & file names
RUNS_DIR = "runs"
OUTPUT_LOG_FILENAME = "output.log"

# PostgreSQL NOTIFY channel used by workers to wake the API scheduler immediately
PG_NOTIFY_SCHEDULE = "ijm_schedule"

# PostgreSQL NOTIFY channel used by workers to signal that a GPU slot just
# became free (job completed / failed / auto-preempted / user-stopped).
# Payload is "<node_id>:<n_gpus>:<reason>"; the API's listener releases
# that many permits on the per-node ``NodeSlots`` semaphore, then wakes
# the optimiser iff the freed slot is the result of an external event
# (TERMINAL / USER_STOP / ORPHAN_DRAIN).  AUTO_PREEMPT events are the
# API's own /stop calls issued as part of an in-flight plan; the plan's
# own dispatch step will pick up the just-released slot, so re-running
# the optimiser there would just thrash the plan it is already
# executing.  Legacy 2-field payloads ("<node>:<count>", emitted by
# pre-fix workers) are still parsed and default to TERMINAL — safe
# (wakes the scheduler) and forwards-compatible during rolling deploy.
PG_NOTIFY_SLOT_FREED = "ijm_slot_freed"


class SlotFreedReason(StrEnum):
    """Why a GPU slot was just freed.  Drives whether the API's slot
    listener should wake the optimiser after releasing the slot count."""

    TERMINAL = "terminal"  # SUCCEEDED / FAILED — job ended naturally
    USER_STOP = "user_stop"  # /stop?reason=user — UI / human action
    AUTO_PREEMPT = "auto_preempt"  # /stop?reason=auto — API's own plan
    ORPHAN_DRAIN = "orphan_drain"  # worker zombie / reconcile cleanup

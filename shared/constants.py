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
# Payload is "<node_id>:<n_gpus>"; the API's listener releases that many
# permits on the per-node ``NodeSlots`` semaphore.  Separate from
# ``PG_NOTIFY_SCHEDULE`` because the consumers are different — schedule
# wakes a full optimizer pass, slot-freed just bumps a permit.
PG_NOTIFY_SLOT_FREED = "ijm_slot_freed"

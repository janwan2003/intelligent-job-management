"""Constants for the IJM backend."""

import os
from enum import StrEnum

from shared.constants import JobStatus

# ---------------------------------------------------------------------------
# Job status sets
# ---------------------------------------------------------------------------

# Statuses that can be stopped (includes PROFILING — user can cancel mid-profile)
STOPPABLE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.PROFILING, JobStatus.RUNNING})
# Statuses that can be resumed
RESUMABLE_STATUSES = frozenset({JobStatus.PREEMPTED, JobStatus.FAILED})

# ---------------------------------------------------------------------------
# Node statuses
# ---------------------------------------------------------------------------


class NodeStatusEnum(StrEnum):
    """Cluster node states."""

    IDLE = "idle"
    BUSY = "busy"


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_JOB_PRIORITY = 3
PRIORITY_MIN = 1
PRIORITY_MAX = 5

DEFAULT_EPOCHS_TOTAL = 20
DEFAULT_PROFILING_CONFIGS_PER_JOB = 1

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ijm"

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

CORS_ALLOWED_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")]

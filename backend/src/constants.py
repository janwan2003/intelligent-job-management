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
# How many profile configs a single job *instance* may explore before it
# switches to standard runs.  The scheduler still picks at most one config
# per ``schedule_job`` call (= one profile run at a time per instance).
# Default 2: with two instances per type submitted at the start of a
# scenario (2-types e2e), every type gets its full (type × config) profile
# matrix filled during the initial profile sweep — 2 instances × 2 configs
# each = 4 profile rows covering all 4 valid configs (P600×1, P600×2,
# A40×1, A40×2).  After the sweep no new profile is needed for that type,
# so later urgent submissions of the same type can standard-run
# immediately (no profile-gate stall).  Override via
# ``PROFILING_CONFIGS_PER_JOB``: set to 1 for strict one-profile-per-
# instance semantics (profile coverage then depends on having enough
# instances), or higher to push coverage even further per submission.
DEFAULT_PROFILING_CONFIGS_PER_JOB = 2

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ijm"

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

# Filter out empty strings so an empty/whitespace-only ``CORS_ORIGINS`` env
# var doesn't yield ``[""]`` (which CORSMiddleware treats as a wildcard match
# against any origin string).
CORS_ALLOWED_ORIGINS = [
    o for o in (s.strip() for s in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")) if o
]

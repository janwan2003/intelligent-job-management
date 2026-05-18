"""Constants for the IJM backend."""

import os
from enum import StrEnum

from shared.constants import JobStatus

STOPPABLE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.PROFILING, JobStatus.RUNNING})
RESUMABLE_STATUSES = frozenset({JobStatus.PREEMPTED, JobStatus.FAILED})


class NodeStatusEnum(StrEnum):
    IDLE = "idle"
    BUSY = "busy"


DEFAULT_JOB_PRIORITY = 3
PRIORITY_MIN = 1
PRIORITY_MAX = 5

DEFAULT_EPOCHS_TOTAL = 20
# Profile configs each job instance may explore before switching to standard
# runs.  Override via ``PROFILING_CONFIGS_PER_JOB``.
DEFAULT_PROFILING_CONFIGS_PER_JOB = 2

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ijm"

# Filter out empties so an empty CORS_ORIGINS env doesn't yield [""], which
# CORSMiddleware treats as a wildcard.
CORS_ALLOWED_ORIGINS = [
    o for o in (s.strip() for s in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")) if o
]

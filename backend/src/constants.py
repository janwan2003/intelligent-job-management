"""Constants for the IJM backend."""

from enum import StrEnum

from shared.constants import (
    NATS_STREAM_NAME as NATS_STREAM_NAME,
)
from shared.constants import (
    NATS_SUBJECT_PROFILING_COMPLETE as NATS_SUBJECT_PROFILING_COMPLETE,
)
from shared.constants import (
    NATS_SUBJECT_STOP_REQUESTED as NATS_SUBJECT_STOP_REQUESTED,
)
from shared.constants import (
    NATS_SUBJECT_SUBMITTED as NATS_SUBJECT_SUBMITTED,
)
from shared.constants import (
    NATS_SUBJECTS_PATTERN as NATS_SUBJECTS_PATTERN,
)
from shared.constants import (
    JobStatus as JobStatus,
)

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
# NATS configuration (backend-specific)
# ---------------------------------------------------------------------------

NATS_CONSUMER_PROFILING = "api-profiling-complete"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_JOB_PRIORITY = 3
PRIORITY_MIN = 1
PRIORITY_MAX = 5

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ijm"
DEFAULT_NATS_URL = "nats://localhost:4222"

# ---------------------------------------------------------------------------
# Data directories & file names
# ---------------------------------------------------------------------------

RUNS_DIR = "runs"
OUTPUT_LOG_FILENAME = "output.log"

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

CORS_ALLOWED_ORIGINS = ["http://localhost:5173"]

"""Shared constants for the IJM backend."""

from enum import StrEnum

# ---------------------------------------------------------------------------
# Job statuses
# ---------------------------------------------------------------------------


class JobStatus(StrEnum):
    """Job lifecycle states."""

    QUEUED = "QUEUED"
    PROFILING = "PROFILING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PREEMPTED = "PREEMPTED"


# Backward-compat aliases
STATUS_QUEUED = JobStatus.QUEUED
STATUS_PROFILING = JobStatus.PROFILING
STATUS_RUNNING = JobStatus.RUNNING
STATUS_SUCCEEDED = JobStatus.SUCCEEDED
STATUS_FAILED = JobStatus.FAILED
STATUS_PREEMPTED = JobStatus.PREEMPTED

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


# Backward-compat aliases
NODE_STATUS_IDLE = NodeStatusEnum.IDLE
NODE_STATUS_BUSY = NodeStatusEnum.BUSY

# ---------------------------------------------------------------------------
# NATS configuration
# ---------------------------------------------------------------------------

NATS_STREAM_NAME = "JOBS"
NATS_SUBJECTS_PATTERN = "jobs.>"
NATS_SUBJECT_SUBMITTED = "jobs.submitted"
NATS_SUBJECT_STOP_REQUESTED = "jobs.stop_requested"
NATS_SUBJECT_PROFILING_COMPLETE = "jobs.profiling_complete"

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

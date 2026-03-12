"""Shared constants for the IJM worker.

Mirrors the backend constants to keep the event-driven contract consistent.
"""

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

# Statuses eligible for execution (worker picks up QUEUED jobs)
RUNNABLE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.RUNNING})

# ---------------------------------------------------------------------------
# NATS configuration
# ---------------------------------------------------------------------------

NATS_STREAM_NAME = "JOBS"
NATS_SUBJECTS_PATTERN = "jobs.>"
NATS_SUBJECT_SUBMITTED = "jobs.submitted"
NATS_SUBJECT_STOP_REQUESTED = "jobs.stop_requested"
NATS_SUBJECT_RESUME_REQUESTED = "jobs.resume_requested"
NATS_SUBJECT_PROFILING_COMPLETE = "jobs.profiling_complete"

# NATS consumer durable names
CONSUMER_SUBMITTED = "worker-submitted"
CONSUMER_STOP = "worker-stop"
CONSUMER_RESUME = "worker-resume"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_NATS_URL = "nats://nats:4222"
DEFAULT_PROFILING_STEPS = 100

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

CONTAINER_NAME_PREFIX = "ijm-"
JOB_ID_DISPLAY_LENGTH = 8
DOCKER_CMD_TIMEOUT_SECONDS = 120
DOCKER_STOP_GRACE_SECONDS = 30

# ---------------------------------------------------------------------------
# Data directories & mount paths
# ---------------------------------------------------------------------------

CHECKPOINT_DIR = "checkpoints"
RUNS_DIR = "runs"
OUTPUT_LOG_FILENAME = "output.log"
CHECKPOINT_MOUNT_PATH = "/checkpoints"
RUNS_MOUNT_PATH = "/runs"

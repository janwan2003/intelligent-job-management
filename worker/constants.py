"""Constants for the IJM worker."""

from shared.constants import (
    NATS_STREAM_NAME as NATS_STREAM_NAME,
    NATS_SUBJECT_PROFILING_COMPLETE as NATS_SUBJECT_PROFILING_COMPLETE,
    NATS_SUBJECT_STOP_REQUESTED as NATS_SUBJECT_STOP_REQUESTED,
    NATS_SUBJECT_SUBMITTED as NATS_SUBJECT_SUBMITTED,
    NATS_SUBJECTS_PATTERN as NATS_SUBJECTS_PATTERN,
    JobStatus as JobStatus,
)

# Statuses eligible for execution (worker picks up QUEUED jobs)
RUNNABLE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.RUNNING})

# ---------------------------------------------------------------------------
# NATS configuration (worker-specific)
# ---------------------------------------------------------------------------

NATS_SUBJECT_RESUME_REQUESTED = "jobs.resume_requested"

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

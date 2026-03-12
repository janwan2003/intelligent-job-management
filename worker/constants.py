"""Constants for the IJM worker."""

from shared.constants import (
    DEFAULT_LOG_INTERVAL as DEFAULT_LOG_INTERVAL,
)
from shared.constants import (
    DEFAULT_PROFILING_STEPS as DEFAULT_PROFILING_STEPS,
)
from shared.constants import (
    NATS_STREAM_NAME as NATS_STREAM_NAME,
)
from shared.constants import (
    NATS_SUBJECT_COMPLETED as NATS_SUBJECT_COMPLETED,
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
    OUTPUT_LOG_FILENAME as OUTPUT_LOG_FILENAME,
)
from shared.constants import (
    RUNS_DIR as RUNS_DIR,
)
from shared.constants import (
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
CHECKPOINT_MOUNT_PATH = "/checkpoints"
RUNS_MOUNT_PATH = "/runs"

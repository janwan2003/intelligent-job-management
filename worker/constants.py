"""Constants for the IJM worker."""

import os

from shared.constants import JobStatus

# Statuses eligible for execution (worker picks up QUEUED jobs)
RUNNABLE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.RUNNING})

# ---------------------------------------------------------------------------
# Node identity
# ---------------------------------------------------------------------------

NODE_ID: str = os.getenv("NODE_ID", "local")
WORKER_PORT: int = int(os.getenv("WORKER_PORT", "8001"))

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

CONTAINER_NAME_PREFIX = "ijm-"
JOB_ID_DISPLAY_LENGTH = 8
DOCKER_CMD_TIMEOUT_SECONDS = 120

# ---------------------------------------------------------------------------
# Data directories & mount paths
# ---------------------------------------------------------------------------

CHECKPOINT_DIR = "checkpoints"
CHECKPOINT_MOUNT_PATH = "/checkpoints"
RUNS_MOUNT_PATH = "/runs"

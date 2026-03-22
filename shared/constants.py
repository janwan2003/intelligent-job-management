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

"""Shared constants for the IJM event contract.

Imported by both the API backend and the worker to ensure they agree on
status values and NATS subject names.
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


# NATS stream / subject names
NATS_STREAM_NAME = "JOBS"
NATS_SUBJECTS_PATTERN = "jobs.>"
NATS_SUBJECT_SUBMITTED = "jobs.submitted"
NATS_SUBJECT_STOP_REQUESTED = "jobs.stop_requested"
NATS_SUBJECT_PROFILING_COMPLETE = "jobs.profiling_complete"

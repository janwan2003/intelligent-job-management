"""Pydantic models and SQL helpers for the IJM backend."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.constants import (
    DEFAULT_JOB_PRIORITY,
    NODE_STATUS_IDLE,
    PRIORITY_MAX,
    PRIORITY_MIN,
)

# ---------------------------------------------------------------------------
# Node models
# ---------------------------------------------------------------------------


class NodeResources(BaseModel):
    """Hardware resources attached to a node (from config JSON)."""

    model_config = ConfigDict(populate_by_name=True)

    gpu_type: str
    gpu_count: int
    memory_per_gpu_gb: int

    def get_available_memory(self) -> int:
        """Return total VRAM across all GPUs (in GB)."""
        return self.gpu_count * self.memory_per_gpu_gb


class NodeConfig(BaseModel):
    """Node definition loaded from config JSON."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    is_for_profiling: bool = Field(default=False, alias="isForProfiling")
    cost: float = 0.0
    resources: list[NodeResources] = Field(default_factory=list)

    def get_available_memory(self) -> int:
        """Return total VRAM across all GPU groups, or 0 if no resources."""
        return sum(r.get_available_memory() for r in self.resources)


class NodeStatus(BaseModel):
    """Node status returned by GET /nodes."""

    id: str
    is_for_profiling: bool
    cost: float
    resources: list[NodeResources] = Field(default_factory=list)
    status: str = NODE_STATUS_IDLE
    current_job_ids: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Scheduler models
# ---------------------------------------------------------------------------


class ScheduleResult(BaseModel):
    """Result of a scheduling decision."""

    mode: str  # "profiling" or "standard"
    gpu_config: dict[str, int] | None = None
    node_id: str | None = None
    estimated_duration: float | None = None
    is_profiling_run: bool = False


# ---------------------------------------------------------------------------
# Job models
# ---------------------------------------------------------------------------


class JobCreate(BaseModel):
    """Job creation request — supports both legacy and ANDREAS formats."""

    model_config = ConfigDict(populate_by_name=True)

    # Legacy fields (still supported)
    image: str | None = None
    command: list[str] | None = None

    # ANDREAS fields
    docker_image: str | None = Field(default=None, alias="dockerImage")
    priority: int = Field(default=DEFAULT_JOB_PRIORITY, alias="Priority", ge=PRIORITY_MIN, le=PRIORITY_MAX)
    deadline: datetime | None = None
    batch_size: int | None = Field(default=None, alias="batchSize")
    profiling_epochs_no: int | None = Field(default=None, alias="profilingEpochsNo")
    epochs_total: int | None = Field(default=None, alias="epochsTotal")
    required_memory_gb: int | None = Field(default=None, alias="requiredMemoryGb")


class Job(BaseModel):
    """Job response model."""

    id: str
    image: str
    command: list[str]
    status: str
    created_at: datetime
    updated_at: datetime
    container_name: str | None = None
    exit_code: int | None = None
    progress: str | None = None
    # ANDREAS extended fields
    priority: int = DEFAULT_JOB_PRIORITY
    deadline: datetime | None = None
    batch_size: int | None = None
    epochs_total: int | None = None
    profiling_epochs_no: int | None = None
    assigned_node: str | None = None
    required_memory_gb: int | None = None
    # Profiling scheduler fields
    assigned_gpu_config: dict[str, int] | None = None
    estimated_duration: float | None = None
    is_profiling_run: bool = False


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

_JOB_COLUMNS = (
    "id, image, command, status, created_at, updated_at, container_name, exit_code, progress, "
    "priority, deadline, batch_size, epochs_total, profiling_epochs_no, "
    "assigned_node, required_memory_gb, "
    "assigned_gpu_config, estimated_duration, is_profiling_run"
)


def _row_to_job(row: tuple[Any, ...]) -> Job:
    """Convert a database row tuple to a Job model."""
    return Job(
        id=row[0],
        image=row[1],
        command=row[2],
        status=row[3],
        created_at=row[4],
        updated_at=row[5],
        container_name=row[6],
        exit_code=row[7],
        progress=row[8] if len(row) > 8 else None,
        priority=row[9] if len(row) > 9 and row[9] is not None else DEFAULT_JOB_PRIORITY,
        deadline=row[10] if len(row) > 10 else None,
        batch_size=row[11] if len(row) > 11 else None,
        epochs_total=row[12] if len(row) > 12 else None,
        profiling_epochs_no=row[13] if len(row) > 13 else None,
        assigned_node=row[14] if len(row) > 14 else None,
        required_memory_gb=row[15] if len(row) > 15 else None,
        assigned_gpu_config=row[16] if len(row) > 16 else None,
        estimated_duration=row[17] if len(row) > 17 else None,
        is_profiling_run=bool(row[18]) if len(row) > 18 and row[18] is not None else False,
    )

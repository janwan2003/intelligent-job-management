"""Pydantic models and SQL helpers for the IJM backend."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.constants import (
    DEFAULT_EPOCHS_TOTAL,
    DEFAULT_JOB_PRIORITY,
    DEFAULT_PROFILING_STEPS,
    PRIORITY_MAX,
    PRIORITY_MIN,
    NodeStatusEnum,
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
    status: str = NodeStatusEnum.IDLE
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
    """Job creation request."""

    model_config = ConfigDict(populate_by_name=True)

    image: str
    command: list[str]
    priority: int = Field(default=DEFAULT_JOB_PRIORITY, alias="Priority", ge=PRIORITY_MIN, le=PRIORITY_MAX)
    deadline: datetime | None = None
    batch_size: int | None = Field(default=None, alias="batchSize")
    profiling_epochs_no: int = Field(default=DEFAULT_PROFILING_STEPS, alias="profilingEpochsNo", ge=1)
    epochs_total: int = Field(default=DEFAULT_EPOCHS_TOTAL, alias="epochsTotal", ge=1)
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
    priority: int = DEFAULT_JOB_PRIORITY
    deadline: datetime | None = None
    batch_size: int | None = None
    epochs_total: int = DEFAULT_EPOCHS_TOTAL
    profiling_epochs_no: int = DEFAULT_PROFILING_STEPS
    assigned_node: str | None = None
    required_memory_gb: int | None = None
    assigned_gpu_config: dict[str, int] | None = None
    estimated_duration: float | None = None
    is_profiling_run: bool = False


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def _row_to_job(row: dict[str, Any]) -> Job:
    """Convert a database row dict to a Job model."""
    return Job(
        id=row["id"],
        image=row["image"],
        command=row["command"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        container_name=row.get("container_name"),
        exit_code=row.get("exit_code"),
        progress=row.get("progress"),
        priority=row.get("priority") or DEFAULT_JOB_PRIORITY,
        deadline=row.get("deadline"),
        batch_size=row.get("batch_size"),
        epochs_total=row.get("epochs_total") or DEFAULT_EPOCHS_TOTAL,
        profiling_epochs_no=row.get("profiling_epochs_no") or DEFAULT_PROFILING_STEPS,
        assigned_node=row.get("assigned_node"),
        required_memory_gb=row.get("required_memory_gb"),
        assigned_gpu_config=row.get("assigned_gpu_config"),
        estimated_duration=row.get("estimated_duration"),
        is_profiling_run=bool(row.get("is_profiling_run")) if row.get("is_profiling_run") is not None else False,
    )

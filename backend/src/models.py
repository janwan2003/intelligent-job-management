"""Pydantic models and SQL helpers for the IJM backend."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator
from shared.constants import DEFAULT_LOG_INTERVAL, DEFAULT_PROFILING_STEPS

from src.constants import (
    DEFAULT_EPOCHS_TOTAL,
    DEFAULT_JOB_PRIORITY,
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


class NodeConfig(BaseModel):
    """Node definition loaded from config JSON."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    is_for_profiling: bool = Field(default=False, alias="isForProfiling")
    cost: float = 0.0
    resources: list[NodeResources] = Field(default_factory=list)


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
    is_profiling_run: bool = False


# ---------------------------------------------------------------------------
# Job models
# ---------------------------------------------------------------------------


class JobCreate(BaseModel):
    """Job creation request."""

    model_config = ConfigDict(populate_by_name=True)

    image: str
    command: list[str] = Field(default_factory=list)
    priority: int = Field(default=DEFAULT_JOB_PRIORITY, alias="Priority", ge=PRIORITY_MIN, le=PRIORITY_MAX)
    deadline: datetime | None = None
    batch_size: int | None = Field(default=None, alias="batchSize")
    profiling_epochs_no: int = Field(default=DEFAULT_PROFILING_STEPS, alias="profilingEpochsNo", ge=1)
    epochs_total: int = Field(default=DEFAULT_EPOCHS_TOTAL, alias="epochsTotal", ge=1)
    required_memory_gb: int | None = Field(default=None, alias="requiredMemoryGb")
    log_interval: int = Field(default=DEFAULT_LOG_INTERVAL, alias="logInterval", ge=1)


_DB_NULLABLE_DEFAULTS = ("priority", "epochs_total", "profiling_epochs_no", "log_interval")


class Job(BaseModel):
    """Job response model — also used to hydrate DB rows via model_validate()."""

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
    is_profiling_run: bool = False
    log_interval: int = DEFAULT_LOG_INTERVAL

    @model_validator(mode="before")
    @classmethod
    def _defaults_for_db_nulls(cls, data: Any) -> Any:
        """Drop NULL DB values for non-optional fields so Pydantic uses field defaults."""
        if isinstance(data, dict):
            for key in _DB_NULLABLE_DEFAULTS:
                if data.get(key) is None:
                    data.pop(key, None)
        return data

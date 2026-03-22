"""Container execution backends (Docker, SLURM, mock)."""

from __future__ import annotations

import asyncio
import logging
import subprocess
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Executor protocol — the interface all backends must implement
# ---------------------------------------------------------------------------


@dataclass
class JobHandle:
    """Opaque reference to a running container/job."""

    container_name: str
    process: Any = field(default=None, repr=False)  # Popen for Docker, job ID for SLURM


class Executor(Protocol):
    """Abstract interface for running training containers."""

    async def run(
        self,
        container_name: str,
        image: str,
        command: list[str],
        volumes: dict[str, str],
        env_vars: dict[str, str],
    ) -> JobHandle:
        """Launch a container and return a handle."""
        ...

    async def kill(self, handle: JobHandle) -> bool:
        """Kill a running container. Returns True if killed successfully."""
        ...

    async def wait(self, handle: JobHandle, timeout: int) -> int:
        """Wait for container to exit. Returns exit code."""
        ...

    def stream_logs(self, handle: JobHandle) -> AsyncIterator[str]:
        """Yield stdout lines from the container."""
        ...

    async def list_running(self, prefix: str) -> set[str]:
        """Return names of running containers matching the prefix."""
        ...


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

DOCKER_CMD_TIMEOUT_SECONDS = 120


def create_executor(name: str = "docker") -> Executor:
    """Create an executor by name: 'docker' or 'mock-slurm'."""
    if name == "docker":
        from src.executors.docker import DockerExecutor

        return DockerExecutor()
    if name == "mock-slurm":
        from src.executors.docker import DockerExecutor
        from src.executors.mock_slurm import MockSlurmExecutor

        return MockSlurmExecutor(DockerExecutor())
    raise ValueError(f"Unknown executor: {name!r}")

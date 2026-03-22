"""Mock SLURM executor — logs SLURM commands but runs Docker locally."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from src.executors import JobHandle
from src.executors.docker import DockerExecutor

logger = logging.getLogger(__name__)


class MockSlurmExecutor:
    """Wraps DockerExecutor but logs what the SLURM command would be.

    Use this to test the full pipeline on a single machine without a real
    SLURM cluster.  Set ``EXECUTOR=mock-slurm`` to activate.
    """

    def __init__(self, docker: DockerExecutor) -> None:
        self._docker = docker

    async def run(
        self,
        container_name: str,
        image: str,
        command: list[str],
        volumes: dict[str, str],
        env_vars: dict[str, str],
    ) -> JobHandle:
        node_id = env_vars.pop("_NODE_ID", "unknown-node")
        gpu_config = env_vars.pop("_GPU_CONFIG", "unknown")
        logger.info(
            "SLURM would run: srun --nodelist=%s --gres=gpu:%s docker run %s",
            node_id,
            gpu_config,
            image,
        )
        return await self._docker.run(container_name, image, command, volumes, env_vars)

    async def kill(self, handle: JobHandle) -> bool:
        logger.info("SLURM would run: scancel (container=%s)", handle.container_name)
        return await self._docker.kill(handle)

    async def wait(self, handle: JobHandle, timeout: int) -> int:
        return await self._docker.wait(handle, timeout)

    async def stream_logs(self, handle: JobHandle) -> AsyncIterator[str]:
        async for line in self._docker.stream_logs(handle):
            yield line

    async def list_running(self, prefix: str) -> set[str]:
        return await self._docker.list_running(prefix)

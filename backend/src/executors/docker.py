"""Docker CLI executor — runs containers via subprocess."""

from __future__ import annotations

import asyncio
import logging
import subprocess
from collections.abc import AsyncIterator
from typing import Any

from src.executors import DOCKER_CMD_TIMEOUT_SECONDS, JobHandle

logger = logging.getLogger(__name__)

CHECKPOINT_MOUNT_PATH = "/checkpoints"
RUNS_MOUNT_PATH = "/runs"


class DockerExecutor:
    """Execute training jobs as Docker containers via the Docker CLI."""

    async def run(
        self,
        container_name: str,
        image: str,
        command: list[str],
        volumes: dict[str, str],
        env_vars: dict[str, str],
    ) -> JobHandle:
        """Launch a Docker container and return a handle with the Popen process."""
        cmd = ["docker", "run", "--rm", "--name", container_name]
        for host_path, container_path in volumes.items():
            cmd += ["-v", f"{host_path}:{container_path}"]
        for key, val in env_vars.items():
            cmd += ["-e", f"{key}={val}"]
        cmd.append(image)
        cmd.extend(command)

        logger.info("Docker run: %s", " ".join(cmd))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        return JobHandle(container_name=container_name, process=process)

    async def kill(self, handle: JobHandle) -> bool:
        """Kill a container immediately (SIGKILL)."""
        result = await self._docker_cmd("kill", handle.container_name)
        return result.returncode == 0

    async def wait(self, handle: JobHandle, timeout: int) -> int:
        """Wait for the container process to exit."""
        process: subprocess.Popen[str] = handle.process
        loop = asyncio.get_running_loop()
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, process.wait),
                timeout=timeout,
            )
        except TimeoutError:
            logger.error("Container %s timed out after %ds, terminating", handle.container_name, timeout)
            process.terminate()
            return await loop.run_in_executor(None, process.wait)

    async def stream_logs(self, handle: JobHandle) -> AsyncIterator[str]:
        """Yield stdout lines from the container process."""
        process: subprocess.Popen[str] = handle.process
        loop = asyncio.get_running_loop()
        stdout = process.stdout
        if stdout is None:
            return
        while True:
            line = await loop.run_in_executor(None, stdout.readline)
            if not line:
                break
            yield line

    async def list_running(self, prefix: str) -> set[str]:
        """Return names of all containers with the given prefix."""
        result = await self._docker_cmd("ps", "-a", "--filter", f"name={prefix}", "--format", "{{.Names}}")
        return set(result.stdout.strip().split("\n")) if result.stdout.strip() else set()

    @staticmethod
    async def _docker_cmd(*args: str, timeout: int = DOCKER_CMD_TIMEOUT_SECONDS) -> subprocess.CompletedProcess[Any]:
        """Run a docker CLI command in a thread."""
        return await asyncio.to_thread(
            subprocess.run,
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

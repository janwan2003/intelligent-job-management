"""Docker CLI helpers for the IJM worker."""

import asyncio
import subprocess

from constants import CHECKPOINT_MOUNT_PATH, CONTAINER_NAME_PREFIX, DOCKER_CMD_TIMEOUT_SECONDS, RUNS_MOUNT_PATH


def build_run_cmd(
    container_name: str,
    ckpt_host_path: str,
    runs_host_path: str,
    image: str,
    command: list[str],
    env_vars: dict[str, str] | None = None,
    extra_volumes: dict[str, str] | None = None,
) -> list[str]:
    """Construct a ``docker run`` command with volume mounts and env vars."""
    cmd = [
        "docker",
        "run",
        "--rm",
        "--name",
        container_name,
        "-v",
        f"{ckpt_host_path}:{CHECKPOINT_MOUNT_PATH}",
        "-v",
        f"{runs_host_path}:{RUNS_MOUNT_PATH}",
    ]
    for host_path, container_path in (extra_volumes or {}).items():
        cmd += ["-v", f"{host_path}:{container_path}"]
    for key, val in (env_vars or {}).items():
        cmd += ["-e", f"{key}={val}"]
    cmd.append(image)
    return cmd + command


async def docker_exec(*args: str, timeout: int = DOCKER_CMD_TIMEOUT_SECONDS) -> subprocess.CompletedProcess[str]:
    """Run a docker CLI command in a thread."""
    return await asyncio.to_thread(
        subprocess.run,
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


async def list_containers() -> set[str]:
    """List all containers whose name starts with the IJM prefix."""
    result = await docker_exec("ps", "-a", "--filter", f"name={CONTAINER_NAME_PREFIX}", "--format", "{{.Names}}")
    return set(result.stdout.strip().split("\n")) if result.stdout.strip() else set()


async def kill_container(container_name: str) -> subprocess.CompletedProcess[str]:
    """Kill a running container by name."""
    return await docker_exec("kill", container_name)

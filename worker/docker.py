"""Docker CLI helpers for the IJM worker."""

import asyncio
import logging
import os
import subprocess
from pathlib import Path

from constants import CHECKPOINT_MOUNT_PATH, CONTAINER_NAME_PREFIX, DOCKER_CMD_TIMEOUT_SECONDS, RUNS_MOUNT_PATH

logger = logging.getLogger(__name__)


def _host_data_owner() -> tuple[int, int] | None:
    """Return (uid, gid) of the host data directory, or None if unavailable.

    Used to pin ``docker run --user`` so checkpoints are owned by the host
    user that owns ``data/`` on disk — not the container's default root.

    Background: with rootful Docker on one node and rootless on another, root-
    in-container resolves to different host UIDs.  A checkpoint written as
    ``root:root 0600`` on a rootful node is unreadable through an rclone-over-
    SFTP mount on the rootless node (SFTP runs as the unprivileged user) and
    surfaces as ``EPERM`` to the next training container.  Pinning ``--user``
    makes every node write the file as the same host user, so reads succeed
    from anywhere via the shared mount.
    """
    host_root = os.getenv("HOST_ROOT", "/host")
    try:
        st = (Path(host_root) / "data").stat()
        return st.st_uid, st.st_gid
    except OSError as exc:
        logger.warning("Could not stat host data dir to pin --user: %s", exc)
        return None


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
    ]
    owner = _host_data_owner()
    if owner is not None:
        uid, gid = owner
        cmd += ["--user", f"{uid}:{gid}"]
        # Pinned UID has no /etc/passwd entry inside the image, so
        # ``getpass.getuser()`` (called transitively by torch's dynamo cache
        # setup) raises ``OSError: No username set in the environment``.
        # Setting USER + HOME satisfies that lookup and gives torch a
        # writable cache dir owned by the pinned user.
        cmd += ["-e", "USER=ijm", "-e", "HOME=/tmp"]
    cmd += [
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


async def kill_container(container_name: str) -> None:
    """Kill a running container by name, then remove it.

    `docker run --rm` is supposed to remove the container on exit, but if the
    kill races with daemon registration (or if SIGTERM/SIGKILL ordering is off)
    the name reservation can linger.  An explicit `docker rm -f` ensures the
    name is freed so a subsequent dispatch can reuse it.

    Raises ``RuntimeError`` if the container is still present after the kill+rm
    sequence — callers must trust this signals real death (the auto-preempt
    path persists ``status=QUEUED, assigned_node=NULL`` immediately after, and
    a silent kill failure leaves a divergent row pointing at a live container).
    """
    kill = await docker_exec("kill", container_name)
    rm = await docker_exec("rm", "-f", container_name)
    check = await docker_exec("ps", "-a", "--filter", f"name=^{container_name}$", "--format", "{{.Names}}")
    if check.stdout.strip():
        raise RuntimeError(
            f"container {container_name} still present after kill+rm "
            f"(kill rc={kill.returncode}, rm rc={rm.returncode}, "
            f"kill_stderr={kill.stderr.strip()!r}, rm_stderr={rm.stderr.strip()!r})"
        )


async def remove_container_if_exists(container_name: str) -> None:
    """Defensive cleanup: remove any container with this name before launching.

    Handles leftovers from worker crashes or kill races where --rm didn't run.
    Silently ignores "no such container" errors — that's the happy path.
    """
    await docker_exec("rm", "-f", container_name)

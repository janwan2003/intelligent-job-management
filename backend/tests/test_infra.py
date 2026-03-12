"""Validate infra/docker-compose.yml configuration for known version-specific requirements."""

from pathlib import Path
from typing import Any

import yaml

COMPOSE_FILE = Path(__file__).parents[2] / "infra" / "docker-compose.yml"


def _load_compose() -> dict[str, Any]:
    with COMPOSE_FILE.open() as f:
        data: dict[str, Any] = yaml.safe_load(f)
        return data


def _postgres_major(image: str) -> int | None:
    """Extract major version from a postgres image tag like 'postgres:18' or 'postgres:16-alpine'."""
    if not image.startswith("postgres:"):
        return None
    tag = image.split(":", 1)[1].split("-")[0]
    try:
        return int(tag)
    except ValueError:
        return None


def test_postgres_volume_mount_matches_version() -> None:
    """postgres:18+ requires mounting /var/lib/postgresql (not .../data).

    This was a breaking change in the postgres Docker image (PR #1259).
    The pg_ctlcluster-compatible layout stores data in a major-version subdirectory,
    so the parent directory must be the mount point.
    """
    compose = _load_compose()
    pg = compose["services"]["postgres"]
    image: str = pg["image"]
    major = _postgres_major(image)
    assert major is not None, f"Could not parse postgres major version from image '{image}'"

    volumes: list[str] = pg.get("volumes", [])
    container_paths = [v.split(":")[1].rstrip("/") for v in volumes if ":" in v]

    if major >= 18:
        assert "/var/lib/postgresql" in container_paths, (
            f"postgres:{major} requires mounting /var/lib/postgresql "
            f"(not /var/lib/postgresql/data). "
            f"Got: {container_paths}"
        )
        assert "/var/lib/postgresql/data" not in container_paths, (
            f"postgres:{major} must NOT mount /var/lib/postgresql/data directly. "
            f"Mount the parent /var/lib/postgresql instead."
        )
    else:
        assert "/var/lib/postgresql/data" in container_paths, (
            f"postgres:{major} expects mount at /var/lib/postgresql/data. Got: {container_paths}"
        )


def test_all_python_services_use_consistent_base_image() -> None:
    """Dockerfile base images for Python services should all use the same python version."""
    repo_root = Path(__file__).parents[2]
    dockerfiles = {
        "api": repo_root / "backend" / "Dockerfile",
        "worker": repo_root / "worker" / "Dockerfile",
    }
    versions: dict[str, str] = {}
    for name, path in dockerfiles.items():
        text = path.read_text()
        for line in text.splitlines():
            if line.startswith("FROM python:"):
                tag = line.split("FROM python:")[1].split()[0]
                versions[name] = tag
                break

    assert len(set(versions.values())) == 1, (
        f"Python base image mismatch across services: {versions}. "
        "All Python services should use the same python:X.Y-slim tag."
    )

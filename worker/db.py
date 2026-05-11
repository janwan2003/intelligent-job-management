"""Database helpers for the IJM worker."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

DATABASE_URL: str | None = os.getenv("DATABASE_URL")

# Whitelist of columns the worker is allowed to write via ``update_job``.
# Keeping this explicit defends against a future caller passing arbitrary
# user-derived field names (which would otherwise concatenate straight into
# the SQL via the ``SET`` clause builder below).
_ALLOWED_JOB_FIELDS: frozenset[str] = frozenset(
    {
        "status",
        "container_name",
        "exit_code",
        "progress",
        "assigned_node",
        "assigned_gpu_config",
        "is_profiling_run",
        "updated_at",
    }
)


async def connect() -> psycopg.AsyncConnection[Any]:
    """Create a new async DB connection."""
    if DATABASE_URL is None:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    return await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row)


@asynccontextmanager
async def conn() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """Short-lived connection context manager."""
    c = await connect()
    try:
        yield c
    finally:
        await c.close()


async def update_job(c: psycopg.AsyncConnection[Any], job_id: str, **fields: Any) -> None:
    """Update job fields with an automatic updated_at timestamp.

    Does NOT commit.  The caller owns the transaction so that a status flip and
    a downstream ``pg_notify`` (or a second UPDATE) can land in a single atomic
    unit.  Previously this committed inline, which split status + slot-freed
    notifications across two transactions and could lose one if the second
    failed.
    """
    fields["updated_at"] = datetime.now(UTC)
    bad = set(fields) - _ALLOWED_JOB_FIELDS
    if bad:
        raise ValueError(f"update_job: disallowed fields {bad}")
    sets = ", ".join(f"{k} = %({k})s" for k in fields)
    fields["_id"] = job_id
    async with c.cursor() as cur:
        # Whitelisted column names → safe interpolation; values are still
        # bound parameters via psycopg's named-args.
        await cur.execute(f"UPDATE jobs SET {sets} WHERE id = %(_id)s", fields)


_FETCHABLE_JOB_FIELDS: frozenset[str] = frozenset(
    {
        "id",
        "job_id",
        "image",
        "command",
        "script_path",
        "directory_to_mount",
        "status",
        "created_at",
        "updated_at",
        "container_name",
        "exit_code",
        "progress",
        "priority",
        "deadline",
        "batch_size",
        "epochs_total",
        "profiling_epochs_no",
        "assigned_node",
        "assigned_gpu_config",
        "is_profiling_run",
    }
)


async def fetch_job(c: psycopg.AsyncConnection[Any], job_id: str, *columns: str) -> dict[str, Any] | None:
    """Fetch a single job by ID, optionally selecting specific columns."""
    if columns:
        bad = set(columns) - _FETCHABLE_JOB_FIELDS
        if bad:
            raise ValueError(f"fetch_job: disallowed columns {bad}")
        cols = ", ".join(columns)
    else:
        cols = "*"
    async with c.cursor() as cur:
        await cur.execute(f"SELECT {cols} FROM jobs WHERE id = %(id)s", {"id": job_id})
        return await cur.fetchone()

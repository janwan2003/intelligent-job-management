"""Database helpers for the IJM worker."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

DATABASE_URL: str | None = os.getenv("DATABASE_URL")


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
    """Update job fields with an automatic updated_at timestamp."""
    fields["updated_at"] = datetime.now(UTC)
    sets = ", ".join(f"{k} = %({k})s" for k in fields)
    fields["_id"] = job_id
    async with c.cursor() as cur:
        await cur.execute(f"UPDATE jobs SET {sets} WHERE id = %(_id)s", fields)  # noqa: S608
    await c.commit()


async def fetch_job(c: psycopg.AsyncConnection[Any], job_id: str, *columns: str) -> dict[str, Any] | None:
    """Fetch a single job by ID, optionally selecting specific columns."""
    cols = ", ".join(columns) if columns else "*"
    async with c.cursor() as cur:
        await cur.execute(f"SELECT {cols} FROM jobs WHERE id = %(id)s", {"id": job_id})  # noqa: S608
        return await cur.fetchone()

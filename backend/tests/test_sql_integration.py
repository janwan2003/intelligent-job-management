"""Integration tests that run real SQL against PostgreSQL.

These tests catch type-mismatch errors (e.g. jsonb = json) that mock-based
tests cannot detect.  They are skipped automatically when PostgreSQL is not
reachable, so they never block offline development.

Run against the Docker Compose stack:
    cd backend && uv run pytest tests/test_sql_integration.py -v
"""

import os
from typing import Any
from uuid import uuid4

import psycopg  # type: ignore[import-not-found]
import pytest
from psycopg.types.json import Json  # type: ignore[import-not-found]

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ijm")


async def _connect() -> psycopg.AsyncConnection[Any]:
    try:
        return await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True)
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable ({exc})")


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

_CREATE_PROFILING = """
CREATE TABLE IF NOT EXISTS profiling_results (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    gpu_config JSONB NOT NULL,
    node_id TEXT NOT NULL,
    duration_seconds FLOAT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
)
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_jsonb_equality_in_where_clause() -> None:
    """Querying a JSONB column with = %s::jsonb must not raise UndefinedFunction.

    Regression test for: psycopg.errors.UndefinedFunction: operator does not
    exist: jsonb = json  (triggered when Json() adapter is used without ::jsonb
    cast in the WHERE clause).
    """
    conn = await _connect()
    try:
        async with conn.cursor() as cur:
            await cur.execute(_CREATE_PROFILING)

        job_id = f"test-{uuid4()}"
        gpu_config: dict[str, int] = {"A40": 2}

        # Insert a result row
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO profiling_results (id, job_id, gpu_config, node_id, duration_seconds, created_at) "
                "VALUES (%s, %s, %s, %s, %s, NOW())",
                (str(uuid4()), job_id, Json(gpu_config), "node-test", 42.0),
            )

        # This query previously raised:
        #   psycopg.errors.UndefinedFunction: operator does not exist: jsonb = json
        # The fix is to cast: gpu_config = %s::jsonb
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT duration_seconds FROM profiling_results WHERE job_id = %s AND gpu_config = %s::jsonb",
                (job_id, Json(gpu_config)),
            )
            row = await cur.fetchone()

        assert row is not None, "Expected to find the inserted profiling result"
        assert float(row[0]) == 42.0

    finally:
        # Clean up test rows
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM profiling_results WHERE job_id LIKE 'test-%'")
        await conn.close()


@pytest.mark.asyncio
async def test_jsonb_equality_wrong_cast_fails() -> None:
    """Confirm that omitting ::jsonb cast raises a PostgreSQL type error.

    This documents the exact failure mode that the bug produced.
    """
    conn = await _connect()
    try:
        async with conn.cursor() as cur:
            await cur.execute(_CREATE_PROFILING)

        with pytest.raises(psycopg.errors.UndefinedFunction):
            async with conn.cursor() as cur:
                # Intentionally omit ::jsonb — this should fail
                await cur.execute(
                    "SELECT 1 FROM profiling_results WHERE gpu_config = %s",
                    (Json({"A40": 1}),),
                )

    finally:
        await conn.close()

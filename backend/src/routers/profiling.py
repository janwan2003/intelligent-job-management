"""Profiling results endpoint."""

from typing import Any

from fastapi import APIRouter

import src.state as state

router = APIRouter()


@router.get("/profiling-results/{job_id}")
async def get_profiling_results(job_id: str) -> list[dict[str, Any]]:
    """Get all profiling results for a job, ordered by duration (fastest first)."""
    async with state.get_conn() as conn:
        cur = await conn.execute(
            """SELECT id, gpu_config, node_id, duration_seconds, created_at
               FROM profiling_results
               WHERE job_id = %s
               ORDER BY duration_seconds ASC""",
            (job_id,),
        )
        cols = ("id", "gpu_config", "node_id", "duration_seconds", "created_at")
        return [dict(zip(cols, row, strict=True)) for row in await cur.fetchall()]

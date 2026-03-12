"""Profiling results endpoint."""

from typing import Any

from fastapi import APIRouter

from src.state import require_db

router = APIRouter()


@router.get("/profiling-results/{job_id}")
async def get_profiling_results(job_id: str) -> list[dict[str, Any]]:
    """Get all profiling results for a job, ordered by duration (fastest first)."""
    conn = require_db()

    async with conn.cursor() as cur:
        await cur.execute(
            """SELECT id, gpu_config, node_id, duration_seconds, created_at
               FROM profiling_results
               WHERE job_id = %s
               ORDER BY duration_seconds ASC""",
            (job_id,),
        )
        rows = await cur.fetchall()

    return [
        {
            "id": r[0],
            "gpu_config": r[1],
            "node_id": r[2],
            "duration_seconds": r[3],
            "created_at": r[4].isoformat() if r[4] else None,
        }
        for r in rows
    ]

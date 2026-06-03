"""SQL helpers for profile-claim cleanup, shared by API and worker.

These helpers operate on a psycopg cursor so the caller controls transaction
boundaries.  Logging is left to the caller — callers in the API
(``routers/jobs.py``) and worker log through their own module loggers.
"""

from __future__ import annotations

from typing import Any


async def delete_unmeasured_claims(cur: Any, instance_id: str) -> int:
    """Delete every unmeasured profile claim (``duration_seconds IS NULL``)
    owned by *instance_id*.  Returns the number of rows deleted.

    Measured rows are preserved because they cache results for the type.
    """
    await cur.execute(
        "DELETE FROM profiling_results "
        "WHERE instance_id = %s AND duration_seconds IS NULL",
        (instance_id,),
    )
    return int(getattr(cur, "rowcount", 0) or 0)

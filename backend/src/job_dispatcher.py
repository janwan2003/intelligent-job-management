"""JobDispatcher — routes enqueue/stop to local runner or remote worker HTTP server."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import psycopg

from src.cluster import ClusterManager
from src.job_runner import JobRunner

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 10.0  # seconds for dispatch calls to worker


class JobDispatcher:
    """Dispatches job execution to the local JobRunner or a remote worker server.

    Nodes with ``workerUrl`` set in nodes_config receive HTTP dispatch calls.
    Nodes without it fall through to the embedded JobRunner + DockerExecutor.
    """

    def __init__(
        self,
        local_runner: JobRunner,
        get_conn: Any,
        cluster: ClusterManager,
    ) -> None:
        self._local = local_runner
        self._get_conn = get_conn
        self._cluster = cluster

    def _get_worker_url(self, node_id: str | None) -> str | None:
        """Return the workerUrl for *node_id*, or None if local execution."""
        if node_id is None:
            return None
        for raw in self._cluster.nodes:
            if raw.get("id") == node_id:
                url = raw.get("workerUrl")
                return str(url) if url is not None else None
        return None

    @asynccontextmanager
    async def _db(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        async with self._get_conn() as conn:
            yield conn

    async def _fetch_assigned_node(self, job_id: str) -> str | None:
        async with self._db() as conn:
            cur = await conn.execute("SELECT assigned_node FROM jobs WHERE id = %s", (job_id,))
            row = await cur.fetchone()
        return row[0] if row else None

    async def enqueue(self, job_id: str) -> None:
        """Dispatch a job to run — remote worker or local runner."""
        node_id = await self._fetch_assigned_node(job_id)
        worker_url = self._get_worker_url(node_id)
        if worker_url:
            asyncio.create_task(self._remote_run(worker_url, job_id), name=f"remote-run-{job_id[:8]}")
        else:
            await self._local.enqueue(job_id)

    async def stop(self, job_id: str) -> None:
        """Stop a job — remote worker or local runner."""
        node_id = await self._fetch_assigned_node(job_id)
        worker_url = self._get_worker_url(node_id)
        if worker_url:
            asyncio.create_task(self._remote_stop(worker_url, job_id), name=f"remote-stop-{job_id[:8]}")
        else:
            await self._local.stop(job_id)

    async def _remote_run(self, worker_url: str, job_id: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
                resp = await client.post(f"{worker_url}/jobs/{job_id}/run")
                resp.raise_for_status()
            logger.info("Dispatched job %s to worker %s", job_id[:8], worker_url)
        except Exception as exc:
            logger.warning(
                "Failed to dispatch job %s to %s: %s — queue watcher will retry", job_id[:8], worker_url, exc
            )

    async def _remote_stop(self, worker_url: str, job_id: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
                resp = await client.post(f"{worker_url}/jobs/{job_id}/stop")
                resp.raise_for_status()
            logger.info("Sent stop for job %s to worker %s", job_id[:8], worker_url)
        except Exception as exc:
            logger.warning("Failed to stop job %s on %s: %s", job_id[:8], worker_url, exc)

    async def start(self) -> None:
        await self._local.start()

    async def shutdown(self) -> None:
        await self._local.shutdown()

"""JobDispatcher — routes enqueue/stop to local runner or remote worker HTTP server."""

import asyncio
import logging
from typing import Any

import httpx

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

    def get_worker_url(self, node_id: str | None) -> str | None:
        """Return the workerUrl for *node_id*, or None if local execution."""
        if node_id is None:
            return None
        for raw in self._cluster.nodes:
            if raw.get("id") == node_id:
                return raw.get("workerUrl")
        return None

    async def _fetch_assigned_node(self, job_id: str) -> str | None:
        async with self._get_conn() as conn:
            cur = await conn.execute("SELECT assigned_node FROM jobs WHERE id = %s", (job_id,))
            row = await cur.fetchone()
        return row[0] if row else None

    async def enqueue(self, job_id: str) -> None:
        """Dispatch a job to run — remote worker or local runner."""
        node_id = await self._fetch_assigned_node(job_id)
        worker_url = self.get_worker_url(node_id)
        if worker_url:
            asyncio.create_task(self._remote_request(worker_url, job_id, "run"), name=f"remote-run-{job_id[:8]}")
        else:
            await self._local.enqueue(job_id)

    async def stop(self, job_id: str, *, reason: str = "user") -> None:
        """Stop a job — remote worker or local runner.

        ``reason="user"``: user-initiated, status → PREEMPTED, stays stopped
        until the user resumes it.
        ``reason="auto"``: scheduler-initiated preemption (e.g. optimizer
        making room for an urgent job), status → QUEUED with assignment
        cleared, ready to be picked up by the next scheduler pass without an
        intermediate PREEMPTED state.
        """
        node_id = await self._fetch_assigned_node(job_id)
        worker_url = self.get_worker_url(node_id)
        if worker_url:
            params = {"reason": reason}
            asyncio.create_task(
                self._remote_request(worker_url, job_id, "stop", params=params),
                name=f"remote-stop-{job_id[:8]}",
            )
        else:
            await self._local.stop(job_id, reason=reason)

    async def _remote_request(
        self, worker_url: str, job_id: str, action: str, params: dict[str, str] | None = None
    ) -> None:
        # ``/run`` may legitimately return 409 if the job's previous Phase 4
        # hasn't yet popped ``running_jobs[job_id]`` on the worker (common
        # right after a migration: stop fired, persist committed, but the
        # _run_job task is still finishing its drain).  Retry briefly so the
        # urgent dispatch isn't permanently lost — the OPP-side state will
        # settle within a few hundred ms.
        max_retries = 3 if action == "run" else 1
        retry_delay = 0.3
        last_exc: Exception | None = None
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
                    resp = await client.post(f"{worker_url}/jobs/{job_id}/{action}", params=params)
                    if resp.status_code == 409 and action == "run":
                        last_exc = httpx.HTTPStatusError(
                            f"409 Conflict (attempt {attempt + 1}/{max_retries})",
                            request=resp.request,
                            response=resp,
                        )
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                    resp.raise_for_status()
                logger.info("Remote %s for job %s on %s", action, job_id[:8], worker_url)
                return
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries - 1 and action == "run":
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                break
        logger.warning(
            "Remote %s failed for job %s on %s after %d attempt(s): %s",
            action,
            job_id[:8],
            worker_url,
            max_retries,
            last_exc,
        )

    def _local_node_ids(self) -> frozenset[str]:
        """IDs of nodes that use the local runner (no workerUrl configured)."""
        return frozenset(raw["id"] for raw in self._cluster.nodes if not raw.get("workerUrl"))

    async def start(self) -> None:
        await self._local.start(local_node_ids=self._local_node_ids())

    async def shutdown(self) -> None:
        await self._local.shutdown()

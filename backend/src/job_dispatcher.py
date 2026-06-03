"""JobDispatcher — routes enqueue/stop to the assigned node's worker HTTP server."""

import asyncio
import logging
from typing import Any

import httpx

from src.cluster import ClusterManager

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 10.0  # seconds for /run and other quick worker calls
# /stop on the worker can legitimately take up to ~60 s — it awaits the
# in-flight run task drain (worker/app.py: asyncio.wait_for(run_task,
# timeout=60.0)) which includes SIGTERM grace, CUDA context release, and
# checkpoint write.  Set the client-side timeout above the worker's
# upper bound so the in-flight stop task stays open for the whole worker
# drain — this keeps _await_inflight_stops_on() correctly blocking new
# dispatches to the same node, instead of giving up at 10 s and letting
# a new /run race the still-draining old /stop.
_STOP_TIMEOUT = 65.0


class JobDispatcher:
    """Dispatches job execution to the assigned node's worker over HTTP.

    Every node in nodes_config must set ``workerUrl``; the dispatcher posts
    ``/run`` and ``/stop`` to that worker.  (The embedded in-process runner
    that used to back ``workerUrl``-less nodes has been removed — local dev
    runs a worker container too.)
    """

    def __init__(
        self,
        get_conn: Any,
        cluster: ClusterManager,
        node_slots: Any = None,
    ) -> None:
        self._get_conn = get_conn
        self._cluster = cluster
        self._node_slots = node_slots
        # Defense in depth against the optimizer issuing back-to-back preempts
        # for the same job: a second /stop arriving while the first is still
        # in flight on the network would race the scheduler's re-assignment
        # and clobber assigned_node on the worker side.  The worker has a
        # stale-stop guard, but deduping here keeps the network quieter and
        # protects against a regression in that guard.
        #
        # Keyed by (node_id, job_id) so ``_await_inflight_stops_on`` can find
        # the stops affecting a given node without a DB roundtrip per task.
        self._inflight_stops: dict[tuple[str, str], asyncio.Task[None]] = {}

    def set_node_slots(self, node_slots: Any) -> None:
        """Late-binding hook — NodeSlots is built after JobDispatcher in lifespan."""
        self._node_slots = node_slots

    async def dispatch_with_slot(self, instance_id: str, node_id: str, gpu_config: dict[str, int]) -> None:
        """Acquire a per-node permit, then send /run.  Releases on any failure.

        This is THE dispatch entry point for any caller that has just decided
        a job goes to a specific (node, gpu_config) — direct submission,
        resume, or the central scheduler's optimizer/greedy paths.  Skipping
        the slot acquire (e.g. by calling ``enqueue`` directly) bypasses the
        per-node capacity invariant and lets more containers run on a node
        than it has GPUs for.  Always go through this method.

        Cancel-safe: catches ``BaseException`` so an external cancel (e.g.
        the reaper cancelling a stuck dispatch) cannot leak a held permit.

        After acquire we re-read ``assigned_node`` from the DB.  In the time
        we spent waiting on the semaphore the row may have been reassigned
        (concurrent preempt cleared and re-Phase-1b'd it onto another node)
        — in that case we'd be sending /run to the wrong worker while
        holding ``node_id``'s permit forever.  Bail out instead.
        """
        n = sum(int(v) for v in gpu_config.values())
        acquired = False
        if self._node_slots is not None:
            await self._node_slots.acquire(node_id, n)
            acquired = True
        try:
            # Wait for any in-flight /stop calls targeting THIS node to finish
            # before issuing /run.  Otherwise the worker can race the two
            # requests: a /run that arrives while the previous container's
            # CUDA context hasn't yet released can fail to claim a GPU and
            # exit with a non-zero code (observed as FAILED ec=1 in the
            # PR-method scenario run).  Per-node await is finer-grained than
            # per-job since multiple stops on a single node have to complete
            # before we can trust the GPU state.
            await self._await_inflight_stops_on(node_id)
            current_node = await self._fetch_assigned_node(instance_id)
            if current_node != node_id:
                raise RuntimeError(
                    f"dispatch_with_slot({instance_id[:8]}): row reassigned "
                    f"to {current_node!r} while waiting for {node_id!r}'s slot"
                )
            await self.enqueue(instance_id)
        except BaseException:
            if acquired and self._node_slots is not None:
                self._node_slots.release(node_id, n)
            raise

    async def _await_inflight_stops_on(self, node_id: str) -> None:
        """Block until every in-flight /stop targeting *node_id* completes.

        Looks up stops by their ``(node_id, _)`` key directly — no DB roundtrip.
        Defensive 10 s bound: if the worker has truly hung, the drift heartbeat
        repairs the slot count regardless, so we don't need to wait the full
        ``_STOP_TIMEOUT`` here.
        """
        if not self._inflight_stops:
            return
        relevant = [task for (n, _jid), task in self._inflight_stops.items() if n == node_id and not task.done()]
        if not relevant:
            return
        logger.info(
            "Waiting for %d in-flight stop(s) on %s before dispatch",
            len(relevant),
            node_id,
        )
        try:
            await asyncio.wait_for(
                asyncio.shield(asyncio.gather(*relevant, return_exceptions=True)),
                timeout=10.0,
            )
        except TimeoutError:
            logger.warning(
                "Timed out waiting for /stop drain on %s (after 10s) — proceeding; drift heartbeat will reconcile",
                node_id,
            )

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
        """Dispatch a job to its assigned node's worker via HTTP ``/run``.

        We await the HTTP /run call so dispatch failures (worker unreachable,
        image pull error) propagate back to the caller and the slot is released
        by ``_dispatch_when_slot_free``.  Previously the call was fire-and-forget,
        which silently masked unreachable workers and left slots claimed for the
        row's lifetime.

        If the row's ``assigned_node`` was cleared between Phase 1b and now
        (concurrent preempt, delete, or scheduler churn), there is no valid
        worker to target.  Raising surfaces the race so the dispatcher releases
        the slot.
        """
        node_id = await self._fetch_assigned_node(job_id)
        worker_url = self.get_worker_url(node_id)
        if worker_url:
            await self._remote_request(worker_url, job_id, "run")
        elif node_id is None:
            raise RuntimeError(
                f"enqueue({job_id[:8]}): no assigned_node — likely cleared by a concurrent preempt/delete"
            )
        else:
            # A node with an id but no workerUrl is a configuration error now
            # that the embedded local runner is gone — every node must point at
            # a worker.  Raise so dispatch_with_slot releases the slot instead
            # of wedging the row in QUEUED forever.
            raise RuntimeError(
                f"enqueue({job_id[:8]}): node {node_id!r} has no workerUrl — "
                "every node must be backed by a worker (local execution removed)"
            )

    async def stop(self, job_id: str, *, reason: str = "user") -> None:
        """Stop a job — remote worker or local runner.

        ``reason="user"``: user-initiated, status → PREEMPTED, stays stopped
        until the user resumes it.
        ``reason="auto"``: scheduler-initiated preemption (e.g. optimizer
        making room for an urgent job), status → QUEUED with assignment
        cleared, ready to be picked up by the next scheduler pass without an
        intermediate PREEMPTED state.
        """
        # Resolve the node up-front so _inflight_stops can be keyed by
        # (node_id, job_id) — _await_inflight_stops_on can then locate the
        # relevant stops without a per-task DB roundtrip.  If the row has no
        # assigned_node (already cleared / never assigned), _do_stop will
        # no-op via the worker's stale-stop guard; tag it under a sentinel
        # so duplicate-stop dedup still works.
        node_id = await self._fetch_assigned_node(job_id) or ""
        key = (node_id, job_id)
        existing = self._inflight_stops.get(key)
        if existing is not None and not existing.done():
            logger.info(
                "Skipping duplicate /stop for %s (reason=%s) — already in flight",
                job_id[:8],
                reason,
            )
            return

        task = asyncio.create_task(self._do_stop(job_id, reason=reason), name=f"stop-{job_id[:8]}")
        self._inflight_stops[key] = task

        def _cleanup(_t: asyncio.Task[None], k: tuple[str, str] = key) -> None:
            self._inflight_stops.pop(k, None)

        task.add_done_callback(_cleanup)

    async def _do_stop(self, job_id: str, *, reason: str) -> None:
        node_id = await self._fetch_assigned_node(job_id)
        worker_url = self.get_worker_url(node_id)
        if worker_url:
            params = {"reason": reason}
            try:
                await self._remote_request(worker_url, job_id, "stop", params=params)
            except Exception:
                # Stops are best-effort; worker reconcile catches orphans on startup.
                logger.warning("Remote stop for job %s failed (suppressed)", job_id[:8])
        elif node_id is None:
            # No assigned_node — nothing to stop.
            logger.info(
                "Skipping /stop for %s — assigned_node already cleared",
                job_id[:8],
            )
        else:
            # Node has an id but no workerUrl — misconfig (no local runner to
            # fall back to).  Nothing to stop here; log so it's visible.
            logger.warning(
                "Skipping /stop for %s — node %s has no workerUrl (local execution removed)",
                job_id[:8],
                node_id,
            )

    async def _remote_request(
        self, worker_url: str, job_id: str, action: str, params: dict[str, str] | None = None
    ) -> None:
        # /run can legitimately return 409 right after a migration while the
        # worker's previous _run_job task is still draining.  Retry briefly so
        # the urgent dispatch isn't lost.
        max_retries = 3 if action == "run" else 1
        retry_delay = 0.3
        last_exc: Exception | None = None
        timeout = _STOP_TIMEOUT if action == "stop" else _HTTP_TIMEOUT
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
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
        # Callers (enqueue) need the failure so they can release the slot.
        if last_exc is not None:
            raise last_exc

"""Job execution lifecycle for the IJM worker."""

import asyncio
import contextlib
import logging
import os
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from shared.constants import (
    DEFAULT_PROFILING_EPOCHS,
    OUTPUT_LOG_FILENAME,
    PG_NOTIFY_SCHEDULE,
    PG_NOTIFY_SLOT_FREED,
    RUNS_DIR,
    JobStatus,
    SlotFreedReason,
)
from shared.profiling_sql import delete_unmeasured_claims

from constants import CHECKPOINT_DIR, JOB_ID_DISPLAY_LENGTH, NODE_ID, RUNNABLE_STATUSES, container_name_for
from db import conn, fetch_job, update_job
from docker import build_run_cmd, remove_container_if_exists, total_gpus
from profiling import handle_complete

logger = logging.getLogger(__name__)

PROGRESS_RE = re.compile(r"Epoch\s+(\d+)/(\d+)")
# Matches ONLY the epoch-*completion* line and captures (epoch_num, compute_s)
# from the runtime's own log: ``Epoch 13/50 - Loss: .. - Acc: ..% - 77.42s``.
# The trailing ``77.42s`` is the per-epoch time the trainer measured (train +
# eval, see runtime/base.py); profiling records THAT, one sample per epoch.
# We deliberately do NOT time when these lines arrive at the worker: arrival
# intervals fold in checkpoint-save I/O and log/network jitter, which are far
# heavier during the concurrent profile sweep than during the steady standard
# run and bias the estimate high (observed 2026-06-25: A40x1 measured 8.0s by
# arrival vs ~5.2s real compute).  The ``starting`` line has no trailing
# duration, so it never matches.
EPOCH_DONE_RE = re.compile(r"Epoch\s+(\d+)/\d+\s+-\s+Loss:.*-\s+([0-9.]+)s\s*$")
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", str(24 * 3600)))

HOST_ROOT: str = os.getenv("HOST_ROOT", "/host")
HOST_PROJECT_ROOT: str = os.path.normpath(os.getenv("HOST_PROJECT_ROOT", HOST_ROOT))

# In-process tracking
# A value of ``None`` is a placeholder meaning "Phase 1 has claimed this job
# but Phase 2 hasn't launched the container yet".  /stop must wait for the
# Popen to materialize (or the placeholder to clear) before deciding what to
# do; otherwise it would persist QUEUED while the container is about to be
# launched, leaving the row lying about a live container.
# Tracks which physical GPU indices each in-flight job has claimed on this
# node.  Without this, ``--gpus N`` and ``--device nvidia.com/gpu=0`` both
# default to GPU index 0 for every concurrent dispatch — two 1-GPU jobs on
# the same node land on the same physical GPU while the other sits idle.
# We pick concrete indices under a lock so each job ends up on a distinct
# GPU; release on container exit.
_claimed_gpus: dict[str, set[int]] = {}
_gpu_claim_lock = asyncio.Lock()

running_jobs: dict[str, subprocess.Popen[str] | None] = {}
# Set when Phase 2 has either populated the Popen or bailed out (claim lost,
# launch failure).  /stop awaits this Event instead of polling running_jobs,
# avoiding the race where a 0.1s sleep loop expires while Phase 2 is still
# inside a slow mkdir/remove_container_if_exists call.
dispatch_ready: dict[str, asyncio.Event] = {}
# The asyncio.Task for each in-flight _run_job — /stop awaits it to know when
# the run has fully drained (replacing a 60s polling loop on running_jobs).
running_tasks: dict[str, asyncio.Task[None]] = {}
# Set by ``/stop`` at the very top so Phase 4 can distinguish "container
# exited 137 because /stop killed it" from "container exited 137 because it
# OOM'd / was externally killed".  Without this flag Phase 4 always wrote
# ``status=FAILED`` for non-zero exits, and the frontend's 3-s poll caught
# the brief FAILED window before ``_persist_stop`` flipped it to
# QUEUED/PREEMPTED.  When the flag is present, Phase 4 records exit_code
# only and lets ``_persist_stop`` (or the stale-stop branch) own status +
# slot release.
stopping: dict[str, str] = {}
_job_tasks: set[asyncio.Task[None]] = set()


def _resolve_paths(job_id: str) -> tuple[Path, Path, Path, Path]:
    """Return (ckpt_local, runs_local, ckpt_host, runs_host)."""
    ckpt_local = Path(HOST_ROOT) / "data" / CHECKPOINT_DIR / job_id
    runs_local = Path(HOST_ROOT) / "data" / RUNS_DIR / job_id
    ckpt_host = Path(HOST_PROJECT_ROOT) / "data" / CHECKPOINT_DIR / job_id
    runs_host = Path(HOST_PROJECT_ROOT) / "data" / RUNS_DIR / job_id
    return ckpt_local, runs_local, ckpt_host, runs_host


def _prepare_checkpoint_dir(ckpt_local: Path, *, is_profiling: bool) -> None:
    """Create the checkpoint directory.  Profiling uses an isolated
    ``.profiling/`` subdir which is always wiped — profiling is a fresh
    short run and shouldn't inherit state from a previous attempt.

    For real runs we never delete files here.  The only file we care about
    is ``latest.pt``, which the trainer overwrites atomically on every
    save.  Wiping on a perceived "first run" is what caused checkpoint
    loss when two dispatches raced (the second saw progress=None — reset
    by Phase 1 — and nuked the live checkpoint mid-resume).  If the file
    isn't there, the trainer just starts from scratch.
    """
    if is_profiling:
        mount = ckpt_local / ".profiling"
        mount.mkdir(parents=True, exist_ok=True)
        # chmod 0777 so the pinned (non-root) container UID can write here,
        # mirroring the widening in _run_job for ckpt_local/runs_local.  Both
        # rootful and rootless workers pass through this code path; root
        # creates dirs at 0755 by default which blocks the container user.
        with contextlib.suppress(OSError):
            os.chmod(mount, 0o777)
        for f in mount.iterdir():
            if f.is_file():
                f.unlink(missing_ok=True)
    else:
        ckpt_local.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            os.chmod(ckpt_local, 0o777)


async def _claim_gpu_indices(job_id: str, n: int) -> list[int]:
    """Reserve ``n`` distinct physical GPU indices for this job.  Held until
    ``_release_gpu_indices(job_id)``.  If ``n`` exceeds free capacity (slot
    accounting bug), fall back to the lowest ``n`` indices and warn — never
    block the launch, but the operator-visible warning makes the bug obvious.
    """
    if n <= 0:
        return []
    async with _gpu_claim_lock:
        total = total_gpus()
        all_claimed: set[int] = set().union(*_claimed_gpus.values()) if _claimed_gpus else set()
        free = [i for i in range(total) if i not in all_claimed]
        if len(free) < n:
            logger.warning(
                "GPU allocator: requested %d but only %d free (claimed=%s, total=%d) — "
                "falling back to lowest indices (slot-accounting bug upstream?)",
                n,
                len(free),
                sorted(all_claimed),
                total,
            )
            picked = list(range(n))
        else:
            picked = free[:n]
        _claimed_gpus[job_id] = set(picked)
        logger.info("GPU allocator: job %s claims indices %s", job_id[:JOB_ID_DISPLAY_LENGTH], picked)
        return picked


def _release_gpu_indices(job_id: str) -> None:
    """Release the GPU indices held by ``job_id`` (no-op if never claimed)."""
    indices = _claimed_gpus.pop(job_id, None)
    if indices is not None:
        logger.info("GPU allocator: job %s releases indices %s", job_id[:JOB_ID_DISPLAY_LENGTH], sorted(indices))


def _build_env_vars(job: dict[str, Any]) -> dict[str, str]:
    """Build environment variables for the training container."""
    env: dict[str, str] = {
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
    }
    if job["is_profiling_run"]:
        env["EPOCHS_TOTAL"] = str(job.get("profiling_epochs_no") or DEFAULT_PROFILING_EPOCHS)
    elif job.get("epochs_total"):
        env["EPOCHS_TOTAL"] = str(job["epochs_total"])
    if job.get("batch_size"):
        env["BATCH_SIZE"] = str(job["batch_size"])
    return env


async def _stream_output(
    process: subprocess.Popen[str],
    job_id: str,
    container_name: str,
    log_path: Path,
    epoch_durations: list[tuple[int, float]],
    *,
    is_profiling: bool,
) -> None:
    """Stream container stdout, write to log file, parse progress updates.

    Progress writes are guarded: we only update if the row still has
    ``status IN (RUNNING, PROFILING)`` AND ``container_name`` matches what
    we own.  If the row was reassigned (auto-preempt + reschedule on a new
    node), the UPDATE is a no-op and we stop streaming — the new owner is
    authoritative now.
    """
    loop = asyncio.get_running_loop()
    stdout = process.stdout
    if stdout is None:
        return

    # Coalescing progress writer: the reader pushes only the latest progress
    # into ``pending``; a background task flushes it via a short-lived
    # connection at most once per PROGRESS_FLUSH_INTERVAL_S.  Opening a fresh
    # connection per epoch line over the SSH-tunnelled DB took ~5s/line on
    # polimi-gpu, making the reader fall progressively behind the trainer and
    # /stop drains hit the 60s timeout.
    PROGRESS_FLUSH_INTERVAL_S = 1.0
    pending: dict[str, str | None] = {"progress": None}
    wakeup = asyncio.Event()
    stop_writer = asyncio.Event()
    row_lost = False

    async def _writer() -> None:
        nonlocal row_lost
        last_written: str | None = None
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wakeup.wait(), timeout=PROGRESS_FLUSH_INTERVAL_S)
            wakeup.clear()
            progress = pending["progress"]
            if progress is not None and progress != last_written:
                try:
                    async with conn() as c, c.cursor() as cur:
                        await cur.execute(
                            "UPDATE jobs SET progress = %s, updated_at = %s "
                            "WHERE id = %s AND status = ANY(%s) AND container_name = %s",
                            (
                                progress,
                                datetime.now(UTC),
                                job_id,
                                [JobStatus.RUNNING, JobStatus.PROFILING],
                                container_name,
                            ),
                        )
                        rowcount = cur.rowcount
                        await c.commit()
                    last_written = progress
                    if rowcount == 0:
                        row_lost = True
                        return
                except Exception:
                    logger.warning("Progress write for job %s failed", job_id[:JOB_ID_DISPLAY_LENGTH], exc_info=True)
            if stop_writer.is_set() and pending["progress"] == last_written:
                return

    writer_task = asyncio.create_task(_writer())
    try:
        with open(log_path, "a") as log_file:
            while True:
                if row_lost:
                    logger.warning(
                        "Job %s row no longer owns container %s — stopping stream",
                        job_id[:JOB_ID_DISPLAY_LENGTH],
                        container_name,
                    )
                    break
                line = await loop.run_in_executor(None, stdout.readline)
                if not line:
                    break
                stripped = line.rstrip()
                logger.info("[Job %s] %s", job_id[:JOB_ID_DISPLAY_LENGTH], stripped)
                log_file.write(line)
                log_file.flush()
                match = PROGRESS_RE.search(stripped)
                if match:
                    epoch_num = int(match.group(1))
                    pending["progress"] = f"{epoch_num}/{match.group(2)}"
                    wakeup.set()
                    # One profiling sample per epoch end: the runtime's own
                    # measured compute time, not a worker-side arrival clock.
                    if is_profiling:
                        done = EPOCH_DONE_RE.search(stripped)
                        if done:
                            epoch_durations.append((int(done.group(1)), float(done.group(2))))
    except Exception:
        # An error here means we lose the rest of the container output and any
        # further epoch samples — surface it instead of swallowing.
        logger.warning("Output streaming for job %s ended unexpectedly", job_id[:JOB_ID_DISPLAY_LENGTH], exc_info=True)
    finally:
        # Final flush: signal writer to drain the latest progress, then await.
        stop_writer.set()
        wakeup.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(writer_task, timeout=10.0)
        if not writer_task.done():
            writer_task.cancel()
            with contextlib.suppress(Exception):
                await writer_task


async def _run_job(job_id: str) -> None:
    """Execute a job in a Docker container."""
    ready = dispatch_ready.setdefault(job_id, asyncio.Event())
    try:
        # Phase 1: atomically claim the job (short-lived connection).
        # Two ops in sequence (fetch then update) used to allow a /stop
        # arriving between them to clobber RUNNING with QUEUED — leaving the
        # row at QUEUED while Phase 2 still launched the container.  The
        # claim+placeholder pair below makes the dispatch indivisible from
        # the perspective of /stop: once we hold ``running_jobs[job_id]``,
        # /stop blocks on us; if the claim loses, we never touch the dict.
        async with conn() as c:
            job = await fetch_job(
                c,
                job_id,
                "image",
                "command",
                "status",
                "is_profiling_run",
                "profiling_epochs_no",
                "exit_code",
                "epochs_total",
                "batch_size",
                "assigned_node",
                "assigned_gpu_config",
                "progress",
            )
            if not job:
                logger.error("Job %s not found", job_id)
                return
            if job["status"] not in RUNNABLE_STATUSES:
                logger.info("Job %s status %s not runnable, skipping", job_id[:JOB_ID_DISPLAY_LENGTH], job["status"])
                return

            run_status = JobStatus.PROFILING if job["is_profiling_run"] else JobStatus.RUNNING
            run_start_time = datetime.now(UTC)
            name = container_name_for(job_id)
            # Atomic claim — only succeeds if the row is still in a runnable
            # status AND still pointed at this worker.  A concurrent /stop
            # whose _persist_stop already committed (status=QUEUED,
            # container_name=NULL) makes this miss.  The ``assigned_node``
            # check is required because the API-side reaper can clear
            # ``assigned_node`` for a stuck QUEUED row while this claim is
            # still in flight — without the check we'd commit status=RUNNING
            # while the row's slot has already been released by the reaper,
            # producing a phantom RUNNING row with no slot (the 5/4 GPUs bug).
            # Don't reset progress here: back-to-back dispatches would both
            # see progress=None and both treat themselves as a first run,
            # wiping the live checkpoint.
            async with c.cursor() as cur:
                await cur.execute(
                    "UPDATE jobs SET status = %s, container_name = %s, updated_at = %s "
                    "WHERE id = %s AND status = ANY(%s) AND assigned_node = %s "
                    "RETURNING id",
                    (run_status, name, datetime.now(UTC), job_id, list(RUNNABLE_STATUSES), NODE_ID),
                )
                claimed = await cur.fetchone()
            await c.commit()
            if not claimed:
                logger.info(
                    "Job %s no longer in a runnable status — dispatch losing the claim",
                    job_id[:JOB_ID_DISPLAY_LENGTH],
                )
                return

        # Reserve a placeholder so /stop can synchronize with us BEFORE the
        # container exists.  Must happen synchronously after the claim and
        # before any awaitable work that could let /stop run in our gap.
        # The ``finally`` below pops it on every exit path.
        running_jobs[job_id] = None

        # Phase 2: prepare dirs, launch container (no DB connection held)
        ckpt_local, runs_local, ckpt_host, runs_host = _resolve_paths(job_id)
        ckpt_local.mkdir(parents=True, exist_ok=True)
        runs_local.mkdir(parents=True, exist_ok=True)
        # Worker may run as root (rootful) or wangrat (rootless).  The
        # container is pinned to the host data-dir owner via --user, so
        # widen perms here in case mkdir created the dir with a stricter
        # umask under root.  Cheap and idempotent.
        for d in (ckpt_local, runs_local):
            with contextlib.suppress(OSError):
                os.chmod(d, 0o777)

        _prepare_checkpoint_dir(ckpt_local, is_profiling=job["is_profiling_run"])
        ckpt_host_mount = (ckpt_host / ".profiling") if job["is_profiling_run"] else ckpt_host

        # Shared dataset cache (e.g. MNIST) — mounted at /runs/data inside the
        # container so training scripts can find pre-downloaded data.
        shared_data_local = Path(HOST_ROOT) / "data" / "shared" / "data"
        shared_data_host = Path(HOST_PROJECT_ROOT) / "data" / "shared" / "data"
        extra_volumes: dict[str, str] = {}
        if shared_data_local.exists():
            extra_volumes[str(shared_data_host)] = "/runs/data"

        n_gpus = sum(int(v) for v in (job.get("assigned_gpu_config") or {}).values())
        gpu_indices = await _claim_gpu_indices(job_id, n_gpus)
        # Per-node image tag override.  matemagician's NVIDIA driver
        # (418.x, CUDA 10.1) is too old for the default CUDA-12.4 PyTorch
        # build, so the worker there sets ``IMAGE_TAG_OVERRIDE=legacy`` to
        # rewrite ``:latest`` → ``:legacy`` (torch 1.5.1 + CUDA 10.1).
        image = job["image"]
        tag_override = os.getenv("IMAGE_TAG_OVERRIDE")
        if tag_override and ":" in image:
            image = f"{image.rsplit(':', 1)[0]}:{tag_override}"
        docker_cmd = build_run_cmd(
            name,
            str(ckpt_host_mount),
            str(runs_host),
            image,
            job["command"],
            env_vars=_build_env_vars(job),
            extra_volumes=extra_volumes,
            gpu_indices=gpu_indices,
        )
        logger.info("Starting job %s: %s", job_id[:JOB_ID_DISPLAY_LENGTH], " ".join(docker_cmd))

        # Clear any leftover container with this name (worker crash, kill race,
        # or daemon-side --rm delay).  Otherwise `docker run --name` will fail
        # with exit 125 "name already in use".
        await remove_container_if_exists(name)

        # Per-run header in the output log so a viewer can see which node and
        # GPU config served each run, even before the container produces any
        # output (and even while the row is back in QUEUED).
        epoch_durations: list[tuple[int, float]] = []
        log_path = runs_local / OUTPUT_LOG_FILENAME
        run_kind = "PROFILING" if job["is_profiling_run"] else "RUNNING"
        header = (
            f"=== {run_kind} on node={NODE_ID} gpu_config={job.get('assigned_gpu_config')} "
            f"image={job['image']} command={job['command']} "
            f"start={run_start_time.isoformat()} ===\n"
        )
        with open(log_path, "a") as _f:
            _f.write(header)

        process = subprocess.Popen(docker_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        running_jobs[job_id] = process
        ready.set()  # /stop can now safely target the live container

        # Phase 3: stream output + wait (connections acquired per progress update)
        stream_task = asyncio.create_task(
            _stream_output(process, job_id, name, log_path, epoch_durations, is_profiling=job["is_profiling_run"])
        )

        loop = asyncio.get_running_loop()
        try:
            exit_code: int = await asyncio.wait_for(
                loop.run_in_executor(None, process.wait),
                timeout=JOB_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.error("Job %s timed out after %ds", job_id[:JOB_ID_DISPLAY_LENGTH], JOB_TIMEOUT_SECONDS)
            process.terminate()
            exit_code = await loop.run_in_executor(None, process.wait)

        await stream_task
        running_jobs.pop(job_id, None)

        # Phase 4: handle completion (short-lived connection).
        # Capture (assigned_node, n_gpus) before the terminal UPDATE so we
        # can NOTIFY ijm_slot_freed with the right payload.  After the
        # UPDATE, the row may still have assigned_node (for SUCCEEDED /
        # FAILED) but logically the slot is free.
        async with conn() as c:
            current = await fetch_job(c, job_id, "status", "assigned_node", "assigned_gpu_config")
            slot_node = current.get("assigned_node") if current else None
            slot_cfg = current.get("assigned_gpu_config") if current else None
            slot_n = sum(int(v) for v in (slot_cfg or {}).values())

            async def _notify_slot_freed() -> None:
                if slot_node and slot_n > 0:
                    # TERMINAL: the container ended of its own accord
                    # (SUCCEEDED / FAILED) — external from the API's
                    # perspective, so the listener will wake the optimiser.
                    await c.execute(
                        "SELECT pg_notify(%s, %s)",
                        (PG_NOTIFY_SLOT_FREED, f"{slot_node}:{slot_n}:{SlotFreedReason.TERMINAL}"),
                    )
                # Always commit so the preceding update_job() persists even
                # when there is no slot to free (already-cleared assignment).
                await c.commit()

            # If the job has been migrated to a different node, our run is the
            # *old* one — don't touch the row.  The new node's worker owns it
            # now, including any final state transition.  Skipping this check
            # would let our exit (often exit=137 from the migration kill)
            # clobber the new run's RUNNING with FAILED.
            if current and current["assigned_node"] not in (None, NODE_ID):
                logger.info(
                    "Job %s migrated to %s, skipping completion update from %s",
                    job_id[:JOB_ID_DISPLAY_LENGTH],
                    current["assigned_node"],
                    NODE_ID,
                )
                return
            # If the status is no longer RUNNING/PROFILING, the API has already
            # decided this run is done (e.g. it requeued PREEMPTED → QUEUED for
            # rescheduling).  Don't override — just record the exit code.
            # The /stop handler that flipped the status already fired
            # ijm_slot_freed, so we don't double-notify here.
            if current and current["status"] not in (JobStatus.RUNNING, JobStatus.PROFILING):
                logger.info(
                    "Job %s already in terminal/external status %s, recording exit_code only",
                    job_id[:JOB_ID_DISPLAY_LENGTH],
                    current["status"],
                )
                await update_job(c, job_id, exit_code=exit_code)
                if current["status"] == JobStatus.PREEMPTED:
                    await c.execute(f"NOTIFY {PG_NOTIFY_SCHEDULE}")
                # Single commit covers both the exit_code update and the
                # optional NOTIFY.
                await c.commit()
                return

            if exit_code != 0:
                if job_id in stopping:
                    # /stop is in flight; it will own the final status and the
                    # ``ijm_slot_freed`` NOTIFY.  Just record the exit code so
                    # the UI doesn't flash FAILED during the preempt cycle.
                    logger.info(
                        "Job %s exited (exit=%d) during /stop (reason=%s) — leaving status to /stop",
                        job_id[:JOB_ID_DISPLAY_LENGTH],
                        exit_code,
                        stopping[job_id],
                    )
                    await update_job(c, job_id, exit_code=exit_code)
                    return
                logger.error("Job %s failed (exit=%d)", job_id[:JOB_ID_DISPLAY_LENGTH], exit_code)
                await update_job(c, job_id, status=JobStatus.FAILED, exit_code=exit_code)
                # Drop unmeasured profile claims for this dead instance — the
                # API no longer runs a periodic sweep, so without this the
                # cells stay reserved until the 60-min lease watcher fires.
                async with c.cursor() as _gc_cur:
                    await delete_unmeasured_claims(_gc_cur, job_id)
                await _notify_slot_freed()
                return

            is_profiling = await handle_complete(c, job_id, run_start_time, epoch_durations)
            if not is_profiling:
                logger.info("Job %s completed successfully", job_id[:JOB_ID_DISPLAY_LENGTH])
                await update_job(c, job_id, status=JobStatus.SUCCEEDED, exit_code=exit_code)
                await _notify_slot_freed()
            else:
                # Profiling completion — handle_complete clears assigned_node
                # and resets to QUEUED, freeing the slot for re-scheduling.
                await _notify_slot_freed()

    except Exception:
        logger.exception("Failed to run job %s", job_id)
        try:
            async with conn() as c:
                # Read assignment BEFORE updating so we know which slot to
                # release even if assigned_node gets cleared elsewhere.
                cur = await c.execute(
                    "SELECT assigned_node, assigned_gpu_config FROM jobs WHERE id = %s",
                    (job_id,),
                )
                row = await cur.fetchone()
                slot_node = row.get("assigned_node") if row else None
                slot_cfg = row.get("assigned_gpu_config") if row else None
                slot_n = sum(int(v) for v in (slot_cfg or {}).values())

                await update_job(c, job_id, status=JobStatus.FAILED)
                # Drop unmeasured profile claims for this dead instance — the
                # API no longer runs a periodic sweep.
                async with c.cursor() as _gc_cur:
                    await delete_unmeasured_claims(_gc_cur, job_id)
                # Without this NOTIFY a pre-Phase-4 exception (e.g. mkdir
                # failure, docker daemon hiccup, runtime image missing) would
                # leak the API permit forever — Phase 4 normally emits this
                # but we never got there.
                if slot_node and slot_n > 0:
                    # Pre-Phase-4 failure: container never started, status
                    # forced to FAILED.  External from the API's view ->
                    # TERMINAL reason so the listener wakes the optimiser.
                    await c.execute(
                        "SELECT pg_notify(%s, %s)",
                        (PG_NOTIFY_SLOT_FREED, f"{slot_node}:{slot_n}:{SlotFreedReason.TERMINAL}"),
                    )
                await c.commit()
        except Exception:
            logger.exception("Failed to update job %s status to FAILED", job_id)
    finally:
        running_jobs.pop(job_id, None)
        _release_gpu_indices(job_id)
        # Always set so any /stop awaiting the dispatch placeholder unblocks
        # — even on early-bail paths (claim lost, launch error) where Phase 2
        # never reached the Popen line.
        ready.set()
        running_tasks.pop(job_id, None)
        dispatch_ready.pop(job_id, None)


def dispatch_job(job_id: str) -> asyncio.Task[None]:
    """Create an asyncio task for job execution."""
    task: asyncio.Task[None] = asyncio.create_task(_run_job(job_id), name=f"job-{job_id[:JOB_ID_DISPLAY_LENGTH]}")
    running_tasks[job_id] = task
    _job_tasks.add(task)
    task.add_done_callback(_job_tasks.discard)
    return task

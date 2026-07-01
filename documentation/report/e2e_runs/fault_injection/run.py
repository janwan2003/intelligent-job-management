"""Reproducible live fault-injection run for the slot/drift invariants.

Boots the real IJM API against a disposable Postgres with a short drift
heartbeat and NO workers, then injects two faults via direct DB writes and
records the /admin/slots timeseries while the autonomous drift heartbeat
reconciles each:

  Phase A  baseline               — node idle, used_mem == used_db == 0.
  Phase B  missed-acquire         — INSERT RUNNING rows the API never acquired
                                     (an API restart with an in-flight container,
                                     or the acquire-direction of a lost event).
                                     used_db runs ahead of used_mem (drift=true);
                                     the heartbeat ACQUIRES to match the DB.
  Phase C  dropped slot-freed NOTIFY — flip those rows to SUCCEEDED WITHOUT a
                                     NOTIFY (the tunnel-reset case).  used_mem now
                                     runs ahead of used_db (drift=true); the
                                     heartbeat RELEASES the leaked permits.
  Phase D  settled                — back in sync, no drift, no oversubscription.

This is the live counterpart to backend/tests/test_fault_injection.py (which
asserts the same invariants against a real DB at the unit level).  It is fully
isolated from any operator deployment: its own DB, its own API port, and a
dummy workerUrl so no container is ever launched.

Reproduce:
    docker run -d --name ijm-fault-pg -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=ijm -p 55432:5432 postgres:18
    DATABASE_URL=postgresql://postgres:postgres@localhost:55432/ijm \
        python documentation/report/e2e_runs/fault_injection/run.py
    docker rm -f ijm-fault-pg

Env knobs: DATABASE_URL, IJM_API_PORT (58080), IJM_DRIFT_HEARTBEAT_S (2),
REPO_ROOT (autodetected), OUT_DIR (this directory).
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg
from psycopg.types.json import Json

HERE = Path(__file__).resolve().parent
# fault_injection -> e2e_runs -> report -> documentation -> <repo root>
REPO = Path(os.getenv("REPO_ROOT", str(HERE.parents[3])))
OUT = Path(os.getenv("OUT_DIR", str(HERE)))
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:55432/ijm")
PORT = int(os.getenv("IJM_API_PORT", "58080"))
HEARTBEAT_S = os.getenv("IJM_DRIFT_HEARTBEAT_S", "2")
API = f"http://127.0.0.1:{PORT}"
NODE = "polimi-gpu"

records: list[dict] = []
phase = {"name": "A-baseline"}


async def _reset_db() -> None:
    conn = await psycopg.AsyncConnection.connect(DB_URL, autocommit=True)
    # schema.sql is idempotent (CREATE TABLE IF NOT EXISTS); apply it here so
    # TRUNCATE has a table to hit on a fresh DB (the API also applies it at
    # startup, but _reset_db runs before the API launches).
    await conn.execute((REPO / "backend/schema.sql").read_text())
    await conn.execute("TRUNCATE jobs")
    await conn.close()


def _launch_api(log_path: Path) -> subprocess.Popen:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO)
    env["DATABASE_URL"] = DB_URL
    env["IJM_DRIFT_HEARTBEAT_S"] = HEARTBEAT_S
    env["IJM_SLOT_VERBOSE"] = "1"
    env["NODES_CONFIG"] = str(HERE / "nodes_config.json")
    env["GPU_COSTS_CONFIG"] = str(HERE / "gpu_energy_costs.json")
    env.pop("OPTIMIZER_URL", None)
    log = open(log_path, "w")
    py = REPO / "backend/.venv/bin/python"
    py_exe = str(py) if py.exists() else sys.executable
    return subprocess.Popen(
        [py_exe, "-m", "uvicorn", "src.main:app", "--host", "127.0.0.1",
         "--port", str(PORT), "--log-level", "info"],
        cwd=str(REPO / "backend"), env=env, stdout=log, stderr=subprocess.STDOUT,
    )


async def _wait_healthy(timeout: float = 30.0) -> None:
    async with httpx.AsyncClient() as c:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if (await c.get(f"{API}/admin/slots", timeout=2.0)).status_code == 200:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.3)
    raise RuntimeError("API did not become healthy")


async def _insert_running(n_gpus: int) -> str:
    jid = str(uuid4())
    now = datetime.now(UTC)
    conn = await psycopg.AsyncConnection.connect(DB_URL, autocommit=True)
    await conn.execute(
        """INSERT INTO jobs (id, job_id, image, command, status, created_at, updated_at,
                             container_name, assigned_node, assigned_gpu_config, is_profiling_run)
           VALUES (%s,%s,%s,%s,'RUNNING',%s,%s,%s,%s,%s,FALSE)""",
        (jid, "cnn_big", "ijm-cnn-big:latest", Json(["python3", "train.py"]), now, now,
         f"ijm-{jid[:8]}", NODE, Json({"A40": n_gpus})),
    )
    await conn.close()
    return jid


async def _complete(jid: str) -> None:
    conn = await psycopg.AsyncConnection.connect(DB_URL, autocommit=True)
    await conn.execute("UPDATE jobs SET status='SUCCEEDED', updated_at=%s WHERE id=%s", (datetime.now(UTC), jid))
    await conn.close()


async def _poller(t0: float, stop: asyncio.Event) -> None:
    async with httpx.AsyncClient() as c:
        while not stop.is_set():
            try:
                j = (await c.get(f"{API}/admin/slots", timeout=2.0)).json()
                pn = j["per_node"][NODE]
                records.append({
                    "t": round(time.monotonic() - t0, 2),
                    "iso": datetime.now(UTC).isoformat(timespec="milliseconds"),
                    "phase": phase["name"],
                    "available": pn["available"], "used_mem": pn["used_mem"], "used_db": pn["used_db"],
                    "drift": pn["drift"], "drift_recoveries": j["metrics"]["drift_recovery_count"],
                    "acquires": j["metrics"]["acquire_count"], "releases": j["metrics"]["release_count"],
                })
            except Exception:
                pass
            await asyncio.sleep(0.4)


async def main() -> None:
    await _reset_db()
    proc = _launch_api(OUT / "api.log")
    try:
        await _wait_healthy()
        t0 = time.monotonic()
        stop = asyncio.Event()
        poll = asyncio.create_task(_poller(t0, stop))

        phase["name"] = "A-baseline"
        await asyncio.sleep(4)
        phase["name"] = "B-missed-acquire"
        j1 = await _insert_running(1)
        await asyncio.sleep(5)
        j2 = await _insert_running(1)
        await asyncio.sleep(5)
        phase["name"] = "C-dropped-notify"
        await _complete(j1)
        await _complete(j2)
        await asyncio.sleep(5)
        phase["name"] = "D-settled"
        await asyncio.sleep(3)

        stop.set()
        await poll
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()

    (OUT / "slots_timeline.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
    cols = ["t", "iso", "phase", "available", "used_mem", "used_db", "drift", "drift_recoveries", "acquires", "releases"]
    (OUT / "slots_timeline.csv").write_text(
        ",".join(cols) + "\n" + "".join(",".join(str(r[c]) for c in cols) + "\n" for r in records)
    )
    print(f"captured {len(records)} polls -> {OUT}")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)

# Fault-injection evidence — slot & drift invariants

This directory turns the control-plane invariants documented in
[`documentation/SLOT_INVARIANTS.md`](../../../SLOT_INVARIANTS.md) and the
architecture chapter's "Concurrency invariants" section from *asserted prose*
into *observed recovery*. Two complementary layers:

1. **Assertion-level** — `backend/tests/test_fault_injection.py` (real-SQL
   pytest, skips when no Postgres). Each test injects one fault and asserts the
   invariant holds against the real engine's row-locking.
2. **Run-level** — this directory. A live run of the real API against a
   disposable Postgres, with the autonomous drift heartbeat reconciling injected
   leaks in real time. Artifacts here are the `/admin/slots` timeseries and the
   API log; `Files/chart_faults.tex` (the report figure) is generated from
   `slots_timeline.csv` by `infra/generate_fault_chart.py`.

Everything is isolated from any operator deployment: a throwaway DB on port
55432, the API on 58080, and a dummy `workerUrl` so no container is launched.

## The run

Single node `polimi-gpu` = 2×A40 (capacity 2). Drift heartbeat
`IJM_DRIFT_HEARTBEAT_S=2`. Poller samples `GET /admin/slots` every 0.4 s.

| Phase | Injected fault | What it models |
|---|---|---|
| A baseline | none | idle node |
| B missed-acquire | `INSERT` two `RUNNING` rows the API never acquired | API restart with an in-flight container / acquire-direction of a lost event |
| C dropped slot-freed NOTIFY | flip both rows to `SUCCEEDED` **without** `NOTIFY ijm_slot_freed` | the `ijm_slot_freed` event lost on a tunnel reset |
| D settled | none | reconciled steady state |

## Observed result (`slots_timeline.csv`, change-points)

```
   t  phase              avail mem  db  drift  recoveries
0.01  A-baseline             2    0   0  False      0
4.07  B-missed-acquire       2    0   1  True       0   <- DB ahead of memory
6.10  B-missed-acquire       1    1   1  False      1   <- heartbeat ACQUIRED (+1)
9.34  B-missed-acquire       1    1   2  True       1   <- second row inserted
10.15 B-missed-acquire       0    2   2  False      2   <- heartbeat ACQUIRED (+1), node full
14.21 C-dropped-notify       0    2   0  True       2   <- both completed, NOTIFY dropped
15.84 C-dropped-notify       2    0   0  False      3   <- heartbeat RELEASED (-2), leak reclaimed
19.48 D-settled              2    0   0  False      3
```

The `drift` flag flips **true → false** at every fault and the
`drift_recoveries` counter advances, in **both** directions — the heartbeat
*acquires* when the DB knows of more occupied slots than memory (B) and
*releases* when memory holds permits the DB no longer backs (C). No
`SLOT-OVERSUB` / `SLOT-OVERRELEASE` line appears in `api.log` at any point.

The matching `api.log` lines:

```
NodeSlots reconciled: available={'polimi-gpu': 2}
Drift watcher: {'polimi-gpu': (0, 1)} — reconciling
recover_from_drift: polimi-gpu adjusted by +1 (mem_used 0→1, db_used=1)
Drift watcher: {'polimi-gpu': (1, 2)} — reconciling
recover_from_drift: polimi-gpu adjusted by +1 (mem_used 1→2, db_used=2)
Drift watcher: {'polimi-gpu': (2, 0)} — reconciling
recover_from_drift: polimi-gpu adjusted by -2 (mem_used 2→0, db_used=0)
```

## Files

| File | Contents |
|---|---|
| `run.py` | the driver (boots API, injects faults, polls `/admin/slots`) |
| `nodes_config.json`, `gpu_energy_costs.json` | the isolated 1-node cluster |
| `slots_timeline.jsonl` / `.csv` | every poll sample (full timeseries; source for the chart) |
| `api.log` | the API's own log over the run (drift-watcher + reconcile lines) |

## Reproduce

```bash
docker run -d --name ijm-fault-pg -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=ijm -p 55432:5432 postgres:18
# wait for readiness: docker exec ijm-fault-pg pg_isready -U postgres -d ijm
DATABASE_URL=postgresql://postgres:postgres@localhost:55432/ijm \
    python documentation/report/e2e_runs/fault_injection/run.py
docker rm -f ijm-fault-pg
# regenerate the report figure from the fresh timeseries:
python infra/generate_fault_chart.py
```

The assertion-level suite (no live API needed):

```bash
cd backend
DATABASE_URL=postgresql://postgres:postgres@localhost:55432/ijm \
    uv run pytest tests/test_fault_injection.py -v
```

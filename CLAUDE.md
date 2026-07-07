# CLAUDE.md

Guidance for AI pair-programming (Claude Code, claude.ai/code) on this
repository. For human-facing setup, commands, ports, environment
variables, and deployment, see [README.md](README.md) — this file does
not duplicate that material.

## Project, in one paragraph

**Intelligent Job Management (IJM)** — a job-management system for GPU
deep-learning clusters with stoppable/resumable jobs, modelled after the
ANDREAS project (Polimi). Docker-based execution on per-node workers
(`worker/` drives the Docker CLI directly); PostgreSQL for state; optional
GPUspb optimizer for cost-aware batch scheduling. Architecture: FastAPI backend
(`backend/src/`) dispatching over HTTP to per-node workers (`worker/`);
React 19 SPA frontend; training containers in `runtime/`. See
[README.md](README.md#architecture) for the full diagram and the job
lifecycle.

## Engineering principles

These are the project's load-bearing rules — keep them in mind on every
change.

**No band-aids.** When you find a bug, fix the root cause — do not patch
the symptom. If a rule of thumb (a guard, an extra `IF`, a sweep, a
"rescue" UPDATE) compensates for a wrong invariant elsewhere, the wrong
invariant is the bug; remove the band-aid and fix the source of truth.
If you cannot identify a root cause, say so explicitly instead of
inserting a heuristic. Code paths that exist only because "X sometimes
happens" decay into load-bearing hacks; refuse to write them.

**One source of truth per fact.** A piece of state should be derivable
in exactly one place. When two stores (DB row, in-memory counter, flag)
both encode the same fact, they will drift; pick the authoritative one
and derive the rest from it. Removing redundancy is usually the fix.

**Never weaken a scenario assertion to make a buggy run pass.** If a
scenario fails, either the system is wrong (fix the system) or the
assertion is wrong (justify and rewrite the assertion). Never edit the
scenario to side-step a real bug.

## AI-dev gotchas

Things that have tripped up past sessions:

- **Distributed is the supported topology.** API on user's machine,
  postgres + workers on remote GPU nodes via SSH-tunnelled HTTP. The
  local-only `docker compose up` path exists for dev convenience but is
  **not** the target. Any tradeoff between "local-dev simplicity" and
  "distributed correctness" resolves toward distributed.

- **matemagician's CUDA-10.1 ceiling.** Its NVIDIA driver (418.x) cannot
  run the default `:latest` (PyTorch 2.6 / CUDA 12.4) images, so its
  deploy uses `IMAGE_TAG_OVERRIDE=legacy` to rewrite the tag to the
  PyTorch 1.5.1 / CUDA 10.1 build from `runtime/Dockerfile.legacy`. The
  worker code must stay importable under Python 3.7 (the legacy image)
  — `runtime/base.py` uses `from __future__ import annotations`.

- **Cross-node checkpoint resume must work across torch versions.**
  Checkpoints are written in the pre-1.6 torch serialization format and
  loaded with `strict=False` + try/except around the optimizer state
  dict. `cnn_big` specifically migrates between modern A40 and legacy
  P600 workers and exercises this path — don't tighten the loader.

- **`cnn_big` is the placement-choice scenario.** Deliberately sized so
  per-step compute amortises DataParallel sync overhead on both GPU
  classes (P600×2 is 1.68× faster than P600×1; A40×2 is 1.12× faster
  than A40×1, per the 2026-06-25 profiling runs, after the per-epoch
  profiler-measurement fix). `e2e_scenario_2gpu.sh` (Scenario 2b) uses it
  to exercise the 2-GPU standard-placement path;
  `e2e_scenario_2types.sh` (Scenario 2) uses it for the A40 pins and the
  urgent job (which lands on P600×1, then migrates to A40). Don't change
  its shape without re-measuring.

- **Profile-always policy.** The `ProfilingScheduler` runs one untested
  GPU configuration per submission. Jobs transition
  `QUEUED → PROFILING → QUEUED → RUNNING`. Re-queuing happens via
  PostgreSQL `NOTIFY ijm_schedule` after profiling completes — don't
  add an in-memory fast-path that bypasses it.

- **Slot accounting is `pg` + `node_slots.py`.** `db_used` (counted from
  RUNNING/PROFILING rows) is authoritative; `mem_used` is the in-memory
  semaphore mirror, reconciled to it by a periodic drift heartbeat
  (`IJM_DRIFT_HEARTBEAT_S`). The heartbeat only checks at quiescence
  (`drift_tick` in `backend/src/app.py`) — while a dispatch or migrate is
  in flight the two counters legitimately disagree; don't remove the
  gate, and never paper over a slot mismatch with a rescue UPDATE — fix
  the producer.

## Design docs

Read these before making non-trivial scheduler or worker changes:

- [documentation/SLOT_INVARIANTS.md](documentation/SLOT_INVARIANTS.md)
  — the invariants the slot tracker is required to maintain.
- [documentation/e2e-scenarios.md](documentation/e2e-scenarios.md)
  — what each `infra/e2e_scenario*.sh` exercises and why.
- [documentation/andreas.md](documentation/andreas.md)
  — notes on the upstream ANDREAS design IJM is modelled after.
- [documentation/external/](documentation/external/) — third-party
  reference PDFs (ANDREAS deliverables, GPUspb paper).

## Common pitfalls when making changes

- Tests live in `backend/tests/` (real pytest suite, 8 test modules —
  `test_fault_injection.py` and `test_sql_integration.py` need a
  reachable Postgres, else they skip) and `worker/tests/` (thin — only
  `test_profiling_duration.py`; most worker behaviour is covered by
  `infra/e2e_scenario*.sh`). Adding worker-side logic without an e2e to
  back it up is a known gap.
- Pre-commit hooks enforce ruff + mypy strict + deptry + eslint. See
  [.pre-commit-config.yaml](.pre-commit-config.yaml). Don't disable
  hooks; fix the cause.
- The PostgreSQL data volume must be mounted at `/var/lib/postgresql`
  (not `/var/lib/postgresql/data`) — enforced by
  `backend/tests/test_infra.py`. Postgres 18+ requirement.

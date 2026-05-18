# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Intelligent Job Management (IJM) — a job management system for GPU deep learning clusters with stoppable/resumable jobs. Modeled after the ANDREAS project (Polimi). Docker-based execution with an Executor abstraction for future SLURM integration. PostgreSQL for state.

**Deployment target.** The only supported configuration is the **distributed cluster: API runs on the user's machine, postgres + workers run on remote GPU nodes (matemagician, polimi-gpu) reached over SSH-tunnelled HTTP.** Single-process / docker-compose-on-localhost runs are useful for development but are not the supported topology. Any tradeoff between "local-dev simplicity" and "distributed correctness" resolves toward distributed.

## Common Commands

### Full stack (distributed cluster — the supported path)
```bash
# 1. SSH tunnels to remote postgres + workers (one-shot, leave running)
bash infra/tunnel.sh polimi   # forwards 5433, 8001, 8002 with ServerAlive*

# 2. Native API + dockerised optimizer + frontend.
bash infra/launch.sh tunnel

# 3. Run scenarios
bash infra/e2e_scenario.sh                # single-type (lstm-small) sweep
bash infra/e2e_scenario_2types.sh         # lstm-small + cnn_big, urgent preempts, cross-node resume
```

### Full stack (Docker Compose, local-only — dev / smoke tests)
```bash
docker build -t ijm-lstm-small:dev --build-arg SCRIPT=lstm_small.py runtime/  # Build runtime image (needed first)
mkdir -p data/pg data/checkpoints data/runs   # Create data dirs
cd infra && docker compose up --build         # Start all services
```

### Backend (Python 3.13, uses uv)
```bash
cd backend
uv sync                        # Install dependencies
uv run pytest                  # Run tests (coverage auto-enabled)
uv run pytest tests/test_main.py::test_health  # Run single test
uv run ruff check .            # Lint
uv run ruff format .           # Format
uv run mypy src                # Type check (strict mode)
uv run deptry .                # Check for unused/missing deps
```

### Frontend (Node 23+, uses pnpm)
```bash
cd frontend
pnpm install                   # Install dependencies
pnpm dev                       # Dev server on :5173
pnpm build                     # Type-check + production build
pnpm lint                      # ESLint
```

## Architecture

**Async-first, single-process** — the API handles both HTTP requests and job execution:

1. **API** (`backend/src/`) — FastAPI app. Manages job records in PostgreSQL. Contains the `JobRunner` (`job_runner.py`) which executes training containers concurrently via an `Executor` interface. Includes a `ProfilingScheduler` (`profiling.py`) that incrementally profiles ONE untested GPU configuration per submission. Optional integration with the GPUspb optimizer (`optimizer.py`) for cost-aware batch scheduling. Modular layout: `app.py` (factory + lifespan), `job_runner.py` (container execution), `executors/` (Docker/SLURM backends), `cluster.py` (ClusterManager), `profiling.py` (ProfilingScheduler), `optimizer.py` (GPUspb client), `state.py` (shared mutable state), `models.py`, `routers/`.

2. **Frontend** (`frontend/`) — React 19 SPA with Tailwind CSS + shadcn/ui components, React Router for multi-page navigation (Dashboard, Job Queue, Submit Job, Cluster Status, Profiling), TanStack React Query for data fetching (polls every 3-5s). API base URL configurable via `VITE_API_URL` env var (defaults to `http://localhost:8000`).
   - Path alias: `@/` maps to `src/`
   - Key directories: `src/api/` (client + React Query hooks), `src/components/ui/` (shadcn primitives), `src/components/` (custom), `src/pages/`, `src/config/features.ts` (feature flags)

3. **Runtime** (`runtime/`) — Training containers matching ANDREAS job types. Shared base class in `base.py`, individual scripts: `lstm_small.py`, `lstm_big.py`, `convnet.py`, `efficientnet.py`, `cnn_big.py`. **Two Dockerfiles**: `Dockerfile` (PyTorch 2.6 + CUDA 12.4, default `:latest` tag) and `Dockerfile.legacy` (PyTorch 1.5.1 + CUDA 10.1, `:legacy` tag — used on matemagician whose driver maxes out at CUDA 10.1). Checkpoints are saved in the **legacy (pre-1.6) torch serialization format** and loaded with `strict=False` + try/except around the optimizer-state dict, so a job can resume across nodes regardless of torch version (cnn_big specifically migrates between modern A40 and legacy P600 workers and needs this). `base.py` uses `from __future__ import annotations` to stay importable on Python 3.7 (the legacy image). `download_dataset` first tries `download=False` (uses pre-staged data at `data/datasets/MNIST/`, `data/datasets/cifar-10-batches-py/`) and only falls back to `download=True` on RuntimeError, which sidesteps the rootless-docker DNS flakiness on polimi-gpu. Each script saves checkpoints after every epoch, loads on startup if exists. No SIGTERM handling — system kills containers between epochs (at most 1 epoch lost). Real datasets: MNIST (LSTM) and CIFAR-10 (CNN/EfficientNet); synthetic 128×128×3 tensors for cnn_big (no torchvision dependency at all).

**The cnn_big type** is a deep CNN (10 conv blocks, channels $3{\to}64{\to}128{\to}256{\to}384{\to}512$, $128{\times}128{\times}3$ input, batch 32) designed so per-step compute amortises the DataParallel sync overhead on \emph{both} GPU classes — measured profile: A40×1=6.29 s/epoch vs A40×2=5.63 s/epoch (1.12× faster on 2 GPUs); P600×1=51.06 s/epoch vs P600×2=30.32 s/epoch (1.68× faster). It is the type that exercises Scenario 2's 2-GPU placement-choice path.

### Executor Abstraction
Container execution is decoupled via `src/executors/`:
- `DockerExecutor` — runs containers via Docker CLI (current default)
- `MockSlurmExecutor` — logs SLURM commands but runs Docker locally (for testing)
- Future: `SlurmExecutor` for real cluster deployment

Set via `EXECUTOR` env var: `docker` (default) or `mock-slurm`.

### Job State Machine
```
QUEUED → PROFILING → QUEUED (re-queued as standard run) → RUNNING → SUCCEEDED / FAILED
                                                               ↘ PREEMPTED ─┬──→ QUEUED (resume)
                                                                 FAILED ─────┘
```

### Data Persistence
- PostgreSQL stores job metadata (`jobs` table) and profiling results (`profiling_results` table)
- GPU configurations stored as JSONB (`{"A40": 2}` or `{"A40": 1, "L40S": 1}` for mixed nodes)
- Checkpoints: `data/checkpoints/{job_id}/` mounted to container `/checkpoints`
- Run outputs: `data/runs/{job_id}/` mounted to container `/runs`

## Code Style

**Python**: ruff (line-length 120, double quotes), mypy strict mode, Python 3.13 target. All functions must have type annotations. Pre-commit hooks enforce ruff + mypy.

**TypeScript/React**: ESLint with react-hooks and react-refresh plugins. Tailwind CSS for styling, shadcn/ui component library. `strict: true` + `verbatimModuleSyntax` in tsconfig.

## Key Environment Variables

| Variable | Used by | Default in Docker Compose |
|---|---|---|
| `DATABASE_URL` | API | `postgresql://postgres:postgres@postgres:5432/ijm` |
| `HOST_ROOT` | API | `/host` (maps to repo root) |
| `HOST_PROJECT_ROOT` | API | `${PWD}/..` (host-resolvable path for Docker volumes) |
| `EXECUTOR` | API | `docker` (or `mock-slurm`) |
| `OPTIMIZER_URL` | API | unset (jobs wait until profiling completes, then sit idle); set to `http://optimizer:8080` for batch optimizer (required for placement). |
| `IJM_DRIFT_HEARTBEAT_S` | API | `15` — period of the slot-tracker drift heartbeat that reconciles `mem_used` against `db_used` per node.  Lower = faster recovery from acquire/release races, higher = less per-round overhead.  Counted in the `drift_recovery_count` metric exposed at `/admin/slots`. |
| `WORKER_GPU_MODE` | worker | `runtime` (other values: `cdi` for rootless docker on polimi-gpu, `none` for CPU only). Selects how `docker run` is told to expose GPUs to the training container. |
| `IMAGE_TAG_OVERRIDE` | worker | unset. If set, rewrites the job's image tag (`:latest` → `:$IMAGE_TAG_OVERRIDE`). Used on matemagician (`legacy`) where the node's NVIDIA driver (418.x, CUDA 10.1 max) cannot run the default CUDA-12.4 PyTorch image. |

## Ports

5173 (frontend), 8000 (API), 5432 (PostgreSQL), 8080 (optimizer, optional)

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Intelligent Job Management (IJM) — a job management system for GPU deep learning clusters with stoppable/resumable jobs. Modeled after the ANDREAS project (Polimi). Docker-based execution with an Executor abstraction for future SLURM integration. PostgreSQL for state.

## Common Commands

### Full stack (Docker Compose)
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

1. **API** (`backend/src/`) — FastAPI app. Manages job records in PostgreSQL. Contains the `JobRunner` (`job_runner.py`) which executes training containers concurrently via an `Executor` interface. Includes a `ProfilingScheduler` (`profiling.py`) that incrementally profiles ONE untested GPU configuration per submission. Modular layout: `app.py` (factory + lifespan), `job_runner.py` (container execution), `executors/` (Docker/SLURM backends), `cluster.py` (ClusterManager), `profiling.py` (ProfilingScheduler), `state.py` (shared mutable state), `models.py`, `routers/`.

2. **Frontend** (`frontend/`) — React 19 SPA with Tailwind CSS + shadcn/ui components, React Router for multi-page navigation (Dashboard, Job Queue, Submit Job, Cluster Status, Profiling), TanStack React Query for data fetching (polls every 3-5s). API base URL configurable via `VITE_API_URL` env var (defaults to `http://localhost:8000`).
   - Path alias: `@/` maps to `src/`
   - Key directories: `src/api/` (client + React Query hooks), `src/components/ui/` (shadcn primitives), `src/components/` (custom), `src/pages/`, `src/config/features.ts` (feature flags)

3. **Runtime** (`runtime/`) — Training containers matching ANDREAS job types. Shared base class in `base.py`, individual scripts: `lstm_small.py`, `lstm_big.py`, `convnet.py`, `efficientnet.py`. Single Dockerfile with `SCRIPT` build arg. Each saves checkpoints after every epoch, loads on startup if exists. No SIGTERM handling — system kills containers between epochs (at most 1 epoch lost). Real datasets: MNIST (LSTM) and CIFAR-10 (CNN/EfficientNet).

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

## Ports

5173 (frontend), 8000 (API), 5432 (PostgreSQL)

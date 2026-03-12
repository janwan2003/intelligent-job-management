# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Intelligent Job Management (IJM) — a minimal job management system for GPU deep learning clusters with stoppable/resumable jobs. v0.1.0 prototype using Docker-based execution, PostgreSQL state, and NATS JetStream events.

## Common Commands

### Full stack (Docker Compose)
```bash
docker build -t ijm-runtime:dev runtime/     # Build runtime image (needed first)
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

**Event-driven, async-first** — three services communicate via NATS JetStream:

1. **API** (`backend/src/`) — FastAPI app. Manages job records in PostgreSQL, publishes events (`jobs.submitted`, `jobs.stop_requested`) to NATS stream "JOBS". Includes a `ProfilingScheduler` (`profiling.py`) that incrementally profiles ONE untested GPU configuration per submission, then immediately runs the job on the best available config. Modular layout: `app.py` (factory + lifespan), `cluster.py` (ClusterManager), `profiling.py` (ProfilingScheduler), `state.py` (shared mutable state), `models.py`, `routers/`.

2. **Worker** (`worker/worker.py`) — Async Python process. Subscribes to NATS events, runs jobs as Docker containers with concurrent execution. Manages container lifecycle: start, stop (SIGTERM with 30s grace), resume. Reports profiling results back to the API via NATS (`jobs.profiling_complete`). Reconciles DB state with Docker on startup.

3. **Frontend** (`frontend/`) — React 19 SPA with Tailwind CSS + shadcn/ui components, React Router for multi-page navigation (Dashboard, Job Queue, Submit Job, Cluster Status, Profiling), TanStack React Query for data fetching (polls every 3-5s). Features not yet supported by the API are gated behind boolean flags in `src/config/features.ts`. API base URL configurable via `VITE_API_URL` env var (defaults to `http://localhost:8000`).
   - Path alias: `@/` maps to `src/`
   - Key directories: `src/api/` (client + React Query hooks), `src/components/ui/` (shadcn primitives), `src/components/` (custom), `src/pages/`, `src/config/features.ts` (feature flags)

4. **Runtime** (`runtime/`) — Example training containers demonstrating the checkpoint contract: write to `/checkpoints/latest.pt`, load on startup if exists, handle SIGTERM by checkpointing then exiting cleanly. Includes `train.py` (simple MLP), `train_cnn.py` (ConvNet), `train_lstm.py` (LSTM), and `train_efficientnet.py` (EfficientNet-style).

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
| `DATABASE_URL` | API, Worker | `postgresql://postgres:postgres@postgres:5432/ijm` |
| `NATS_URL` | API, Worker | `nats://nats:4222` |
| `HOST_ROOT` | Worker | `/host` (maps to repo root) |

## Ports

5173 (frontend), 8000 (API), 4222/8222 (NATS), 5432 (PostgreSQL)

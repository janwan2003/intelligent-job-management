# Intelligent Job Management System

A job management system for GPU deep learning clusters with profiling-based scheduling, stoppable/resumable jobs, and multi-node support. Modelled after the ANDREAS project (Polimi).

## Prerequisites

- Docker & Docker Compose
- Python 3.13+ with [uv](https://docs.astral.sh/uv/)
- Node.js 24+ with [pnpm](https://pnpm.io/)

## Quick Start (local)

### 1. Build the runtime images

```bash
docker build -t ijm-lstm-small:dev --build-arg SCRIPT=lstm_small.py runtime/
docker build -t ijm-lstm-big:dev   --build-arg SCRIPT=lstm_big.py   runtime/
docker build -t ijm-convnet:dev    --build-arg SCRIPT=convnet.py    runtime/
docker build -t ijm-efficientnet:dev --build-arg SCRIPT=efficientnet.py runtime/
```

### 2. Create data directories

```bash
mkdir -p data/pg data/checkpoints data/runs
```

### 3. Start all services

```bash
cd infra && docker compose up --build
```

Opens:
- **Frontend** → http://localhost:5173
- **API** → http://localhost:8000
- **Postgres** → localhost:5432

The API runs jobs directly via its embedded `JobRunner` + `DockerExecutor` — no separate worker process needed for local dev.

---

## Cluster Deployment (Polimi server)

The production setup splits responsibilities: the **worker** runs on the GPU node, the **API** runs anywhere (laptop, CI server) and connects via SSH tunnel.

### Server side — deploy once

```bash
# On the server
cd ~/ijm/infra
docker-compose -f docker-compose.server.yml up -d   # starts postgres + worker only
```

The server runs two containers:
- **postgres** (port 5433) — shared job state database
- **worker** (port 8001) — executes Docker training containers on the GPU node

### Client side — run API locally against the cluster

```bash
# Terminal 1: open SSH tunnels (keeps running)
./infra/tunnel.sh          # forwards localhost:5433 and localhost:8001 to server

# Terminal 2: start the API
cd backend
DATABASE_URL=postgresql://postgres:postgres@localhost:5433/ijm \
NODES_CONFIG=config/nodes_config.tunnel.json \
HOST_PROJECT_ROOT=/home/wangrat/ijm \
uv run uvicorn src.app:app --port 8000
```

Then open the frontend normally (`cd frontend && pnpm dev` or point `VITE_API_URL` at the API).

The tunnel forwards the server's ports since the Polimi network does not expose them directly. Close Terminal 1 to disconnect.

### Building the runtime image on the server

```bash
ssh polimi
docker build -t wangrat/ijm-lstm-small:latest \
  --build-arg SCRIPT=lstm_small.py ~/ijm/runtime/
```

---

## Architecture

```
User / Frontend
      │  REST
      ▼
Central API  (FastAPI — scheduler + state + dispatch)
      │  POST /jobs/{id}/run          ← HTTP dispatch
      │  POST /jobs/{id}/stop
      ▼
Worker  (FastAPI — per GPU node, port 8001)
      ├── runs Docker training containers
      ├── streams logs + progress to DB
      └── on profiling done: writes duration to DB,
          sends NOTIFY ijm_schedule,
          API re-schedules immediately
```

**Local dev**: nodes without `workerUrl` in config use the embedded `JobRunner + DockerExecutor` — no worker server needed.

**Multi-node**: each GPU node runs a worker container; set `"workerUrl": "http://<host>:8001"` in nodes_config.

### Job lifecycle

```
QUEUED → PROFILING → QUEUED → RUNNING → SUCCEEDED
                       ↑          ↘ PREEMPTED → QUEUED (resume)
                       └──────────── FAILED    → QUEUED (resume)
```

Each new job type runs a short profiling pass first to measure GPU throughput. After profiling, the job is immediately re-scheduled (via PostgreSQL `NOTIFY`) onto the best available configuration.

### Sample training images

| Image | Script | Model | Dataset |
|-------|--------|-------|---------|
| `ijm-lstm-small:dev` | `lstm_small.py` | LSTM (1-layer, 128 hidden) | MNIST |
| `ijm-lstm-big:dev`   | `lstm_big.py`   | LSTM (3-layer, 256 hidden) | MNIST |
| `ijm-convnet:dev`    | `convnet.py`    | ConvNet (3-layer CNN + BN) | CIFAR-10 |
| `ijm-efficientnet:dev` | `efficientnet.py` | EfficientNet (MBConv) | CIFAR-10 |

---

## Development

### Backend

```bash
cd backend
uv sync
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ijm \
HOST_PROJECT_ROOT=$(cd .. && pwd) \
uv run uvicorn src.app:app --port 8000 --reload
```

Requires a running Postgres: `cd infra && docker compose up postgres`

### Frontend

```bash
cd frontend
pnpm install
pnpm dev        # dev server on :5173
```

### Worker (standalone)

```bash
cd worker
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ijm \
HOST_PROJECT_ROOT=$(cd .. && pwd) \
NODE_ID=local-worker \
uvicorn app:app --port 8001
```

### Tests

```bash
cd backend && uv run pytest          # unit tests + infra config validation
cd frontend && pnpm lint && pnpm build   # lint + type-check
cd infra && ./smoke_test.sh          # full-stack smoke test
```

### Pre-commit hooks

```bash
pip install pre-commit
pre-commit install
```

| Hook | Scope | What it does |
|------|-------|-------------|
| **ruff** (lint + fix) | Python | Linting with auto-fix |
| **ruff-format** | Python | Code formatting |
| **mypy** (backend) | shared/, backend/ | Static type checking |
| **mypy** (worker) | worker/ | Static type checking |
| **deptry** | backend/ | Unused/missing deps |
| **eslint** | frontend/src/ | TypeScript/React linting |
| **tsc** | frontend/src/ | TypeScript type checking |

---

## API Reference

### Jobs
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | Submit a job |
| `GET`  | `/jobs` | List all jobs |
| `GET`  | `/jobs/{id}` | Get job details |
| `POST` | `/jobs/{id}/stop` | Stop a running job |
| `POST` | `/jobs/{id}/resume` | Resume a preempted/failed job |
| `DELETE` | `/jobs/{id}` | Delete job and profiling results |
| `GET`  | `/jobs/{id}/logs` | Stream container output |

### Cluster
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/nodes` | List nodes with status |
| `GET` | `/configurations` | List valid GPU configurations |
| `GET` | `/gpu-costs` | GPU energy cost weights |
| `GET` | `/profiling-results/{job_id}` | Profiling results for a job type |

---

## Project Structure

```
backend/    FastAPI API — scheduler, job dispatcher, profiling, routers
shared/     Shared constants (JobStatus, pg notify channel) — backend + worker
frontend/   React 19 SPA — Dashboard, Job Queue, Submit, Cluster, Profiling
worker/     HTTP worker server — executes Docker containers on GPU nodes
runtime/    Training container images (LSTM, ConvNet, EfficientNet)
infra/      Docker Compose configs + smoke test + tunnel.sh
config/     Cluster node configs (local, server, tunnel)
data/       Persistent data (pg/, checkpoints/, runs/)
```

## Tech Stack

**Backend**: Python 3.13, FastAPI, psycopg3, psycopg-pool, uv
**Frontend**: TypeScript, React 19, Vite, TanStack Query, Tailwind, shadcn/ui
**Worker**: Python 3.13, FastAPI, asyncio, Docker CLI
**Infrastructure**: Docker, PostgreSQL 16

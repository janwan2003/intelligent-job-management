# Intelligent Job Management System

A minimal end-to-end job management system for GPU-accelerated deep learning clusters with profiling-based scheduling, stoppable/resumable jobs, and mixed-GPU support.

## Prerequisites

- Node.js 24+
- [pnpm](https://pnpm.io/) - `curl -fsSL https://get.pnpm.io/install.sh | sh -`
- Docker & Docker Compose
- Python 3.13+

## Quick Start

### 1. Build the runtime containers

```bash
docker build -t ijm-runtime:dev runtime/                                        # Simple MLP
docker build -t ijm-cnn:dev -f runtime/Dockerfile.cnn runtime/                  # CNN
docker build -t ijm-lstm:dev -f runtime/Dockerfile.lstm runtime/                # LSTM
docker build -t ijm-efficientnet:dev -f runtime/Dockerfile.efficientnet runtime/ # EfficientNet
```

### 2. Create data directories

```bash
mkdir -p data/pg data/checkpoints data/runs
```

### 3. Start all services

```bash
cd infra
docker compose up --build
```

This starts:
- **Postgres** (port 5432) - Job state database
- **NATS** (ports 4222, 8222) - Event bus with JetStream
- **API** (port 8000) - FastAPI backend with profiling scheduler
- **Worker** - Job executor with Docker container management
- **Frontend** (port 5173) - React UI

### 4. Access the UI

Open [http://localhost:5173](http://localhost:5173) in your browser.

## Acceptance Test

Test the complete workflow:

1. **Submit a job**:
   - Open the UI at http://localhost:5173
   - Use the Submit Job form with:
     - Image: `ijm-runtime:dev` (or `ijm-cnn:dev`, `ijm-lstm:dev`, `ijm-efficientnet:dev`)
     - Command: `python -u train.py` (or `train_cnn.py`, `train_lstm.py`, `train_efficientnet.py`)
   - Job is assigned a GPU configuration and enters `PROFILING` mode first
   - After profiling, it runs on the best configuration in `RUNNING` mode

2. **Stop the job**:
   - Click "Stop" button on the running job
   - Container receives SIGTERM and exits cleanly
   - Checkpoint file is created at `data/checkpoints/<job_id>/latest.pt`
   - Job status changes to `PREEMPTED`

3. **Resume the job**:
   - Click "Resume" button on the preempted job
   - Container starts again with same checkpoint mount
   - Runtime loads checkpoint and continues from previous step
   - Job profiles the next untested GPU configuration

4. **Verify checkpoint persistence**:
   - Check that training continues from the step where it was stopped
   - Look at console output to see step numbers resume correctly

## Architecture

### Services

- **Frontend (React)**: Job submission, queue management, cluster status, and profiling results UI
- **API (FastAPI)**: REST endpoints for job management, profiling scheduler, persists to Postgres, publishes to NATS
- **Worker (Python)**: Consumes NATS events, executes jobs in Docker containers with concurrent execution
- **Postgres**: Stores job metadata, state, and profiling results
- **NATS JetStream**: Event bus for job lifecycle and profiling events

### Data Flow

1. User submits job via React UI → POST `/jobs`
2. API creates job record in Postgres with status `QUEUED`
3. Profiling scheduler assigns a GPU configuration to profile
4. API publishes `jobs.submitted` event to NATS
5. Worker runs job in Docker container with checkpoint/log mounts
6. Worker reports profiling duration back to API via `jobs.profiling_complete`
7. API schedules next profiling run or standard run on best config
8. Job can be stopped (SIGTERM) or resumed (restart with same mounts)

### Job States

- `QUEUED` - Job submitted, waiting for execution
- `PROFILING` - Job running a profiling pass on a GPU configuration
- `RUNNING` - Job executing in standard mode on best configuration
- `PREEMPTED` - Job was stopped by user request
- `SUCCEEDED` - Job completed successfully (exit code 0)
- `FAILED` - Job failed (non-zero exit code)

### Runtime Contract

Training containers must:
- Write checkpoints to `/checkpoints/latest.pt`
- Load checkpoint on startup if it exists
- Handle SIGTERM gracefully by checkpointing and exiting with code 0
- Periodically checkpoint during training

### Sample Training Images

| Image | Script | Architecture | Description |
|-------|--------|-------------|-------------|
| `ijm-runtime:dev` | `train.py` | Simple MLP | 2-layer feedforward network |
| `ijm-cnn:dev` | `train_cnn.py` | ConvNet | 3-layer CNN for image classification |
| `ijm-lstm:dev` | `train_lstm.py` | LSTM | 2-layer LSTM for sequence modelling |
| `ijm-efficientnet:dev` | `train_efficientnet.py` | EfficientNet | MBConv-based image classifier |

All images follow the same checkpoint contract and support `MAX_STEPS` and `BATCH_SIZE` environment variables.

## Development

All services depend on PostgreSQL and NATS. Start them first, then run whichever service(s) you need natively:

### 1. Start infrastructure (required)

```bash
cd infra && docker compose up postgres nats
```

### 2. Backend

```bash
cd backend
uv sync
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ijm \
NATS_URL=nats://localhost:4222 \
DATA_DIR=../data \
uv run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Frontend

```bash
cd frontend
pnpm install
pnpm dev
```

### 4. Worker

```bash
cd worker
pip install -r requirements.txt
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ijm \
NATS_URL=nats://localhost:4222 \
HOST_ROOT=.. \
HOST_PROJECT_ROOT=$(cd .. && pwd) \
python worker.py
```

### Running tests

```bash
cd backend && uv run pytest          # Backend unit tests + infra config validation
cd worker  && python -m pytest       # Worker tests
cd frontend && pnpm lint && pnpm build   # Frontend lint + type-check
cd infra && ./smoke_test.sh          # Full-stack smoke test (starts Docker Compose)
```

The backend suite includes `tests/test_infra.py` which validates `infra/docker-compose.yml` for known version-specific requirements (e.g., postgres 18+ volume mount path). This catches config/version incompatibilities before running the stack.

## API Endpoints

### Jobs
- `POST /jobs` - Submit new job (requires `image` and `command`)
- `GET /jobs` - List all jobs (newest first)
- `GET /jobs/{id}` - Get specific job
- `POST /jobs/{id}/stop` - Stop a QUEUED, PROFILING, or RUNNING job
- `POST /jobs/{id}/resume` - Resume a PREEMPTED or FAILED job
- `DELETE /jobs/{id}` - Delete a job and its profiling results
- `GET /jobs/{id}/logs` - Get container output logs

### Cluster
- `GET /nodes` - List all cluster nodes with status
- `GET /configurations` - List all valid GPU configurations
- `GET /gpu-costs` - Get GPU energy cost weights

### Profiling
- `GET /profiling-results/{job_id}` - Get profiling results for a job (sorted by duration)

### Images
- `POST /images/upload` - Upload a Docker image (.tar/.tar.gz)

## NATS Subjects

- `jobs.submitted` - New job created (or resumed)
- `jobs.stop_requested` - User requested stop for a RUNNING job
- `jobs.profiling_complete` - Worker completed a profiling run

## Project Structure

```
backend/          # FastAPI application + profiling scheduler (modular: app, cluster, profiling, routers/)
shared/           # Shared constants (JobStatus enum, NATS subjects) — imported by both backend and worker
frontend/         # React UI (Dashboard, Job Queue, Submit, Cluster, Profiling)
worker/           # Job execution worker
runtime/          # Training containers with checkpoint support
  train.py        # Simple MLP
  train_cnn.py    # CNN image classifier
  train_lstm.py   # LSTM sequence model
  train_efficientnet.py  # EfficientNet-style classifier
infra/            # Docker Compose configuration
config/           # Cluster configuration (nodes, GPU energy costs)
data/             # Local persistent data
  pg/             # Postgres data
  checkpoints/    # Job checkpoints
  runs/           # Job outputs
documentation/    # ANDREAS project deliverables (D1, D2, D3)
```

## Tech Stack

**Backend**: Python 3.13, FastAPI, psycopg (Postgres), nats-py, uv
**Frontend**: TypeScript, React 19, Vite, TanStack React Query, Tailwind, shadcn/ui
**Infrastructure**: Docker, Postgres 16, NATS 2.12 with JetStream
**Worker**: Python 3.13, asyncio, Docker CLI, pip

## Non-Goals (v0)

- Authentication/authorization
- API gateway
- Kubernetes deployment
- Multi-tenancy
- Resource quotas/limits

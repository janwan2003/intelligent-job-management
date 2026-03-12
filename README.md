# Intelligent Job Management System

A minimal end-to-end job management system for GPU-accelerated deep learning clusters with support for stoppable/resumable jobs.

## Prerequisites

- Node.js 23+
- [pnpm](https://pnpm.io/) - `curl -fsSL https://get.pnpm.io/install.sh | sh -`
- Docker & Docker Compose
- Python 3.11+

## Quick Start

### 1. Build the runtime container

```bash
docker build -t ijm-runtime:dev runtime/
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
- **API** (port 8000) - FastAPI backend
- **Worker** - Job executor with Docker container management
- **Frontend** (port 5173) - React UI

### 4. Access the UI

Open [http://localhost:5173](http://localhost:5173) in your browser.

## Acceptance Test

Test the complete workflow:

1. **Submit a job**:
   - Open the UI at http://localhost:5173
   - Use the form to submit a job with:
     - Image: `ijm-runtime:dev`
     - Command: `python -u train.py`
   - Job should appear with status `QUEUED`, then `RUNNING`
   - Worker starts a Docker container for the job

2. **Stop the job**:
   - Click "Stop" button on the running job
   - Container receives SIGTERM and exits cleanly
   - Checkpoint file is created at `data/checkpoints/<job_id>/latest.pt`
   - Job status changes to `PREEMPTED`

3. **Resume the job**:
   - Click "Resume" button on the preempted job
   - Container starts again with same checkpoint mount
   - Runtime loads checkpoint and continues from previous step
   - Status becomes `RUNNING` again

4. **Verify checkpoint persistence**:
   - Check that training continues from the step where it was stopped
   - Look at console output to see step numbers resume correctly

## Architecture

### Services

- **Frontend (React)**: Job submission UI with polling-based updates
- **API (FastAPI)**: REST endpoints for job management, persists to Postgres, publishes to NATS
- **Worker (Python)**: Consumes NATS events, executes jobs in Docker containers (FIFO, single-concurrency)
- **Postgres**: Stores job metadata and state
- **NATS JetStream**: Event bus for job lifecycle events

### Data Flow

1. User submits job via React UI → POST `/jobs`
2. API creates job record in Postgres with status `QUEUED`
3. API publishes `jobs.submitted` event to NATS
4. Worker consumes event and enqueues job
5. Worker runs job in Docker container with checkpoint/log mounts
6. Job can be stopped (SIGTERM) or resumed (restart with same mounts)

### Job States

- `QUEUED` - Job submitted, waiting for execution
- `RUNNING` - Job currently executing
- `PREEMPTED` - Job was stopped by user request
- `SUCCEEDED` - Job completed successfully (exit code 0)
- `FAILED` - Job failed (non-zero exit code)

### Runtime Contract

Training containers must:
- Write checkpoints to `/checkpoints/latest.pt`
- Load checkpoint on startup if it exists
- Handle SIGTERM gracefully by checkpointing and exiting with code 0
- Periodically checkpoint during training

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
uv sync
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ijm \
NATS_URL=nats://localhost:4222 \
HOST_ROOT=.. \
HOST_PROJECT_ROOT=$(cd .. && pwd) \
uv run python worker.py
```

### Running tests

```bash
cd backend && uv run pytest          # Backend tests
cd worker  && uv run --with pytest --with psycopg --with "nats-py" pytest tests/  # Worker tests
cd frontend && pnpm lint && pnpm build   # Frontend lint + type-check
```

## API Endpoints

- `POST /jobs` - Submit new job
- `GET /jobs` - List all jobs (newest first)
- `GET /jobs/{id}` - Get specific job
- `POST /jobs/{id}/stop` - Stop a QUEUED or RUNNING job
- `POST /jobs/{id}/resume` - Resume a PREEMPTED or FAILED job
- `DELETE /jobs/{id}` - Delete a job
- `GET /jobs/{id}/logs` - Get container output logs
- `POST /images/upload` - Upload a Docker image (.tar/.tar.gz)

## NATS Subjects

- `jobs.submitted` - New job created (or resumed)
- `jobs.stop_requested` - User requested stop for a RUNNING job

## Project Structure

```
backend/          # FastAPI application
frontend/         # React UI
worker/           # Job execution worker
runtime/          # Training container with checkpoint support
infra/            # Docker Compose configuration
data/             # Local persistent data
  pg/             # Postgres data
  checkpoints/    # Job checkpoints
  runs/           # Job outputs
documentation/    # Architecture diagrams
```

## Tech Stack

**Backend**: Python 3.12, FastAPI, psycopg (Postgres), nats-py, uv
**Frontend**: TypeScript, React 19, Vite, TanStack Query/Table, Tailwind, shadcn/ui
**Infrastructure**: Docker, Postgres 16, NATS 2.10 with JetStream
**Worker**: Python 3.12, asyncio, Docker CLI, uv

## Non-Goals (v0)

- Authentication/authorization
- API gateway
- Kubernetes deployment
- Complex scheduling (only FIFO, single-concurrency)
- Multi-tenancy
- Resource quotas/limits

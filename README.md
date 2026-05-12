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
- **Optimizer** → http://localhost:8080
- **Postgres** → localhost:5432

The API runs jobs directly via its embedded `JobRunner` + `DockerExecutor` — no separate worker process needed for local dev. The GPUspb optimizer is started by default for deadline-aware, cost-optimised scheduling with cross-job preemption; to disable it, set `OPTIMIZER_URL=` (the API falls back to a greedy FIFO scheduler).

### 4. (Optional) Simulate multi-node locally

By default every node in [config/nodes_config.json](config/nodes_config.json) has `workerUrl: null`, so the API runs jobs in-process. To exercise the real HTTP dispatch path against a separate worker container:

1. In [config/nodes_config.json](config/nodes_config.json), set `"workerUrl": "http://worker:8001"` on one node.
2. Start with the `worker` profile:
   ```bash
   cd infra && docker compose --profile worker up --build
   ```

The fake worker (`ijm-worker`, `NODE_ID=local-worker`) runs containers via the host Docker socket. GPU presence is trusted from the config rather than probed, so you can declare any `resources` you like (e.g. `4× A40` on a laptop) to exercise scheduling/preemption end-to-end without real GPUs. Duplicate the `worker` service in [infra/docker-compose.yml](infra/docker-compose.yml) (different `container_name`, host port, `NODE_ID`) to fake additional nodes.

---

## Cluster Deployment (Polimi server)

The production setup splits responsibilities: **workers** run on each GPU node, the **API** runs anywhere (laptop, CI server) and connects via SSH tunnel.

### Primary node — postgres + worker

Run from your local machine:

```bash
NODE_ID=matemagician ./infra/deploy.sh polimi
```

`deploy.sh` rsyncs worker source + Dockerfile + compose file to `~/ijm/` on the server and starts:
- **postgres** (port 5433) — shared job state database
- **worker** (port 8001) — executes Docker training containers

### Additional GPU nodes (worker-only)

For each extra GPU node, deploy worker only — they connect to the primary node's postgres:

```bash
NODE_ID=polimi-gpu ./infra/deploy.sh --worker-only polimi-gpu
```

The `--worker-only` flag uses `docker-compose.worker.yml` which omits postgres and points `DATABASE_URL` at the primary node.

For nodes where `wangrat` doesn't have docker group access, see [docs on rootless Docker setup](#rootless-docker) below.

### Shared filesystem (for cross-node checkpoint resume)

So a job preempted on node-A can resume on node-B from its checkpoint, all nodes must share `~/ijm/data/checkpoints/`. We use rclone over SSH:

```bash
# On each non-primary node
sudo dnf install fuse-sshfs   # or apt install fuse3
~/.local/bin/rclone mount matemagician:/home/wangrat/ijm/data ~/ijm/data \
  --daemon --vfs-cache-mode writes --dir-cache-time 10s
```

NFS works too if you have admin access.

### Client side — run API + frontend locally against the cluster

```bash
# Terminal 1: open SSH tunnels (keep running)
./infra/tunnel.sh

# Terminal 2: start API + frontend (+ optimizer)
cd infra && docker compose -f docker-compose.tunnel.yml up --build
```

Opens:
- **Frontend** → http://localhost:5173
- **API** → http://localhost:8000
- **Optimizer** → localhost:8080

`tunnel.sh` forwards 3 ports through matemagician: `5433` (postgres), `8001` (matemagician worker), `8002` (polimi-gpu worker via internal LAN). Close Terminal 1 to disconnect.

### Building the runtime images on the server

The `runtime/` directory is not deployed to the server. Copy it over and build there. **Two image variants are required per training script**:

- `:latest` — built from [`runtime/Dockerfile`](runtime/Dockerfile) with PyTorch 2.6 + CUDA 12.4. Used on nodes with a modern NVIDIA driver (`555.x` or newer, supports CUDA 12.4+). E.g. polimi-gpu.
- `:legacy` — built from [`runtime/Dockerfile.legacy`](runtime/Dockerfile.legacy) with PyTorch 1.5.1 + CUDA 10.1. Used on nodes whose driver caps at CUDA 10.1 (`418.x`). E.g. matemagician.

```bash
rsync -av runtime/ polimi:~/ijm-runtime/
ssh polimi 'cd ~/ijm-runtime &&
  for s in lstm_small.py lstm_big.py convnet.py efficientnet.py; do
    tag=${s%.py}; tag=${tag//_/-}
    docker build --build-arg SCRIPT=$s -t wangrat/ijm-$tag:latest .
    docker build -f Dockerfile.legacy --build-arg SCRIPT=$s -t wangrat/ijm-$tag:legacy .
  done'
```

If the node can't reach Docker Hub (Polimi firewall), build the image locally and transfer:

```bash
docker save wangrat/ijm-lstm-small:latest | gzip | ssh polimi "gunzip | docker load"
```

To distribute the modern image from matemagician to polimi-gpu (which can't reach Docker Hub through its rootless network namespace), pipe over SSH:

```bash
ssh polimi 'docker save wangrat/ijm-lstm-small:latest | gzip' \
  | ssh polimi-gpu 'gunzip | docker load'
```

#### Pre-staged MNIST/CIFAR-10 cache

The legacy torchvision (0.6.1) bundled with `:legacy` looks up MNIST via its old `processed/{training,test}.pt` layout, so the shared `data/datasets/MNIST/` directory must contain *both* `raw/` and `processed/`. Preprocess once:

```bash
# On the primary node, materialise processed/ alongside raw/
ssh polimi 'cp -r ~/ijm/data/datasets/MNIST /tmp/mnist-rw &&
  docker run --rm -v /tmp/mnist-rw:/data wangrat/ijm-lstm-small:legacy \
    python -c "from torchvision import datasets;
               datasets.MNIST(\"/data\", train=True, download=True);
               datasets.MNIST(\"/data\", train=False, download=True)" &&
  cp -r /tmp/mnist-rw/MNIST/processed ~/ijm/data/datasets/MNIST/'
```

### GPU passthrough setup

The worker passes one of `--gpus N`, `--device nvidia.com/gpu=<i>`, or no GPU flag based on `WORKER_GPU_MODE` (set per-deploy in `deploy.sh` / `deploy_native.sh`):

| Mode | Flag emitted | When |
|---|---|---|
| `runtime` (default) | `--gpus N` | Rootful Docker with `nvidia` registered as the **default** runtime in `/etc/docker/daemon.json`. matemagician is configured this way. |
| `cdi` | `--device nvidia.com/gpu=0 … nvidia.com/gpu=N-1` | Rootless Docker, where the nvidia OCI hook can't write to `/sys/fs/cgroup/devices/devices.allow`. polimi-gpu is configured this way. |
| `none` | _(no flag)_ | Force CPU only. |

`IMAGE_TAG_OVERRIDE=legacy` rewrites the job's `wangrat/ijm-*:latest` image to `:legacy`. Set on matemagician.

#### Rootless Docker + CDI (polimi-gpu)

```bash
# 1. Generate a user-local CDI spec for the GPUs.  Run as the worker user.
ssh polimi-gpu 'nvidia-ctk cdi generate --output=$HOME/.config/cdi/nvidia.yaml'

# 2. Tell rootless dockerd where to find it and enable CDI.
ssh polimi-gpu 'mkdir -p ~/.config/docker && cat > ~/.config/docker/daemon.json <<EOF
{
    "features": {"cdi": true},
    "cdi-spec-dirs": ["/home/wangrat/.config/cdi"],
    "runtimes": {
        "nvidia": {"path": "nvidia-container-runtime", "args": []}
    }
}
EOF
systemctl --user restart docker'

# 3. Smoke-test
ssh polimi-gpu 'docker run --rm --device nvidia.com/gpu=0 nvidia/cuda:12.4.0-base-ubuntu22.04 nvidia-smi'
```

If your user isn't in the `docker` group on a node, install rootless Docker (no sudo needed beyond `loginctl enable-linger` for processes to survive logout):

```bash
dockerd-rootless-setuptool.sh install --skip-iptables
sudo loginctl enable-linger $USER     # asks admin
echo 'export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock' >> ~/.bashrc
```

Then deploy as usual — the worker compose detects `DOCKER_SOCK` env var to mount the rootless socket.

### Per-node deploy summary

```bash
# matemagician (rootful docker, nvidia default runtime, CUDA-10.1 driver)
NODE_ID=matemagician WORKER_GPU_MODE=runtime IMAGE_TAG_OVERRIDE=legacy ./infra/deploy.sh polimi

# polimi-gpu (rootless docker, CDI mode, CUDA-12.5 driver)
NODE_ID=polimi-gpu WORKER_GPU_MODE=cdi ./infra/deploy_native.sh polimi-gpu
```

---

## Architecture

```
User / Frontend
      │  REST
      ▼
Central API  (FastAPI — scheduler + state + dispatch)
      ├── POST /jobs/{id}/run          ← HTTP dispatch
      ├── POST /jobs/{id}/stop
      │       ▼
      │   Worker  (FastAPI — per GPU node, port 8001)
      │       ├── runs Docker training containers
      │       ├── streams logs + progress to DB
      │       └── on profiling done: writes duration to DB,
      │           sends NOTIFY ijm_schedule
      │
      └── POST /optimizer/v5           ← optional batch optimizer
          GPUspb Optimizer (port 8080)
              └── cost-aware scheduling with deadlines & priorities
```

**Local dev**: nodes without `workerUrl` in config use the embedded `JobRunner + DockerExecutor` — no worker server needed.

**Multi-node**: each GPU node runs a worker container; set `"workerUrl": "http://<host>:8001"` in nodes_config.

**Optimizer**: started by default. Provides batch-optimal scheduling with deadlines, priorities, and preemption via the [GPUspb](https://github.com/FFede0/GPUspb) C++ optimizer. Set `OPTIMIZER_URL=` (empty) to disable and fall back to a greedy per-job scheduler.

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
HOST_ROOT=$(cd .. && pwd) \
HOST_PROJECT_ROOT=$(cd .. && pwd) \
PYTHONPATH=$(cd .. && pwd) \
NODE_ID=local-worker \
uvicorn app:app --port 8001
```

The worker is split into modules: `app.py` (HTTP endpoints), `db.py`, `docker.py`, `execution.py`, `profiling.py`, `reconcile.py`.

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
optimizer/  GPUspb cost-aware batch optimizer (C++ core + Flask wrapper)
runtime/    Training container images (LSTM, ConvNet, EfficientNet)
infra/      Docker Compose configs + smoke test + tunnel.sh
config/     Cluster node configs (local, server, tunnel) + GPU energy costs
data/       Persistent data (pg/, checkpoints/, runs/)
```

## Tech Stack

**Backend**: Python 3.13, FastAPI, psycopg3, psycopg-pool, uv
**Frontend**: TypeScript, React 19, Vite, TanStack Query, Tailwind, shadcn/ui
**Worker**: Python 3.13, FastAPI, asyncio, Docker CLI
**Optimizer**: C++ (scheduling algorithms) + Python 3.8 (Flask REST wrapper)
**Infrastructure**: Docker, PostgreSQL 16

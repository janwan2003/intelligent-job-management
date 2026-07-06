# Intelligent Job Management System

A job management system for GPU deep learning clusters with profiling-based scheduling, stoppable/resumable jobs, and multi-node support. Modelled after the ANDREAS project (Polimi).

## Prerequisites

**To run the cluster** (the supported path, `bash infra/ijm up`):

- Docker & Docker Compose (v2 plugin)
- `ssh` client with key auth to the cluster
- Two host aliases in `~/.ssh/config`:

  ```
  Host polimi          HostName matemagician.deib.polimi.it  User wangrat
  Host polimi-gpu      HostName 10.79.23.173                  User wangrat
                       ProxyCommand ssh polimi -W %h:%p
  ```

  Replace `wangrat` if your cluster account differs. The scripts treat `polimi` as the SSH alias for the postgres + matemagician host, and `polimi-gpu` for the GPU node behind it.

> **Operator note.** The infra scripts default to the `wangrat` account; to deploy under a different one, set `IJM_REMOTE_USER` — the deploy path, container names, and compose environment all derive from it (see [Bootstrap & lifecycle](#bootstrap--lifecycle--bash-infraijm)). The only `wangrat` literals left are the Docker Hub image namespace (`wangrat/ijm-*`), which only matters if you build and push your own training images, and the SSH config example above.

**To work on backend / frontend source** (optional, dev only):

- Python 3.13+ with [uv](https://docs.astral.sh/uv/)
- Node.js 24+ with [pnpm](https://pnpm.io/)

## Cluster Deployment (Polimi server) — the supported path

The production setup splits responsibilities: **workers** run on each GPU node (matemagician + polimi-gpu); the **API**, optimizer, and frontend run on your machine, containerised, and reach the cluster via an SSH tunnel.

### Bootstrap & lifecycle — `bash infra/ijm`

After the SSH aliases are configured (see Prerequisites), one command brings the whole supported topology up. The scripts default to deploying under the `wangrat` account (`/home/wangrat/ijm`, containers `wangrat-ijm-*`); to deploy under a different account, set `IJM_REMOTE_USER` once — `REMOTE_DIR` and the container names all derive from it:

```bash
IJM_REMOTE_USER=alice bash infra/ijm deploy   # deploys to /home/alice/ijm, containers alice-ijm-*
```

```bash
bash infra/ijm deploy    # one-time / when worker code changed: rsync, force-recreate
                         # container on matemagician, apply DB schema, start native
                         # uvicorn worker on polimi-gpu
bash infra/ijm up        # daily: open tunnel (if down), bring up API + optimizer + frontend
bash infra/ijm status    # check tunnel / workers / local stack / API / per-node slots
bash infra/ijm down      # stop local docker + close tunnel (add --workers to also stop remote workers)
bash infra/ijm logs      # tail ijm-api docker logs
```

After `bash infra/ijm up`:
- **Frontend** → http://localhost:5173
- **API** → http://localhost:8000  (containerised; `network_mode: host` so it reaches the tunnel ports at `localhost:5433/8001/8002`)
- **Optimizer** → http://localhost:8080

### Shared filesystem (for cross-node checkpoint resume)

So a job preempted on node-A can resume on node-B from its checkpoint, all nodes must share `~/ijm/data/checkpoints/`. We use rclone over SSH:

```bash
# On each non-primary node
sudo dnf install fuse-sshfs   # or apt install fuse3
~/.local/bin/rclone mount matemagician:/home/wangrat/ijm/data ~/ijm/data \
  --daemon --vfs-cache-mode writes --dir-cache-time 10s
```

NFS works too if you have admin access.

### End-to-end scenarios

```bash
bash infra/e2e_scenario.sh         # default: cnn_big single-type, prio-staggered patients + URGENT migration
bash infra/e2e_scenario_2types.sh  # cnn_big PINs + lstm-small patients + one URGENT cnn_big
```

Both clear DB state at Stage 0 and run the full submission → profile-sweep → URGENT preempt → cross-node resume → drain pipeline. Override defaults via env: `JOB_TYPE=lstm-small bash infra/e2e_scenario.sh`, `EPOCHS_BIG=80 EPOCHS_SMALL=400 bash infra/e2e_scenario_2types.sh`, etc. Current script defaults: scenario 1 uses `JOB_TYPE=cnn_big`; scenario 2 uses `EPOCHS_BIG=40` for the URGENT cnn_big and `EPOCHS_SMALL=200` for the patient lstm-smalls.

### Advanced: manual per-node deploy

`bash infra/ijm deploy` is the recommended path. The raw scripts it wraps are kept for the rare case where you want to redeploy only one node:

```bash
# matemagician (rootful docker, nvidia default runtime, CUDA-10.1 driver — needs the legacy image)
NODE_ID=matemagician WORKER_GPU_MODE=runtime IMAGE_TAG_OVERRIDE=legacy NODE_TOTAL_GPUS=2 \
    ./infra/deploy.sh polimi

# polimi-gpu (rootless docker, CDI mode, CUDA-12.5 driver — native uvicorn, not docker-compose)
NODE_ID=polimi-gpu WORKER_GPU_MODE=cdi NODE_TOTAL_GPUS=2 \
    ./infra/deploy_native.sh polimi-gpu
```

Note that the *raw* `deploy.sh` does not run `docker-compose up --force-recreate`, so if the container already exists with stale env vars (e.g. wrong `NODE_ID`) it will silently leave the old one in place. `bash infra/ijm deploy` adds the force-recreate plus a schema-apply step and handles both nodes in one shot.

### Building the runtime images on the server

The `runtime/` directory is not deployed to the server. Copy it over and build there. **Two image variants are required per training script**:

- `:latest` — built from [`runtime/Dockerfile`](runtime/Dockerfile) with PyTorch 2.6 + CUDA 12.4. Used on nodes with a modern NVIDIA driver (`555.x` or newer, supports CUDA 12.4+). E.g. polimi-gpu.
- `:legacy` — built from [`runtime/Dockerfile.legacy`](runtime/Dockerfile.legacy) with PyTorch 1.5.1 + CUDA 10.1. Used on nodes whose driver caps at CUDA 10.1 (`418.x`). E.g. matemagician.

```bash
rsync -av runtime/ polimi:~/ijm-runtime/
ssh polimi 'cd ~/ijm-runtime &&
  for s in lstm_small.py lstm_big.py convnet.py efficientnet.py cnn_big.py; do
    tag=${s%.py}; tag=${tag//_/-}
    docker build --build-arg SCRIPT=$s -t wangrat/ijm-$tag:latest .
    docker build -f Dockerfile.legacy --build-arg SCRIPT=$s -t wangrat/ijm-$tag:legacy .
  done'
```

Note: the image-name convention uses dashes (`lstm-small`, `cnn-big`) but the python sources keep underscores (`lstm_small.py`, `cnn_big.py`). The scenarios submit with the **tag** in image+job_id, e.g.\ `wangrat/ijm-cnn_big:latest` — that's a tag that intentionally keeps the underscore (the worker / API don't care which separator the type uses; they just propagate it verbatim).

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

---

## Local development (no cluster)

For UI-only or scheduler-logic-only work without a real GPU cluster. **This is a dev convenience, not the production topology** — the cluster path above is what gets exercised by the e2e scenarios and what should be used in practice. Useful for fast iteration on the frontend or backend code paths that don't depend on GPU passthrough.

### 1. Build the runtime images

```bash
docker build -t ijm-lstm-small:dev --build-arg SCRIPT=lstm_small.py runtime/
docker build -t ijm-lstm-big:dev   --build-arg SCRIPT=lstm_big.py   runtime/
docker build -t ijm-convnet:dev    --build-arg SCRIPT=convnet.py    runtime/
docker build -t ijm-efficientnet:dev --build-arg SCRIPT=efficientnet.py runtime/
docker build -t ijm-cnn_big:dev    --build-arg SCRIPT=cnn_big.py    runtime/   # heavy CNN, 2-GPU-beneficial
```

`cnn_big` is a deep CNN (10 conv blocks, channels grow up to 512, 128×128×3 synthetic-tensor input, batch 32) deliberately sized so per-step compute amortises the DataParallel sync overhead on both GPU classes — see `tab:e2e-2gpu` in `documentation/report` for the measured per-bundle epoch times (P600×2 is **1.68× faster** than P600×1, A40×2 is 1.12× faster than A40×1).

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

`docker compose up` starts a local **worker container** (`ijm-worker`, `NODE_ID=local-worker`) alongside the API; jobs are dispatched to it over HTTP exactly as in a distributed deployment — there is no separate in-process execution path. The default [config/nodes_config.local_worker.json](config/nodes_config.local_worker.json) has one node (`local-worker`, `A40×2`) whose `workerUrl` points at that container. The worker is CPU-only by default (`WORKER_GPU_MODE=none`) so it launches on a box without an NVIDIA runtime; GPU presence is trusted from the config rather than probed, so you can declare any `resources` you like to exercise scheduling/preemption end-to-end without real GPUs. The GPUspb optimizer is started by default; to disable it set `OPTIMIZER_URL=` (the API falls back to a greedy FIFO scheduler).

### 4. (Optional) Simulate a second node locally

To exercise multi-node scheduling/preemption, start a second worker and point the API at the 2-node config:

```bash
cd infra && NODES_CONFIG=config/nodes_config.local_2workers.json \
  docker compose --profile worker2 up --build
```

This adds `ijm-worker2` (`NODE_ID=local-worker2`, `QuadroP600×2`); [config/nodes_config.local_2workers.json](config/nodes_config.local_2workers.json) declares both nodes.

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

**Local dev**: `docker compose up` runs a single worker container (`local-worker`) on the same box; the API dispatches to it over HTTP — the same path as a real deployment, just CPU-only.

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
| `ijm-cnn_big:dev`    | `cnn_big.py`    | 10-block deep CNN ($3{\to}{\dots}{\to}512$) | synthetic 128×128×3 |

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
runtime/    Training container images (LSTM, ConvNet, EfficientNet, cnn_big)
infra/      ijm orchestrator + deploy.sh / deploy_native.sh / tunnel.sh +
            docker-compose{,.server,.tunnel,.worker}.yml + smoke_test.sh +
            e2e_scenario*.sh + snapshot_run.sh + generate_chart{,_tex}.py
config/     Cluster node configs (local, server, tunnel) + GPU energy costs
data/       Persistent data (pg/, checkpoints/, runs/)
```

## Tech Stack

**Backend**: Python 3.13, FastAPI, psycopg3, psycopg-pool, uv
**Frontend**: TypeScript, React 19, Vite, TanStack Query, Tailwind, shadcn/ui
**Worker**: Python 3.13, FastAPI, asyncio, Docker CLI
**Optimizer**: C++ (scheduling algorithms) + Python 3.8 (Flask REST wrapper)
**Infrastructure**: Docker, PostgreSQL 16

---

## Environment Variables

| Variable | Used by | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | API, worker | `postgresql://postgres:postgres@postgres:5432/ijm` | In tunnel mode the host is `localhost:5433`. |
| `HOST_ROOT` | API | `/host` | Maps to repo root inside the API container. |
| `HOST_PROJECT_ROOT` | API | `${PWD}/..` | Host-resolvable path used for Docker bind mounts. On the cluster the `infra/` scripts derive it from `IJM_REMOTE_USER`. |
| `IJM_REMOTE_USER` | `infra/` scripts | `wangrat` | Account the remote deploy dir (`/home/$IJM_REMOTE_USER/ijm`) and the postgres/worker container names (`$IJM_REMOTE_USER-ijm-*`) are namespaced under. Set once to deploy under a different account — `REMOTE_DIR` and the container names all derive from it. |
| `OPTIMIZER_URL` | API | `http://optimizer:8080` | Set to empty string to fall back to greedy FIFO scheduling. |
| `OPTIMIZER_VERBOSE` | API | unset | Set to `1` for verbose optimizer-client diagnostic logs. |
| `IJM_DRIFT_HEARTBEAT_S` | API | `15` | Period of the slot-tracker drift heartbeat (reconciles `mem_used` vs `db_used` per node). |
| `WORKER_GPU_MODE` | worker | `runtime` | `runtime` (rootful + nvidia default runtime), `cdi` (rootless via CDI spec), or `none` (CPU-only — used by the all-in-one compose). |
| `NODE_TOTAL_GPUS` | worker | (probes `nvidia-smi`) | Declares the node's GPU count without probing; set on GPU-less hosts (the all-in-one worker uses `2`). |
| `IMAGE_TAG_OVERRIDE` | worker | unset | Rewrites the job's image tag (`:latest` → `:$IMAGE_TAG_OVERRIDE`). Set to `legacy` on matemagician. |
| `NODE_ID` | worker | from deploy script | Identifies the node in `config/nodes_config.json`. |
| `VITE_API_URL` | frontend | `http://localhost:8000` | API base URL the SPA talks to. |

---

## Documentation

- [documentation/SLOT_INVARIANTS.md](documentation/SLOT_INVARIANTS.md) — invariants maintained by the slot tracker.
- [documentation/e2e-scenarios.md](documentation/e2e-scenarios.md) — what each `infra/e2e_scenario*.sh` exercises.
- [documentation/andreas.md](documentation/andreas.md) — notes on the upstream ANDREAS design.
- [documentation/report/](documentation/report/) — the project's thesis report (LaTeX + PDF).
- [documentation/external/](documentation/external/) — third-party reference PDFs (ANDREAS deliverables, GPUspb paper, Polimi server notes).

---

## License

[MIT](LICENSE).

#!/usr/bin/env bash
# Deploy the IJM worker (+ postgres on matemagician) to a GPU node.
# Usage:
#   ./infra/deploy.sh [HOST]                       # full deploy (default: matemagician)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${1:-polimi}"
# Remote deploy identity — the account whose name prefixes the containers and
# deploy path on the shared host.  REMOTE_DIR and the compose container names
# both derive from it; override with IJM_REMOTE_USER=alice for another account.
IJM_REMOTE_USER="${IJM_REMOTE_USER:-wangrat}"
REMOTE_DIR="${REMOTE_DIR:-/home/$IJM_REMOTE_USER/ijm}"
NODE_ID="${NODE_ID:-$HOST}"
DOCKER_CMD="${DOCKER_CMD:-docker-compose}"
# GPU passthrough mode for ``docker run`` from the worker (one of:
# ``runtime`` = ``--gpus N`` via nvidia default runtime;
# ``cdi`` = ``--device nvidia.com/gpu=<n>`` via CDI for rootless docker;
# ``none`` = CPU only).  Required for the container to see GPUs.
WORKER_GPU_MODE="${WORKER_GPU_MODE:-runtime}"
# Image tag rewrite (``:latest`` → ``:$IMAGE_TAG_OVERRIDE``).  Used on
# matemagician where the legacy CUDA-10.1 image is needed.
IMAGE_TAG_OVERRIDE="${IMAGE_TAG_OVERRIDE:-}"
# Physical GPU count on the node — used by the worker's per-launch GPU
# allocator.  Required because the matemagician worker runs in a non-GPU
# container, so ``nvidia-smi -L`` from inside it reports 0.
NODE_TOTAL_GPUS="${NODE_TOTAL_GPUS:-0}"
SOCKET="/tmp/ijm-deploy-$$"

echo "==> Deploying full stack to $HOST:$REMOTE_DIR (NODE_ID=$NODE_ID)"

ssh -fNM -S "$SOCKET" "$HOST"
trap 'ssh -S "$SOCKET" -O exit "$HOST" 2>/dev/null; rm -f "$SOCKET"' EXIT
SSH="ssh -S $SOCKET"
RSYNC="rsync -av -e 'ssh -S $SOCKET'"

# Clean remote dir (keep data/ intact)
$SSH "$HOST" "find $REMOTE_DIR -mindepth 1 -maxdepth 1 ! -name data -exec rm -rf {} + && mkdir -p $REMOTE_DIR/shared $REMOTE_DIR/data/pg $REMOTE_DIR/data/checkpoints $REMOTE_DIR/data/runs"

# Sync all worker modules (the worker was split into multiple files)
WORKER_FILES=(app.py constants.py db.py docker.py execution.py profiling.py reconcile.py)
for f in "${WORKER_FILES[@]}"; do
  eval "$RSYNC '$REPO_ROOT/worker/$f' '$HOST:$REMOTE_DIR/'"
done
eval "$RSYNC '$REPO_ROOT/worker/Dockerfile.server' '$HOST:$REMOTE_DIR/Dockerfile'"
eval "$RSYNC '$REPO_ROOT/shared/constants.py' '$HOST:$REMOTE_DIR/shared/'"
eval "$RSYNC '$REPO_ROOT/shared/profiling_sql.py' '$HOST:$REMOTE_DIR/shared/'"
eval "$RSYNC '$REPO_ROOT/infra/docker-compose.server.yml' '$HOST:$REMOTE_DIR/docker-compose.yml'"
$SSH "$HOST" "printf 'data\n' > $REMOTE_DIR/.dockerignore"

echo "==> Starting services"
$SSH "$HOST" "cd $REMOTE_DIR && IJM_REMOTE_USER=$IJM_REMOTE_USER HOST_PROJECT_ROOT=$REMOTE_DIR NODE_ID=$NODE_ID WORKER_GPU_MODE=$WORKER_GPU_MODE IMAGE_TAG_OVERRIDE=$IMAGE_TAG_OVERRIDE NODE_TOTAL_GPUS=$NODE_TOTAL_GPUS $DOCKER_CMD up --build -d"

echo "==> Done."

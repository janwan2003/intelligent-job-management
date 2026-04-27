#!/usr/bin/env bash
# Deploy the IJM worker (+ postgres on matemagician) to a GPU node.
# Usage:
#   ./infra/deploy.sh                              # full deploy to matemagician
#   ./infra/deploy.sh --worker-only polimi-gpu     # worker-only deploy
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKER_ONLY=0
if [ "${1:-}" = "--worker-only" ]; then
  WORKER_ONLY=1; shift
fi
HOST="${1:-polimi}"
REMOTE_DIR="${REMOTE_DIR:-/home/wangrat/ijm}"
NODE_ID="${NODE_ID:-$HOST}"
DOCKER_CMD="${DOCKER_CMD:-docker-compose}"
SOCKET="/tmp/ijm-deploy-$$"

echo "==> Deploying $([ "$WORKER_ONLY" = 1 ] && echo "worker-only" || echo "full stack") to $HOST:$REMOTE_DIR (NODE_ID=$NODE_ID)"

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
if [ "$WORKER_ONLY" = 1 ]; then
  eval "$RSYNC '$REPO_ROOT/infra/docker-compose.worker.yml' '$HOST:$REMOTE_DIR/docker-compose.yml'"
else
  eval "$RSYNC '$REPO_ROOT/infra/docker-compose.server.yml' '$HOST:$REMOTE_DIR/docker-compose.yml'"
fi
$SSH "$HOST" "printf 'data\n' > $REMOTE_DIR/.dockerignore"

echo "==> Starting services"
$SSH "$HOST" "cd $REMOTE_DIR && NODE_ID=$NODE_ID $DOCKER_CMD up --build -d"

echo "==> Done."

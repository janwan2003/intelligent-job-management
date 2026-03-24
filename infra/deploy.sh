#!/usr/bin/env bash
# Deploy the IJM worker + postgres to the Polimi GPU server.
# Rsyncs only the worker source, shared module, server Dockerfile, and compose file.
# Usage: ./infra/deploy.sh [ssh-host]   (default: polimi)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${1:-polimi}"
REMOTE_DIR="/home/wangrat/ijm"
SOCKET="/tmp/ijm-deploy-$$"

echo "==> Deploying to $HOST:$REMOTE_DIR"

# Open a single multiplexed SSH connection; all subsequent ssh/rsync reuse it
ssh -fNM -S "$SOCKET" "$HOST"
trap 'ssh -S "$SOCKET" -O exit "$HOST" 2>/dev/null; rm -f "$SOCKET"' EXIT
SSH="ssh -S $SOCKET"
RSYNC="rsync -av -e 'ssh -S $SOCKET'"

# Clean remote dir (keep data/ intact), then recreate needed subdirs
$SSH "$HOST" "find $REMOTE_DIR -mindepth 1 -maxdepth 1 ! -name data -exec rm -rf {} + && mkdir -p $REMOTE_DIR/shared $REMOTE_DIR/data/pg $REMOTE_DIR/data/checkpoints $REMOTE_DIR/data/runs"

# Sync files over the same connection
eval "$RSYNC '$REPO_ROOT/worker/app.py' '$REPO_ROOT/worker/constants.py' '$HOST:$REMOTE_DIR/'"
eval "$RSYNC '$REPO_ROOT/worker/Dockerfile.server' '$HOST:$REMOTE_DIR/Dockerfile'"
eval "$RSYNC '$REPO_ROOT/shared/constants.py' '$HOST:$REMOTE_DIR/shared/'"
eval "$RSYNC '$REPO_ROOT/infra/docker-compose.server.yml' '$HOST:$REMOTE_DIR/docker-compose.yml'"
$SSH "$HOST" "printf 'data\n' > $REMOTE_DIR/.dockerignore"

echo "==> Starting services (postgres + worker)"
$SSH "$HOST" "cd $REMOTE_DIR && docker-compose up --build -d"

echo "==> Done. Postgres at $HOST:5433, worker at $HOST:8001"

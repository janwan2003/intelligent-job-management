#!/usr/bin/env bash
# Deploy the IJM worker + postgres to the Polimi GPU server.
# Rsyncs only the worker source, shared module, server Dockerfile, and compose file.
# Usage: ./infra/deploy.sh [ssh-host]   (default: polimi)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${1:-polimi}"
REMOTE_DIR="/home/wangrat/ijm"

echo "==> Deploying to $HOST:$REMOTE_DIR"

# Ensure remote directories exist
ssh "$HOST" "mkdir -p $REMOTE_DIR/shared $REMOTE_DIR/data/pg $REMOTE_DIR/data/checkpoints $REMOTE_DIR/data/runs"

# Sync worker source and server Dockerfile
rsync -av "$REPO_ROOT/worker/app.py" "$REPO_ROOT/worker/constants.py" "$HOST:$REMOTE_DIR/"
rsync -av "$REPO_ROOT/worker/Dockerfile.server" "$HOST:$REMOTE_DIR/Dockerfile"

# Sync shared module
rsync -av "$REPO_ROOT/shared/constants.py" "$HOST:$REMOTE_DIR/shared/"

# Sync server compose file (becomes docker-compose.yml at deploy root)
rsync -av "$REPO_ROOT/infra/docker-compose.server.yml" "$HOST:$REMOTE_DIR/docker-compose.yml"

echo "==> Starting services (postgres + worker)"
ssh "$HOST" "cd $REMOTE_DIR && docker-compose up --build -d"

echo "==> Done. Postgres at $HOST:5433, worker at $HOST:8001"

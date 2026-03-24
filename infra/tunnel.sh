#!/usr/bin/env bash
# Open SSH tunnels to the Polimi server so the API can run locally
# against the remote postgres and worker.
#
# Usage: ./infra/tunnel.sh [ssh-host]
#   ssh-host defaults to "polimi" (alias in ~/.ssh/config)
#
# Forwards:
#   localhost:5433 → server postgres (5433)
#   localhost:8001 → server worker   (8001)
#
# Once the tunnel is open, start the API with:
#   cd backend && DATABASE_URL=postgresql://postgres:postgres@localhost:5433/ijm \
#     NODES_CONFIG=config/nodes_config.tunnel.json \
#     HOST_PROJECT_ROOT=/home/wangrat/ijm \
#     uv run uvicorn src.app:app --port 8000

set -euo pipefail
HOST="${1:-polimi}"
echo "Opening tunnels to $HOST (ctrl-c to close)..."
exec ssh -N \
  -L 5433:localhost:5433 \
  -L 8001:localhost:8001 \
  "$HOST"

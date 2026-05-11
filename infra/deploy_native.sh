#!/usr/bin/env bash
# Deploy the IJM worker as a NATIVE process (not docker-compose) on a node.
#
# Use this on rootless-docker hosts where:
#   - ``network_mode: host`` doesn't actually expose host ports
#     (slirp4netns isolates the container's net namespace), AND
#   - bridge networking can't reach the postgres on matemagician:5433
#     (slirp4netns NAT is rejected by some firewalls).
#
# polimi-gpu falls into both buckets.  The native worker runs directly on
# the host's network stack — port 8001 binds correctly, outbound to
# matemagician works exactly as it would from any host process.
#
# Usage:
#   ./infra/deploy_native.sh polimi-gpu
#
# Requires: SSH alias ``$1`` reachable, python3.11+ on remote, /home/wangrat/ijm
# pre-existing.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${1:-polimi-gpu}"
REMOTE_DIR="${REMOTE_DIR:-/home/wangrat/ijm}"
NODE_ID="${NODE_ID:-$HOST}"
DB_URL="${DB_URL:-postgresql://postgres:postgres@matemagician.deib.polimi.it:5433/ijm}"
SOCKET="/tmp/ijm-native-deploy-$$"

echo "==> Native deploy of worker to $HOST:$REMOTE_DIR (NODE_ID=$NODE_ID)"

ssh -fNM -S "$SOCKET" "$HOST"
trap 'ssh -S "$SOCKET" -O exit "$HOST" 2>/dev/null; rm -f "$SOCKET"' EXIT
SSH="ssh -S $SOCKET"
RSYNC="rsync -av -e 'ssh -S $SOCKET'"

# Sync source
WORKER_FILES=(app.py constants.py db.py docker.py execution.py profiling.py reconcile.py)
for f in "${WORKER_FILES[@]}"; do
  eval "$RSYNC '$REPO_ROOT/worker/$f' '$HOST:$REMOTE_DIR/'"
done
eval "$RSYNC '$REPO_ROOT/shared/constants.py' '$HOST:$REMOTE_DIR/shared/'"

# Ensure venv exists with required deps
$SSH "$HOST" "cd $REMOTE_DIR && \
    if [ ! -d .venv ]; then python3 -m venv .venv; fi && \
    .venv/bin/pip install --quiet --upgrade pip && \
    .venv/bin/pip install --quiet fastapi 'uvicorn[standard]' 'psycopg[binary]'"

# Stop any prior native worker
# Kill whatever's holding port 8001.  We avoid ``pkill -f`` because any
# pattern that matches the worker is also a literal substring of the
# wrapping SSH shell's argv — so pkill self-kills its own session (SSH
# exits 255 and the start step never runs).  ``ss``-based PID extraction
# sidesteps the self-match entirely.
$SSH "$HOST" "pid=\$(ss -tlnpH 2>/dev/null | awk '\$4 ~ /:8001\$/ {print}' | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2); [ -n \"\$pid\" ] && kill \"\$pid\" 2>/dev/null; sleep 2; ! ss -tlnp 2>/dev/null | grep -q ':8001 '"

# Start fresh.  setsid + nohup so the process survives SSH disconnect.  Logs
# to /tmp/ijm-worker.log on the remote — tail there for diagnostics.
$SSH "$HOST" "cd $REMOTE_DIR && \
    DATABASE_URL='$DB_URL' \
    HOST_ROOT=$REMOTE_DIR \
    HOST_PROJECT_ROOT=$REMOTE_DIR \
    NODE_ID=$NODE_ID \
    setsid nohup .venv/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8001 \
        > /tmp/ijm-worker.log 2>&1 < /dev/null & disown
    sleep 3
    ss -tlnp 2>/dev/null | grep ':8001 ' || (echo 'FAILED to bind 8001'; tail /tmp/ijm-worker.log; exit 1)"

echo "==> Native worker running on $HOST. Tail /tmp/ijm-worker.log on the host for logs."

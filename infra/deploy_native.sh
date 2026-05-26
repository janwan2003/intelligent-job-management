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
# GPU access mode for ``docker run``:
#   - runtime: ``--gpus N`` (nvidia is the default runtime)
#   - cdi:     ``--device nvidia.com/gpu=0 ...`` (rootless docker)
#   - none:    no GPU flags
# polimi-gpu runs rootless docker, which requires CDI mode for GPU
# passthrough (NVIDIA_VISIBLE_DEVICES is silently ignored under rootless,
# causing trainer containers to fall back to CPU at 2-3× slower than the
# expected GPU performance).  Default to ``cdi`` when targeting that host;
# anywhere else default to ``runtime``.  Override via env var if you have
# a non-standard setup.
if [[ -z "${WORKER_GPU_MODE:-}" ]]; then
    case "${1:-polimi-gpu}" in
        polimi-gpu) WORKER_GPU_MODE=cdi ;;
        *)          WORKER_GPU_MODE=runtime ;;
    esac
fi
# Image tag override: ``:latest`` → ``:$IMAGE_TAG_OVERRIDE`` (per-node).
# Used on matemagician where the legacy CUDA-10.1 image is needed.
IMAGE_TAG_OVERRIDE="${IMAGE_TAG_OVERRIDE:-}"
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
eval "$RSYNC '$REPO_ROOT/shared/profiling_sql.py' '$HOST:$REMOTE_DIR/shared/'"

# Ensure venv exists with required deps and Python >= 3.11 (StrEnum from
# shared/constants.py requires it).  Pick the newest python3.1x available;
# if the existing venv is older, rebuild.
$SSH "$HOST" "cd $REMOTE_DIR && \
    PY=\$(for v in python3.13 python3.12 python3.11; do command -v \$v && break; done | head -1); \
    if [ -z \"\$PY\" ]; then echo 'ERROR: need python3.11+ on remote'; exit 1; fi; \
    if [ -d .venv ]; then \
        VENV_VER=\$(.venv/bin/python -c 'import sys; print(sys.version_info[:2] >= (3,11))' 2>/dev/null); \
        if [ \"\$VENV_VER\" != 'True' ]; then rm -rf .venv; fi; \
    fi; \
    if [ ! -d .venv ]; then \$PY -m venv .venv; fi && \
    .venv/bin/pip install --quiet --upgrade pip && \
    .venv/bin/pip install --quiet fastapi 'uvicorn[standard]' 'psycopg[binary]'"

# Stop any prior native worker
# Kill whatever's holding port 8001.  We avoid ``pkill -f`` because any
# pattern that matches the worker is also a literal substring of the
# wrapping SSH shell's argv — so pkill self-kills its own session (SSH
# exits 255 and the start step never runs).  ``ss``-based PID extraction
# sidesteps the self-match entirely.
$SSH "$HOST" "pid=\$(ss -tlnpH 2>/dev/null | awk '\$4 ~ /:8001\$/ {print}' | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2); [ -n \"\$pid\" ] && kill \"\$pid\" 2>/dev/null; sleep 2; ! ss -tlnp 2>/dev/null | grep -q ':8001 '"

# Start fresh.  The ``(cmd &)`` subshell-fork pattern is the only reliable
# way to background a long-running process over SSH multiplex: even with
# ``setsid nohup ... & disown`` and full FD redirection, SSH otherwise
# keeps the session open as long as the bash that backgrounded the child
# is alive (the bash itself waits in an opaque way, leaving SSH hung).
# A subshell that forks the child and then exits lets SSH return.
$SSH "$HOST" "cd $REMOTE_DIR && (
    DATABASE_URL='$DB_URL' \
    HOST_ROOT=$REMOTE_DIR \
    HOST_PROJECT_ROOT=$REMOTE_DIR \
    NODE_ID=$NODE_ID \
    WORKER_GPU_MODE='$WORKER_GPU_MODE' \
    IMAGE_TAG_OVERRIDE='$IMAGE_TAG_OVERRIDE' \
    nohup .venv/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8001 \
        > /tmp/ijm-worker.log 2>&1 < /dev/null &
)"
# Poll for bind in a separate SSH call so the start-step can return cleanly.
$SSH "$HOST" "for i in \$(seq 1 30); do
        ss -tlnp 2>/dev/null | grep -q ':8001 ' && break
        sleep 1
    done
    ss -tlnp 2>/dev/null | grep ':8001 ' >/dev/null || (echo 'FAILED to bind 8001 after 30s'; tail /tmp/ijm-worker.log; exit 1)"

echo "==> Native worker running on $HOST. Tail /tmp/ijm-worker.log on the host for logs."

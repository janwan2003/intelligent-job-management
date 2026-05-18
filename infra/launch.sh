#!/usr/bin/env bash
# IJM full-stack launcher.  Single entry point for both supported topologies:
#
#   bash infra/launch.sh local    # everything in docker (mock cluster)
#   bash infra/launch.sh tunnel   # API native on host + tunneled remote cluster
#
# After ``tunnel`` mode, you can ``bash infra/e2e_scenario.sh`` or
# ``bash infra/e2e_scenario_2types.sh`` and they will work without any
# environment juggling.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-tunnel}"

stop_native_api() {
    # Kill any uvicorn we started in a previous run.  Idempotent.
    pkill -f "uvicorn src.main:app" 2>/dev/null || true
    sleep 1
}

case "$MODE" in
    local)
        # All-in-docker: API uses local postgres + fake nodes_config.json.
        # Useful for UI/dev work without an actual GPU cluster.
        stop_native_api
        cd "$REPO_ROOT/infra"
        docker compose up -d
        echo
        echo "==> Local mode up.  Frontend: http://localhost:5173  API: http://localhost:8000"
        ;;

    tunnel)
        # Real cluster reachable through SSH tunnels.  Verifies the tunnels,
        # starts postgres + optimizer + frontend in docker, then launches the
        # API natively on the host so it can reach localhost:5433/8001/8002.
        for port in 5433 8001 8002; do
            if ! ss -tln 2>/dev/null | awk '{print $4}' | grep -qE "(:|^)$port\$"; then
                echo "ERR: SSH tunnel for port $port is not listening." >&2
                echo "    Expected: ssh -N -L 0.0.0.0:5433:localhost:5433 \\" >&2
                echo "                  -L 0.0.0.0:8001:localhost:8001 \\" >&2
                echo "                  -L 0.0.0.0:8002:10.79.23.173:8001 polimi" >&2
                exit 1
            fi
        done

        # Bring up the docker-side services (postgres is local-only and
        # unused in tunnel mode, but the optimizer + frontend live here).
        stop_native_api
        cd "$REPO_ROOT/infra"
        docker compose up -d --no-deps optimizer frontend
        # Stop any previously-running dockerized API — it would crash-loop
        # against the native API on port 8000.
        docker compose stop api 2>/dev/null || true

        # Launch the native API with the tunnel-aware env.
        cd "$REPO_ROOT/backend"
        export DATABASE_URL="postgresql://postgres:postgres@localhost:5433/ijm"
        export PYTHONPATH="$REPO_ROOT"
        export NODES_CONFIG="$REPO_ROOT/config/nodes_config.tunnel.json"
        export OPTIMIZER_URL="http://localhost:8080"
        export OPTIMIZER_VERBOSE="2"
        nohup /home/janek/.local/bin/uv run --no-sync uvicorn src.main:app \
            --host 0.0.0.0 --port 8000 > /tmp/api.log 2>&1 &
        sleep 5

        # Confirm.
        if curl -sf -m 3 http://localhost:8000/health >/dev/null; then
            echo
            echo "==> Tunnel mode up.  API: http://localhost:8000  Frontend: http://localhost:5173"
            echo "    Logs: /tmp/api.log"
            curl -s http://localhost:8000/admin/slots | python3 -c "
import json, sys
d = json.load(sys.stdin)
for n,v in d['per_node'].items():
    print(f'    {n}: {v[\"available\"]}/{v[\"total\"]} GPUs available')"
        else
            echo "ERR: API failed to start.  Tail of /tmp/api.log:" >&2
            tail -20 /tmp/api.log >&2
            exit 1
        fi
        ;;

    *)
        echo "usage: $0 {local|tunnel}" >&2
        exit 1
        ;;
esac

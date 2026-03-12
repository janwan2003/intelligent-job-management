#!/usr/bin/env bash
# Smoke test: verifies the full stack starts healthy and the API responds.
# Usage: cd infra && ./smoke_test.sh
set -euo pipefail

COMPOSE="docker compose"
API_URL="http://localhost:8000/health"
MAX_WAIT=60  # seconds

cd "$(dirname "$0")"

echo "==> Building and starting services..."
$COMPOSE up --build -d

echo "==> Waiting for API to become healthy (max ${MAX_WAIT}s)..."
elapsed=0
until curl -sf "$API_URL" > /dev/null 2>&1; do
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        echo "FAIL: API did not become healthy within ${MAX_WAIT}s"
        $COMPOSE logs --tail=30
        $COMPOSE down
        exit 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

echo "==> API is healthy. Checking response..."
RESPONSE=$(curl -sf "$API_URL")
STATUS=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin)['status'])")
if [ "$STATUS" != "healthy" ]; then
    echo "FAIL: unexpected status '$STATUS' (expected 'healthy')"
    $COMPOSE down
    exit 1
fi

echo "==> Checking all containers are running..."
UNHEALTHY=$($COMPOSE ps --format json | python3 -c "
import sys, json
services = [json.loads(l) for l in sys.stdin if l.strip()]
bad = [s['Service'] for s in services if s.get('State') != 'running']
print(' '.join(bad))
" 2>/dev/null || echo "")
if [ -n "$UNHEALTHY" ]; then
    echo "FAIL: services not running: $UNHEALTHY"
    $COMPOSE logs --tail=30
    $COMPOSE down
    exit 1
fi

echo "==> All checks passed. Tearing down..."
$COMPOSE down
echo "PASS"

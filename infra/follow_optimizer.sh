#!/usr/bin/env bash
# Live-follow the optimizer's detailed per-call log.
#
# The ijm-optimizer container writes a NEW log file per scheduling call to
# /opt/code-optimizer/build/logs/log_<ts>.log.  ``docker logs`` only shows
# the terse "Reading/Writing results in" wrapper output, not the per-step
# objective + tardiness + cost trace we actually want.
#
# This script polls every 1 s for the most-recent .log inside the container
# and ``tail -F``-tails it.  When a new call file appears it transparently
# switches to that file, printing a separator banner so you can tell where
# one optimizer round ended and the next started.
#
# Usage:
#   ./infra/follow_optimizer.sh           # live follow
#   ./infra/follow_optimizer.sh | grep -E "ASSIGNED|tardiness|best cost"
#
# Container name overridable via OPTIMIZER_CONTAINER (default ijm-optimizer).
set -euo pipefail

CONTAINER="${OPTIMIZER_CONTAINER:-ijm-optimizer}"

exec docker exec -i "$CONTAINER" bash -c '
CUR=""
trap "kill 0" EXIT INT TERM
while true; do
  NEW=$(ls -1t /opt/code-optimizer/build/logs/*.log 2>/dev/null | head -1)
  if [ -n "$NEW" ] && [ "$NEW" != "$CUR" ]; then
    [ -n "$CUR" ] && kill %1 2>/dev/null || true
    printf "\n===== %s =====\n" "$(basename "$NEW")"
    tail -n +1 -F "$NEW" &
    CUR="$NEW"
  fi
  sleep 1
done'

#!/usr/bin/env bash
# Snapshot a finished run's logs into /tmp/runs/latest/.
# Always overwrites the previous snapshot — there is only ever one canonical
# "latest" directory.  The run's start timestamp is embedded in the chart.
# Usage:  bash infra/snapshot_run.sh
set -euo pipefail
dest="/tmp/runs/latest"
rm -rf "$dest"
mkdir -p "$dest"
cp -f /tmp/api.log "$dest/api.log" 2>/dev/null || echo "warn: no api.log"
cp -f /tmp/scenario.log "$dest/scenario.log" 2>/dev/null || echo "warn: no scenario.log"
curl -sS http://localhost:8000/jobs                       > "$dest/jobs.json"      || true
curl -sS http://localhost:8000/profiling-results/cnn_big  > "$dest/profiles.json"  || true
ssh polimi 'docker logs wangrat-ijm-worker 2>&1 | tail -3000' > "$dest/worker_matemagician.log" 2>/dev/null || true
ssh polimi-gpu 'cat /tmp/ijm-worker.log' > "$dest/worker_polimi-gpu.log" 2>/dev/null || true
# Per-job trainer logs (for epoch progress)
mkdir -p "$dest/jobs"
for full in $(jq -r '.[].id' "$dest/jobs.json" 2>/dev/null); do
    curl -sS "http://localhost:8000/jobs/$full/logs" > "$dest/jobs/${full:0:8}.log" 2>/dev/null || true
done
echo "Snapshot saved to $dest"
ls -lh "$dest"

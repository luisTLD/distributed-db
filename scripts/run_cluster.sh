#!/usr/bin/env bash
# Start a local 3-node cluster (Linux / macOS), each node in the background.
# Logs go to logs/nodeN.log. Stop everything with: scripts/stop_cluster.sh
set -e
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p logs data

for id in 1 2 3; do
  echo "starting node $id -> logs/node$id.log"
  python -m distdb.node --id "$id" --data-dir "data" > "logs/node$id.log" 2>&1 &
  echo $! >> logs/cluster.pids
  sleep 0.5
done

echo "cluster up. tail logs with: tail -f logs/node*.log"
echo "open a client with:        python -m distdb.client"

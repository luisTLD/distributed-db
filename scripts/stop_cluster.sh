#!/usr/bin/env bash
# Encerra o cluster local (mata os PIDs em logs/cluster.pids).
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [ -f logs/cluster.pids ]; then
  while read -r pid; do
    kill "$pid" 2>/dev/null && echo "stopped pid $pid"
  done < logs/cluster.pids
  rm -f logs/cluster.pids
else
  echo "no logs/cluster.pids file; nothing to stop"
fi

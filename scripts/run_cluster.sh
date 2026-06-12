#!/usr/bin/env bash
# Sobe um cluster local de 3 nós (Linux/macOS), cada nó em segundo plano —
# evita abrir 3 terminais na mão durante o desenvolvimento.
# Logs em logs/nodeN.log. Para encerrar: scripts/stop_cluster.sh
set -e
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p logs data

for id in 1 2 3; do
  echo "starting node $id -> logs/node$id.log"
  python -m distdb.node --id "$id" --data-dir "data" > "logs/node$id.log" 2>&1 &
  echo $! >> logs/cluster.pids       # guarda o PID para o stop_cluster.sh
  sleep 0.5
done

echo "cluster up. tail logs with: tail -f logs/node*.log"
echo "open a client with:        python -m distdb.client"

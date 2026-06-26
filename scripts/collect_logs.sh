#!/usr/bin/env bash
# =============================================================================
#  collect_logs.sh: roda TUDO de uma vez e junta os logs em logs/coleta/
#
#  Faz, em sequência, a partir da raiz do projeto (com o venv ativo):
#    1. testes automatizados (sem rede)        -> 01_tests.txt
#    2. sobe o cluster de 3 nós                 -> logs/nodeN.log
#    3. demo do cliente (2PC, abort, ...)       -> 02_client_demo.txt
#    4. benchmark distribuído x máquina única   -> 03_benchmark.txt
#    5. teste de estresse + consistência        -> 04_stress.txt
#    6. copia os logs dos nós                    -> 05_node*.log
#    7. encerra o cluster
#
#  Uso:  bash scripts/collect_logs.sh
#        OPS=300 CLIENTS=16 CLIENT_OPS=200 bash scripts/collect_logs.sh
# =============================================================================
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OPS="${OPS:-200}"
CLIENTS="${CLIENTS:-8}"
CLIENT_OPS="${CLIENT_OPS:-100}"

OUT="logs/coleta"
# zera a base para uma coleta limpa e reproduzível
rm -rf logs data
mkdir -p logs data "$OUT"
export PYTHONUNBUFFERED=1
PY="${PYTHON:-python}"

echo "=============================================================="
echo " 1/6  Testes automatizados (sem rede)"
echo "=============================================================="
"$PY" tests/run_all.py 2>&1 | tee "$OUT/01_tests.txt"

echo
echo "=============================================================="
echo " 2/6  Subindo o cluster de 3 nós"
echo "=============================================================="
PIDS=()
for id in 1 2 3; do
  "$PY" -m distdb.node --id "$id" --data-dir data > "logs/node$id.log" 2>&1 &
  PIDS+=($!)
  echo "  node $id -> pid ${PIDS[-1]} (logs/node$id.log)"
  sleep 0.5
done
echo "  aguardando a eleicao inicial (~4s)..."
sleep 4

echo
echo "=============================================================="
echo " 3/6  Demo do cliente"
echo "=============================================================="
"$PY" -m distdb.client --demo 2>&1 | grep -v http_proxy_mapper | tee "$OUT/02_client_demo.txt"

echo
echo "=============================================================="
echo " 4/6  Benchmark (distribuido x maquina unica), ops=$OPS"
echo "=============================================================="
"$PY" scripts/benchmark.py --ops "$OPS" 2>&1 | grep -v http_proxy_mapper | tee "$OUT/03_benchmark.txt"

echo
echo "=============================================================="
echo " 5/6  Estresse: $CLIENTS clientes x $CLIENT_OPS ops + consistencia"
echo "=============================================================="
"$PY" scripts/stress_test.py --clients "$CLIENTS" --ops "$CLIENT_OPS" 2>&1 \
  | grep -v http_proxy_mapper | tee "$OUT/04_stress.txt"

echo
echo "=============================================================="
echo " 6/6  Copiando os logs dos nos e encerrando o cluster"
echo "=============================================================="
for id in 1 2 3; do
  grep -v http_proxy_mapper "logs/node$id.log" > "$OUT/05_node$id.log" 2>/dev/null
done
for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null; done
echo "  cluster encerrado."
echo
echo "Pronto. Todos os logs estao em: $OUT/"
ls -1 "$OUT/"

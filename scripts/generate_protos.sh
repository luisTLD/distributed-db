#!/usr/bin/env bash
# Atalho para o scripts/generate_protos.py (gera os stubs gRPC do .proto).
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python "$DIR/scripts/generate_protos.py"

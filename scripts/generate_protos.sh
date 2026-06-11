#!/usr/bin/env bash
# Convenience wrapper around scripts/generate_protos.py
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python "$DIR/scripts/generate_protos.py"

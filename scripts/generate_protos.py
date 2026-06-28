#!/usr/bin/env python3
"""Gera os stubs gRPC a partir de proto/database.proto."""

import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROTO_DIR = os.path.join(ROOT, "proto")
OUT_DIR = os.path.join(ROOT, "distdb", "generated")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    # Invoca o compilador protoc (vem com o pacote grpcio-tools).
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        os.path.join(PROTO_DIR, "database.proto"),
    ]
    print("running:", " ".join(cmd))
    subprocess.check_call(cmd)

    # Troca "import database_pb2" por um import relativo ao pacote.
    grpc_file = os.path.join(OUT_DIR, "database_pb2_grpc.py")
    with open(grpc_file, encoding="utf-8") as fh:
        src = fh.read()
    src = re.sub(r"^import database_pb2 as",
                 "from distdb.generated import database_pb2 as",
                 src, flags=re.MULTILINE)
    with open(grpc_file, "w", encoding="utf-8") as fh:
        fh.write(src)

    # Garante o marcador de pacote.
    init = os.path.join(OUT_DIR, "__init__.py")
    if not os.path.exists(init):
        open(init, "w").close()

    print("generated:")
    print("  distdb/generated/database_pb2.py")
    print("  distdb/generated/database_pb2_grpc.py")


if __name__ == "__main__":
    main()

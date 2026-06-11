#!/usr/bin/env python3
"""Compile proto/database.proto into distdb/generated/.

Generates ``database_pb2.py`` and ``database_pb2_grpc.py`` and rewrites the
absolute import inside the *_grpc file into a package-relative one so the code
works when imported as ``distdb.generated``.

Usage:  python scripts/generate_protos.py
"""

import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROTO_DIR = os.path.join(ROOT, "proto")
OUT_DIR = os.path.join(ROOT, "distdb", "generated")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        os.path.join(PROTO_DIR, "database.proto"),
    ]
    print("running:", " ".join(cmd))
    subprocess.check_call(cmd)

    # Fix the generated grpc file's import to be package-relative.
    grpc_file = os.path.join(OUT_DIR, "database_pb2_grpc.py")
    with open(grpc_file, encoding="utf-8") as fh:
        src = fh.read()
    src = re.sub(r"^import database_pb2 as",
                 "from distdb.generated import database_pb2 as",
                 src, flags=re.MULTILINE)
    with open(grpc_file, "w", encoding="utf-8") as fh:
        fh.write(src)

    # Ensure the package marker exists.
    init = os.path.join(OUT_DIR, "__init__.py")
    if not os.path.exists(init):
        open(init, "w").close()

    print("generated:")
    print("  distdb/generated/database_pb2.py")
    print("  distdb/generated/database_pb2_grpc.py")


if __name__ == "__main__":
    main()

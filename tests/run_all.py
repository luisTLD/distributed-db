#!/usr/bin/env python3
"""Executa todos os testes desta pasta (sem precisar de pytest).

Um único comando para validar toda a lógica distribuída — não precisa de
rede nem do cluster no ar:

    python tests/run_all.py
"""
import glob
import os
import runpy
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

def main():
    failures = 0
    # Roda cada arquivo test_*.py como se fosse chamado direto.
    for path in sorted(glob.glob(os.path.join(HERE, "test_*.py"))):
        name = os.path.basename(path)
        try:
            runpy.run_path(path, run_name="__main__")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL {name}: {exc}")
            failures += 1
    print("-" * 40)
    print("ALL TESTS PASSED" if failures == 0 else f"{failures} TEST FILE(S) FAILED")
    sys.exit(1 if failures else 0)

if __name__ == "__main__":
    main()

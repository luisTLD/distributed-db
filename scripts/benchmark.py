#!/usr/bin/env python3
"""Benchmark: compara o cluster distribuído com um armazém local."""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb import config
from distdb.store import KeyValueStore, PUT, UPDATE


def _fmt(label, n, seconds):
    # Imprime uma linha de resultado e devolve a vazão (ops/s).
    ops_s = n / seconds if seconds > 0 else float("inf")
    print(f"  {label:<28} {n} ops in {seconds:7.3f}s "
          f"-> {ops_s:9.1f} ops/s, {seconds / n * 1000:7.3f} ms/op")
    return ops_s


def baseline(n):
    # Cenário 2: store local em memória, sem rede e sem 2PC.
    print(f"\n[single machine] {n} writes + {n} reads on one local store (no network, no 2PC)")
    store = KeyValueStore()  # puramente em memória
    t0 = time.perf_counter()
    for i in range(n):
        tx = f"t{i}"
        store.prepare(tx, PUT, f"k{i}", str(i), i)
        store.commit(tx, i)
    w = time.perf_counter() - t0
    t0 = time.perf_counter()
    for i in range(n):
        store.get(f"k{i}")
    r = time.perf_counter() - t0
    wo = _fmt("local writes", n, w)
    ro = _fmt("local reads", n, r)
    return wo, ro


def distributed(n, nodes):
    # Cenário 1: cluster real via gRPC (import local: só este caminho usa rede).
    import grpc  # noqa: F401
    from distdb.client import ClusterClient
    print(f"\n[distributed] {n} writes + {n} reads via leader (2PC across replicas)")
    client = ClusterClient(nodes)
    print("  " + client.who_is_leader())
    t0 = time.perf_counter()
    for i in range(n):
        client.put(f"k{i}", str(i))
    w = time.perf_counter() - t0
    t0 = time.perf_counter()
    for i in range(n):
        client.get(f"k{i}")
    r = time.perf_counter() - t0
    wo = _fmt("distributed writes", n, w)
    ro = _fmt("distributed reads", n, r)
    return wo, ro


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ops", type=int, default=300)
    ap.add_argument("--peers", type=str, default=None)
    ap.add_argument("--baseline-only", action="store_true")
    args = ap.parse_args()
    nodes = config.parse_peers(args.peers) if args.peers else config.DEFAULT_CLUSTER

    print("=" * 64)
    print(f"Performance comparison  (ops = {args.ops})")
    print("=" * 64)

    b_w, b_r = baseline(args.ops)
    if args.baseline_only:
        return

    try:
        d_w, d_r = distributed(args.ops, nodes)
    except Exception as exc:
        print(f"\n[distributed] skipped: could not reach the cluster ({exc})")
        print("Start the cluster first (scripts/run_cluster.*) or use --baseline-only.")
        return

    # Resumo: quantas vezes o distribuído é mais lento que o local.
    print("\n" + "-" * 64)
    print("Replication / network cost (single machine is the fast baseline):")
    if d_w:
        print(f"  writes: distributed is {b_w / d_w:5.1f}x slower than single machine")
    if d_r:
        print(f"  reads:  distributed is {b_r / d_r:5.1f}x slower than single machine")
    print("Writes pay the 2PC + replication cost; reads are served locally by any node.")


if __name__ == "__main__":
    main()

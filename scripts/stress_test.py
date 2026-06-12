#!/usr/bin/env python3
"""Teste de estresse (alta concorrência) + verificação de consistência.

Responde "como o sistema se comporta com MUITAS requisições simultâneas?" e
serve de palco para demonstrar a tolerância a falhas sob carga: dá para
derrubar um nó (ou o líder) no meio do teste e ver o sistema se recuperar.

Dispara N threads-cliente ao mesmo tempo com carga mista:
  * escritas em poucas chaves QUENTES disputadas por todos -> estressa a
    exclusão mútua (serialização no líder + locks por chave no 2PC);
  * escritas em chaves únicas por thread -> paralelismo sem conflito;
  * leituras (atendidas por qualquer nó).

Imprime a vazão por segundo durante a execução (a queda na re-eleição fica
visível) e, ao final:
  1. vazão total, latências p50/p95/p99 e contagem committed/aborted/failed;
  2. CONSISTÊNCIA: lê o estado completo de CADA nó vivo e confere que todos
     têm exatamente os mesmos dados.

Uso (cluster já no ar):
    python scripts/stress_test.py                          # 8 clientes x 100 ops
    python scripts/stress_test.py --clients 16 --ops 200
    python scripts/stress_test.py --hot-ratio 0.5          # mais disputa
    python scripts/stress_test.py --peers "1=192.168.0.10:50051,..."
"""

import argparse
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb import config
from distdb.client import ClusterClient

# Chaves "quentes": todas as threads disputam estas mesmas chaves.
HOT_KEYS = ["hot_a", "hot_b", "hot_c", "hot_d"]


class Stats:
    """Acumulador thread-safe dos resultados das operações."""

    def __init__(self):
        self.lock = threading.Lock()
        self.write_lat = []          # latências (s) das escritas com sucesso
        self.read_lat = []
        self.committed = 0
        self.aborted = 0
        self.errors = 0
        self.reads_ok = 0
        self.reads_err = 0

    def record_write(self, msg: str, dt: float):
        # Classifica pela mensagem devolvida pelo servidor.
        with self.lock:
            if "committed" in msg:
                self.committed += 1
                self.write_lat.append(dt)
            elif "aborted" in msg:
                self.aborted += 1
            else:
                self.errors += 1

    def record_read(self, ok: bool, dt: float):
        with self.lock:
            if ok:
                self.reads_ok += 1
                self.read_lat.append(dt)
            else:
                self.reads_err += 1

    def done(self):
        with self.lock:
            return self.committed + self.aborted + self.errors \
                + self.reads_ok + self.reads_err


def percentile(sorted_vals, p):
    # Percentil simples sobre uma lista já ordenada.
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round((p / 100.0) * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def worker(tid, nodes, ops, hot_ratio, read_ratio, stats, barrier):
    # Cada thread é um cliente gRPC independente.
    client = ClusterClient(nodes)
    rng = random.Random(1000 + tid)
    barrier.wait()                         # todas começam juntas
    for i in range(ops):
        if rng.random() < read_ratio:
            # Operação de LEITURA.
            key = rng.choice(HOT_KEYS) if rng.random() < hot_ratio \
                else f"k_{tid}_{rng.randrange(max(1, i + 1))}"
            t0 = time.perf_counter()
            try:
                client.get(key)
                stats.record_read(True, time.perf_counter() - t0)
            except RuntimeError:
                stats.record_read(False, 0.0)
        else:
            # Operação de ESCRITA: chave quente (disputa) ou única (paralela).
            if rng.random() < hot_ratio:
                key = rng.choice(HOT_KEYS)
            else:
                key = f"k_{tid}_{i}"
            t0 = time.perf_counter()
            msg = client.put(key, f"v{tid}_{i}")
            # Se a escrita caiu na janela de uma eleição, repete algumas
            # vezes: nenhuma requisição se perde, ela só espera o novo
            # líder. O tempo de espera aparece na latência p99.
            attempts = 1
            while ("committed" not in msg and "aborted" not in msg
                   and attempts < 4):
                time.sleep(1.0)
                msg = client.put(key, f"v{tid}_{i}")
                attempts += 1
            stats.record_write(msg, time.perf_counter() - t0)


def progress_monitor(stats, total, stop_evt):
    """Imprime ops/s a cada segundo (falhas injetadas no meio ficam visíveis)."""
    last = 0
    while not stop_evt.wait(1.0):
        cur = stats.done()
        print(f"  ... {cur}/{total} ops ({cur - last} ops in the last second)",
              flush=True)
        last = cur
        if cur >= total:
            return


def consistency_check(nodes):
    """Baixa o estado completo de cada nó e compara todos entre si."""
    print("\n[consistency check] reading the full state of every node:")
    states, unreachable = {}, []
    for n in nodes:
        try:
            c = ClusterClient([n])             # cliente preso a UM nó
            states[n.node_id] = dict(c.get_all())
            print(f"  node {n.node_id} ({n.address}): {len(states[n.node_id])} keys")
        except RuntimeError:
            unreachable.append(n.node_id)
            print(f"  node {n.node_id} ({n.address}): UNREACHABLE (skipped)")

    ids = sorted(states)
    if len(ids) < 2:
        print("  fewer than two reachable nodes -- nothing to compare.")
        return True

    # Compara todo mundo contra o primeiro nó alcançável.
    ref_id, ref = ids[0], states[ids[0]]
    consistent = True
    for nid in ids[1:]:
        if states[nid] == ref:
            print(f"  node {nid} == node {ref_id}  OK")
        else:
            consistent = False
            missing = set(ref) - set(states[nid])
            extra = set(states[nid]) - set(ref)
            diff = {k for k in set(ref) & set(states[nid]) if ref[k] != states[nid][k]}
            print(f"  node {nid} != node {ref_id}  INCONSISTENT "
                  f"(missing={len(missing)} extra={len(extra)} different={len(diff)})")
    if consistent:
        print("  RESULT: all reachable nodes hold IDENTICAL data.")
    else:
        print("  RESULT: INCONSISTENCY detected (see above).")
    if unreachable:
        print(f"  note: nodes {unreachable} were down; after restarting they "
              f"sync from the leader (SyncState) and converge.")
    return consistent


def main():
    ap = argparse.ArgumentParser(description="Concurrent stress test")
    ap.add_argument("--clients", type=int, default=8,
                    help="threads-cliente simultâneas (padrão 8)")
    ap.add_argument("--ops", type=int, default=100,
                    help="operações por cliente (padrão 100)")
    ap.add_argument("--hot-ratio", type=float, default=0.3,
                    help="fração de escritas nas chaves disputadas (padrão 0.3)")
    ap.add_argument("--read-ratio", type=float, default=0.3,
                    help="fração de operações que são leituras (padrão 0.3)")
    ap.add_argument("--peers", type=str, default=None)
    args = ap.parse_args()

    nodes = config.parse_peers(args.peers) if args.peers else config.DEFAULT_CLUSTER
    total = args.clients * args.ops

    print("=" * 66)
    print(f"STRESS TEST: {args.clients} concurrent clients x {args.ops} ops "
          f"= {total} ops")
    print(f"  hot-key write ratio: {args.hot_ratio:.0%}   read ratio: {args.read_ratio:.0%}")
    print("  TIP: kill a replica (or the leader!) while this runs to watch")
    print("       the failure handling under load.")
    print("=" * 66)

    # Sobe as threads-cliente; a barreira garante o início simultâneo.
    stats = Stats()
    barrier = threading.Barrier(args.clients + 1)
    threads = [threading.Thread(target=worker,
                                args=(t, nodes, args.ops, args.hot_ratio,
                                      args.read_ratio, stats, barrier),
                                daemon=True)
               for t in range(args.clients)]
    for t in threads:
        t.start()

    stop_evt = threading.Event()
    mon = threading.Thread(target=progress_monitor, args=(stats, total, stop_evt),
                           daemon=True)
    mon.start()

    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0
    stop_evt.set()

    # ----------------------------- relatório final -----------------------
    writes = stats.committed + stats.aborted + stats.errors
    wl = sorted(stats.write_lat)
    rl = sorted(stats.read_lat)

    print("\n" + "=" * 66)
    print(f"RESULTS  ({total} ops in {elapsed:.2f}s -> {total / elapsed:.1f} ops/s aggregate)")
    print("-" * 66)
    print(f"  writes: {writes}  (committed={stats.committed}  "
          f"aborted={stats.aborted}  failed={stats.errors})")
    if wl:
        print(f"    latency  p50={percentile(wl, 50) * 1000:7.1f} ms   "
              f"p95={percentile(wl, 95) * 1000:7.1f} ms   "
              f"p99={percentile(wl, 99) * 1000:7.1f} ms")
    print(f"  reads:  {stats.reads_ok + stats.reads_err}  "
          f"(ok={stats.reads_ok}  failed={stats.reads_err})")
    if rl:
        print(f"    latency  p50={percentile(rl, 50) * 1000:7.1f} ms   "
              f"p95={percentile(rl, 95) * 1000:7.1f} ms   "
              f"p99={percentile(rl, 99) * 1000:7.1f} ms")

    ok = consistency_check(nodes)
    print("\n" + ("STRESS TEST PASSED" if ok and stats.errors == 0 else
                  "STRESS TEST FINISHED WITH ISSUES (see report above)"))


if __name__ == "__main__":
    main()

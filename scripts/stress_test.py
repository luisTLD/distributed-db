#!/usr/bin/env python3
"""High-concurrency stress test + cluster-wide consistency check.

Answers the question "how does the system behave under many simultaneous
requests?". It launches N client threads that hammer the cluster at the same
time with a mixed workload:

* writes to a small set of HOT keys  -> heavy contention on the same keys,
  exercising the mutual-exclusion path (leader-side serialization + per-key
  locks in the 2PC prepare);
* writes to per-thread unique keys   -> parallel, conflict-free load;
* reads (served by any node).

While it runs you can KILL a replica or even the LEADER to watch the
fault-tolerance behaviour under load: throughput dips for a few seconds
(failure detection + election / cohort shrink) and then recovers, without
inconsistency. A per-second progress line makes the dip visible.

At the end the script:
1. prints throughput, latency percentiles (p50/p95/p99) and the breakdown of
   committed / aborted / failed operations;
2. queries EVERY reachable node individually and checks that they hold exactly
   the same data (replication consistency check).

Usage (cluster already running):
    python scripts/stress_test.py                          # 8 clients x 100 ops
    python scripts/stress_test.py --clients 16 --ops 200
    python scripts/stress_test.py --hot-ratio 0.5          # more contention
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

HOT_KEYS = ["hot_a", "hot_b", "hot_c", "hot_d"]


class Stats:
    """Thread-safe accumulator for operation results."""

    def __init__(self):
        self.lock = threading.Lock()
        self.write_lat = []          # seconds, successful writes only
        self.read_lat = []
        self.committed = 0
        self.aborted = 0
        self.errors = 0
        self.reads_ok = 0
        self.reads_err = 0

    def record_write(self, msg: str, dt: float):
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
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round((p / 100.0) * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def worker(tid, nodes, ops, hot_ratio, read_ratio, stats, barrier):
    client = ClusterClient(nodes)          # each thread = an independent client
    rng = random.Random(1000 + tid)
    barrier.wait()                         # all threads start at the same time
    for i in range(ops):
        if rng.random() < read_ratio:
            key = rng.choice(HOT_KEYS) if rng.random() < hot_ratio \
                else f"k_{tid}_{rng.randrange(max(1, i + 1))}"
            t0 = time.perf_counter()
            try:
                client.get(key)
                stats.record_read(True, time.perf_counter() - t0)
            except RuntimeError:
                stats.record_read(False, 0.0)
        else:
            if rng.random() < hot_ratio:
                key = rng.choice(HOT_KEYS)     # contended key
            else:
                key = f"k_{tid}_{i}"           # unique key (no conflicts)
            t0 = time.perf_counter()
            msg = client.put(key, f"v{tid}_{i}")
            # If the write failed (e.g. it fell inside a leader-election
            # window), retry a few times: no request is lost, it just waits
            # for the new leader. The retry time shows up in the p99 latency.
            attempts = 1
            while ("committed" not in msg and "aborted" not in msg
                   and attempts < 4):
                time.sleep(1.0)
                msg = client.put(key, f"v{tid}_{i}")
                attempts += 1
            stats.record_write(msg, time.perf_counter() - t0)


def progress_monitor(stats, total, stop_evt):
    """Print ops/s once per second so failures injected mid-run are visible."""
    last = 0
    while not stop_evt.wait(1.0):
        cur = stats.done()
        print(f"  ... {cur}/{total} ops ({cur - last} ops in the last second)",
              flush=True)
        last = cur
        if cur >= total:
            return


def consistency_check(nodes):
    """Fetch the full dataset from every node and compare them."""
    print("\n[consistency check] reading the full state of every node:")
    states, unreachable = {}, []
    for n in nodes:
        try:
            c = ClusterClient([n])             # client pinned to a single node
            states[n.node_id] = dict(c.get_all())
            print(f"  node {n.node_id} ({n.address}): {len(states[n.node_id])} keys")
        except RuntimeError:
            unreachable.append(n.node_id)
            print(f"  node {n.node_id} ({n.address}): UNREACHABLE (skipped)")

    ids = sorted(states)
    if len(ids) < 2:
        print("  fewer than two reachable nodes -- nothing to compare.")
        return True

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
                    help="number of simultaneous client threads (default 8)")
    ap.add_argument("--ops", type=int, default=100,
                    help="operations per client (default 100)")
    ap.add_argument("--hot-ratio", type=float, default=0.3,
                    help="fraction of writes aimed at a few contended keys (default 0.3)")
    ap.add_argument("--read-ratio", type=float, default=0.3,
                    help="fraction of operations that are reads (default 0.3)")
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

"""Simulated-cluster integration test (no gRPC needed).

Models a 3-node cluster in-process to exercise the same orchestration that
``distdb.node`` performs over gRPC: leader-coordinated 2PC, replica failure,
leader failure + Bully election, and state-transfer recovery. This validates the
end-to-end fault-tolerance behaviour using the real core modules.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.lamport import LamportClock
from distdb.store import KeyValueStore, PUT
from distdb.coordinator import TwoPhaseCommit, COMMITTED
from distdb.election import run_election


class SimNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.store = KeyValueStore()
        self.clock = LamportClock()
        self.alive = True

    # 2PC participant interface (raises when "down", like a dead gRPC peer)
    def prepare(self, *a):
        if not self.alive:
            raise ConnectionError(f"node {self.node_id} down")
        return self.store.prepare(*a)

    def commit(self, *a):
        if not self.alive:
            raise ConnectionError(f"node {self.node_id} down")
        return self.store.commit(*a)

    def abort(self, *a):
        if not self.alive:
            raise ConnectionError(f"node {self.node_id} down")
        return self.store.abort(*a)


class SimCluster:
    def __init__(self, ids):
        self.nodes = {i: SimNode(i) for i in ids}
        self.ids = sorted(ids)
        self.leader = max(self.ids)  # highest id is leader

    def live_ids(self):
        return [i for i in self.ids if self.nodes[i].alive]

    def write(self, key, value):
        """Leader coordinates 2PC over the live cohort, retrying past failures."""
        leader = self.nodes[self.leader]
        tpc = TwoPhaseCommit(leader.clock)
        excluded = set()
        for _ in range(len(self.ids)):
            cohort = [self.nodes[i] for i in self.live_ids() if i not in excluded]
            res = tpc.execute(cohort, PUT, key, value)
            if res.status == COMMITTED:
                return res
            if not res.failed_nodes:
                return res
            excluded.update(res.failed_nodes)
        return res

    def kill(self, node_id):
        self.nodes[node_id].alive = False

    def elect(self):
        """Run Bully from each live node; the highest live id becomes leader."""
        for nid in sorted(self.live_ids()):
            def send(peer, _self=nid):
                return self.nodes[peer].alive
            became, _ = run_election(nid, self.ids, send)
            if became:
                self.leader = nid
        return self.leader

    def sync(self, node_id):
        """Recovered node pulls the leader's committed state (state transfer)."""
        leader_state = self.nodes[self.leader].store.snapshot_dict()
        self.nodes[node_id].store.replace_all(leader_state)
        self.nodes[node_id].alive = True


def test_full_fault_tolerance_flow():
    c = SimCluster([1, 2, 3])
    assert c.leader == 3

    # 1) Normal write replicates atomically to all three nodes.
    res = c.write("a", "1")
    assert res.status == COMMITTED
    for i in (1, 2, 3):
        assert c.nodes[i].store.get("a") == (True, "1")

    # 2) A replica dies; writes still commit on the surviving cohort.
    c.kill(2)
    res = c.write("b", "2")
    assert res.status == COMMITTED
    assert c.nodes[1].store.get("b") == (True, "2")
    assert c.nodes[3].store.get("b") == (True, "2")
    assert c.nodes[2].store.exists("b") is False  # node 2 missed it (it's down)

    # 3) The leader dies; survivors elect the highest live id (node 1).
    c.kill(3)
    new_leader = c.elect()
    assert new_leader == 1
    res = c.write("c", "3")
    assert res.status == COMMITTED
    assert c.nodes[1].store.get("c") == (True, "3")

    # 4) Node 2 recovers and syncs state from the new leader.
    c.sync(2)
    assert c.nodes[2].store.get("a") == (True, "1")
    assert c.nodes[2].store.get("b") == (True, "2")
    assert c.nodes[2].store.get("c") == (True, "3")


if __name__ == "__main__":
    test_full_fault_tolerance_flow()
    print("integration: OK")

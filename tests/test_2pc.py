import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.lamport import LamportClock
from distdb.store import KeyValueStore, PUT, UPDATE
from distdb.coordinator import (TwoPhaseCommit, COMMITTED, ABORTED,
                                termination_decision)


class StoreParticipant:
    """Adapts a KeyValueStore to the coordinator's Participant interface."""

    def __init__(self, node_id, store):
        self.node_id = node_id
        self.store = store

    def prepare(self, tx_id, op, key, value, timestamp):
        return self.store.prepare(tx_id, op, key, value, timestamp)

    def commit(self, tx_id, timestamp):
        return self.store.commit(tx_id, timestamp)

    def abort(self, tx_id, timestamp):
        return self.store.abort(tx_id, timestamp)


class FlakyParticipant(StoreParticipant):
    """A participant whose prepare/commit raise to simulate a crash."""

    def __init__(self, node_id, store, fail_on="prepare"):
        super().__init__(node_id, store)
        self.fail_on = fail_on

    def prepare(self, *a, **k):
        if self.fail_on == "prepare":
            raise ConnectionError("node down")
        return super().prepare(*a, **k)


def make_cluster(n=3):
    stores = [KeyValueStore() for _ in range(n)]
    parts = [StoreParticipant(i + 1, s) for i, s in enumerate(stores)]
    return stores, parts


def test_commit_replicates_to_all():
    stores, parts = make_cluster(3)
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(parts, PUT, "a", "1")
    assert res.status == COMMITTED
    assert res.yes_votes == 3 and res.cohort_size == 3
    for s in stores:
        assert s.get("a") == (True, "1")   # atomic: visible on every node


def test_abort_when_one_votes_no():
    stores, parts = make_cluster(3)
    tpc = TwoPhaseCommit(LamportClock())
    # UPDATE on a missing key -> every participant votes NO
    res = tpc.execute(parts, UPDATE, "missing", "x")
    assert res.status == ABORTED
    for s in stores:
        assert s.exists("missing") is False


def test_participant_failure_aborts_and_is_reported():
    s_ok1, s_ok2 = KeyValueStore(), KeyValueStore()
    parts = [
        StoreParticipant(1, s_ok1),
        FlakyParticipant(2, KeyValueStore(), fail_on="prepare"),
        StoreParticipant(3, s_ok2),
    ]
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(parts, PUT, "a", "1")
    assert res.status == ABORTED
    assert 2 in res.failed_nodes
    # the healthy nodes did NOT expose the value (atomicity preserved)
    assert s_ok1.exists("a") is False
    assert s_ok2.exists("a") is False


def test_retry_after_excluding_failed_node_succeeds():
    """Mirrors what the node layer does: drop the dead replica, retry, commit."""
    s1, s3 = KeyValueStore(), KeyValueStore()
    live = [StoreParticipant(1, s1), StoreParticipant(3, s3)]  # node 2 excluded
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(live, PUT, "a", "1")
    assert res.status == COMMITTED
    assert s1.get("a") == (True, "1") and s3.get("a") == (True, "1")


def test_no_quorum_aborts_without_touching_stores():
    """With 2 of 3 nodes down, the lone leader must refuse the write."""
    s1 = KeyValueStore()
    alone = [StoreParticipant(1, s1)]              # 1 participant, majority is 2
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(alone, PUT, "a", "1", min_participants=2)
    assert res.status == ABORTED
    assert "quorum" in res.reason
    assert s1.exists("a") is False                 # nothing was even prepared


def test_quorum_satisfied_commits():
    stores, parts = make_cluster(2)                # 2 of 3 alive = majority
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(parts, PUT, "a", "1", min_participants=2)
    assert res.status == COMMITTED


def test_termination_decision_rules():
    """Cooperative termination for in-doubt participants."""
    # someone saw COMMIT -> the tx was decided commit; finish it
    assert termination_decision(["UNKNOWN", "COMMIT"]) == "COMMIT"
    # peers saw the abort -> abort
    assert termination_decision(["ABORT", "UNKNOWN"]) == "ABORT"
    # nobody knows anything (coordinator died before deciding) -> presumed abort
    assert termination_decision(["UNKNOWN", "UNKNOWN"]) == "ABORT"
    assert termination_decision([]) == "ABORT"


def test_in_doubt_participant_resolves_from_peer():
    """End-to-end termination: coordinator dies after sending COMMIT to only
    one participant; the stuck one learns the decision from its peer."""
    s1, s2 = KeyValueStore(), KeyValueStore()
    # phase 1 on both participants
    assert s1.prepare("tx9", PUT, "k", "v", 1)[0]
    assert s2.prepare("tx9", PUT, "k", "v", 1)[0]
    # coordinator decided COMMIT but only reached s1 before crashing
    s1.commit("tx9", 2)
    # s2 is in doubt: key still locked, value invisible
    assert s2.exists("k") is False
    assert s2.is_locked_by_other("k", "other-tx")
    # termination: s2 asks the peers and applies the decision
    outcome = termination_decision([s1.decision_of("tx9")])
    assert outcome == "COMMIT"
    s2.commit("tx9", 3)
    assert s2.get("k") == (True, "v")              # consistent with s1
    assert not s2.is_locked_by_other("k", "other-tx")   # lock released


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("2pc: OK")

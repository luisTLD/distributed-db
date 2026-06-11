import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.store import KeyValueStore, PUT, UPDATE, DELETE


def test_commit_makes_visible():
    s = KeyValueStore()
    ok, _ = s.prepare("t1", PUT, "a", "1", 1)
    assert ok
    assert s.get("a") == (False, "")   # not visible until commit
    assert s.commit("t1", 2)
    assert s.get("a") == (True, "1")


def test_abort_discards():
    s = KeyValueStore()
    s.prepare("t1", PUT, "a", "1", 1)
    s.abort("t1", 2)
    assert s.exists("a") is False
    # lock must be released so a later tx can use the key
    ok, _ = s.prepare("t2", PUT, "a", "2", 3)
    assert ok


def test_update_delete_require_existing_key():
    s = KeyValueStore()
    ok, reason = s.prepare("t1", UPDATE, "missing", "x", 1)
    assert not ok and "does not exist" in reason
    ok, reason = s.prepare("t2", DELETE, "missing", "", 1)
    assert not ok


def test_key_lock_blocks_concurrent_tx():
    s = KeyValueStore()
    ok, _ = s.prepare("t1", PUT, "k", "1", 1)
    assert ok
    ok2, reason = s.prepare("t2", PUT, "k", "2", 2)   # same key, other tx
    assert not ok2 and "locked" in reason
    # different key is fine
    ok3, _ = s.prepare("t3", PUT, "other", "9", 3)
    assert ok3


def test_delete_removes():
    s = KeyValueStore()
    s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
    s.prepare("t2", DELETE, "a", "", 3); s.commit("t2", 4)
    assert s.exists("a") is False


def test_wal_recovery_replays_committed_only():
    with tempfile.TemporaryDirectory() as d:
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.recover()
        s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
        s.prepare("t2", PUT, "b", "2", 3); s.commit("t2", 4)
        s.prepare("t3", PUT, "c", "3", 5)   # prepared but never committed
        # simulate crash: brand new instance recovers from disk
        s2 = KeyValueStore(data_dir=d, node_name="n1")
        s2.recover()
        assert s2.get("a") == (True, "1")
        assert s2.get("b") == (True, "2")
        assert s2.exists("c") is False      # in-doubt tx discarded


def test_snapshot_compacts_and_preserves():
    with tempfile.TemporaryDirectory() as d:
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
        s.take_snapshot()
        s.prepare("t2", PUT, "b", "2", 3); s.commit("t2", 4)
        s2 = KeyValueStore(data_dir=d, node_name="n1")
        s2.recover()
        assert s2.get("a") == (True, "1")   # from snapshot
        assert s2.get("b") == (True, "2")   # from post-snapshot WAL


def test_replace_all_state_transfer():
    s = KeyValueStore()
    s.replace_all({"x": "10", "y": "20"})
    assert s.get("x") == (True, "10")
    assert s.size() == 2


def test_last_commit_ts_tracks_newest_commit():
    s = KeyValueStore()
    assert s.last_commit_ts == 0
    s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 5)
    assert s.last_commit_ts == 5
    s.prepare("t2", PUT, "b", "2", 6); s.commit("t2", 9)
    assert s.last_commit_ts == 9
    # aborts do not advance it
    s.prepare("t3", PUT, "c", "3", 10); s.abort("t3", 12)
    assert s.last_commit_ts == 9
    # state transfer carries the source's last_commit_ts
    s2 = KeyValueStore()
    s2.replace_all(s.snapshot_dict(), s.last_commit_ts)
    assert s2.last_commit_ts == 9


def test_last_commit_ts_survives_recovery():
    with tempfile.TemporaryDirectory() as d:
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 4)
        s.take_snapshot()                                   # ts=4 in snapshot
        s.prepare("t2", PUT, "b", "2", 5); s.commit("t2", 8)  # ts=8 in WAL
        s2 = KeyValueStore(data_dir=d, node_name="n1")
        s2.recover()
        assert s2.get("a") == (True, "1")
        assert s2.get("b") == (True, "2")
        assert s2.last_commit_ts == 8


def test_decision_log_and_stale_pending():
    s = KeyValueStore()
    assert s.decision_of("nope") == "UNKNOWN"
    s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
    s.prepare("t2", PUT, "b", "2", 3); s.abort("t2", 4)
    assert s.decision_of("t1") == "COMMIT"
    assert s.decision_of("t2") == "ABORT"
    # a prepared tx with no decision becomes "stale" after the timeout
    s.prepare("t3", PUT, "c", "3", 5)
    import time as _t
    assert s.stale_pending(max_age=9999) == []                 # too recent
    stale = s.stale_pending(max_age=0.0, now=_t.monotonic() + 1)
    assert [op.tx_id for op in stale] == ["t3"]


def test_decision_log_survives_recovery():
    with tempfile.TemporaryDirectory() as d:
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
        s.prepare("t2", PUT, "b", "2", 3); s.abort("t2", 4)
        s2 = KeyValueStore(data_dir=d, node_name="n1")
        s2.recover()
        assert s2.decision_of("t1") == "COMMIT"
        assert s2.decision_of("t2") == "ABORT"


def test_old_snapshot_format_still_loads():
    """Snapshots written before the last_commit_ts change still recover."""
    import json
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "n1.snapshot"), "w") as fh:
            json.dump({"a": "1"}, fh)          # formato antigo: dict puro
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.recover()
        assert s.get("a") == (True, "1")
        assert s.last_commit_ts == 0


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("store: OK")

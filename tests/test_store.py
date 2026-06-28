"""Testes do armazém: commit/abort, locks e recuperação por WAL."""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.store import KeyValueStore, PUT, UPDATE, DELETE


def test_commit_makes_visible():
    # Escrita preparada NÃO aparece em leituras; só depois do commit.
    s = KeyValueStore()
    ok, _ = s.prepare("t1", PUT, "a", "1", 1)
    assert ok
    assert s.get("a") == (False, "")   # invisível até o commit
    assert s.commit("t1", 2)
    assert s.get("a") == (True, "1")


def test_abort_discards():
    # Abort descarta a operação e libera o lock da chave.
    s = KeyValueStore()
    s.prepare("t1", PUT, "a", "1", 1)
    s.abort("t1", 2)
    assert s.exists("a") is False
    ok, _ = s.prepare("t2", PUT, "a", "2", 3)   # lock liberado p/ outra tx
    assert ok


def test_update_delete_require_existing_key():
    # Validação semântica da fase de votação.
    s = KeyValueStore()
    ok, reason = s.prepare("t1", UPDATE, "missing", "x", 1)
    assert not ok and "does not exist" in reason
    ok, reason = s.prepare("t2", DELETE, "missing", "", 1)
    assert not ok


def test_key_lock_blocks_concurrent_tx():
    # Exclusão mútua: mesma chave bloqueia; chaves diferentes seguem.
    s = KeyValueStore()
    ok, _ = s.prepare("t1", PUT, "k", "1", 1)
    assert ok
    ok2, reason = s.prepare("t2", PUT, "k", "2", 2)   # mesma chave, outra tx
    assert not ok2 and "locked" in reason
    ok3, _ = s.prepare("t3", PUT, "other", "9", 3)    # chave diferente: ok
    assert ok3


def test_delete_removes():
    s = KeyValueStore()
    s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
    s.prepare("t2", DELETE, "a", "", 3); s.commit("t2", 4)
    assert s.exists("a") is False


def test_wal_recovery_replays_committed_only():
    # Recuperação pós-queda: reaplica SÓ transações com COMMIT no log.
    with tempfile.TemporaryDirectory() as d:
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.recover()
        s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
        s.prepare("t2", PUT, "b", "2", 3); s.commit("t2", 4)
        s.prepare("t3", PUT, "c", "3", 5)   # preparada, nunca decidida
        # Simula a queda: instância nova recupera do disco.
        s2 = KeyValueStore(data_dir=d, node_name="n1")
        s2.recover()
        assert s2.get("a") == (True, "1")
        assert s2.get("b") == (True, "2")
        assert s2.exists("c") is False      # tx em dúvida descartada


def test_snapshot_compacts_and_preserves():
    # Snapshot persiste o estado e zera o WAL; nada se perde.
    with tempfile.TemporaryDirectory() as d:
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
        s.take_snapshot()
        s.prepare("t2", PUT, "b", "2", 3); s.commit("t2", 4)
        s2 = KeyValueStore(data_dir=d, node_name="n1")
        s2.recover()
        assert s2.get("a") == (True, "1")   # veio do snapshot
        assert s2.get("b") == (True, "2")   # veio do WAL pós-snapshot


def test_replace_all_state_transfer():
    # Transferência de estado (reingresso): substitui tudo.
    s = KeyValueStore()
    s.replace_all({"x": "10", "y": "20"})
    assert s.get("x") == (True, "10")
    assert s.size() == 2


def test_last_commit_ts_tracks_newest_commit():
    # last_commit_ts acompanha o commit mais novo; abort não conta.
    s = KeyValueStore()
    assert s.last_commit_ts == 0
    s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 5)
    assert s.last_commit_ts == 5
    s.prepare("t2", PUT, "b", "2", 6); s.commit("t2", 9)
    assert s.last_commit_ts == 9
    s.prepare("t3", PUT, "c", "3", 10); s.abort("t3", 12)
    assert s.last_commit_ts == 9
    # A transferência de estado carrega o last_commit_ts da origem.
    s2 = KeyValueStore()
    s2.replace_all(s.snapshot_dict(), s.last_commit_ts)
    assert s2.last_commit_ts == 9


def test_last_commit_ts_survives_recovery():
    # O carimbo sobrevive a snapshot + reinício (vital para a eleição).
    with tempfile.TemporaryDirectory() as d:
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 4)
        s.take_snapshot()                                     # ts=4 no snapshot
        s.prepare("t2", PUT, "b", "2", 5); s.commit("t2", 8)  # ts=8 no WAL
        s2 = KeyValueStore(data_dir=d, node_name="n1")
        s2.recover()
        assert s2.get("a") == (True, "1")
        assert s2.get("b") == (True, "2")
        assert s2.last_commit_ts == 8


def test_decision_log_and_stale_pending():
    # Log de decisões (terminação) + detecção de tx em dúvida por idade.
    s = KeyValueStore()
    assert s.decision_of("nope") == "UNKNOWN"
    s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
    s.prepare("t2", PUT, "b", "2", 3); s.abort("t2", 4)
    assert s.decision_of("t1") == "COMMIT"
    assert s.decision_of("t2") == "ABORT"
    # Tx preparada sem decisão vira "stale" depois do timeout.
    s.prepare("t3", PUT, "c", "3", 5)
    import time as _t
    assert s.stale_pending(max_age=9999) == []                 # recente demais
    stale = s.stale_pending(max_age=0.0, now=_t.monotonic() + 1)
    assert [op.tx_id for op in stale] == ["t3"]


def test_decision_log_survives_recovery():
    # As decisões são recuperadas do WAL após reinício.
    with tempfile.TemporaryDirectory() as d:
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.prepare("t1", PUT, "a", "1", 1); s.commit("t1", 2)
        s.prepare("t2", PUT, "b", "2", 3); s.abort("t2", 4)
        s2 = KeyValueStore(data_dir=d, node_name="n1")
        s2.recover()
        assert s2.decision_of("t1") == "COMMIT"
        assert s2.decision_of("t2") == "ABORT"


def test_old_snapshot_format_still_loads():
    """Snapshots do formato antigo (dict puro) continuam carregando."""
    import json
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "n1.snapshot"), "w") as fh:
            json.dump({"a": "1"}, fh)          # formato antigo
        s = KeyValueStore(data_dir=d, node_name="n1")
        s.recover()
        assert s.get("a") == (True, "1")
        assert s.last_commit_ts == 0


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("store: OK")

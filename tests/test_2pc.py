"""Testes do Two-Phase Commit, do quórum e do protocolo de terminação.

Provam as garantias centrais das transações distribuídas — atomicidade
(tudo-ou-nada), aborto em caso de voto NÃO ou falha, recusa sem quórum e a
resolução de transações "em dúvida" quando o coordenador morre. Os
participantes são stores locais, então tudo roda sem rede.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.lamport import LamportClock
from distdb.store import KeyValueStore, PUT, UPDATE
from distdb.coordinator import (TwoPhaseCommit, COMMITTED, ABORTED,
                                termination_decision)


class StoreParticipant:
    """Adapta um KeyValueStore à interface de participante do coordenador."""

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
    """Participante que lança exceção — simula um nó que caiu."""

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
    # Caminho feliz: todos votam SIM -> commit atômico em todos.
    stores, parts = make_cluster(3)
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(parts, PUT, "a", "1")
    assert res.status == COMMITTED
    assert res.yes_votes == 3 and res.cohort_size == 3
    for s in stores:
        assert s.get("a") == (True, "1")   # visível em TODOS os nós


def test_abort_when_one_votes_no():
    # UPDATE de chave inexistente -> votos NÃO -> aborta em todos.
    stores, parts = make_cluster(3)
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(parts, UPDATE, "missing", "x")
    assert res.status == ABORTED
    for s in stores:
        assert s.exists("missing") is False


def test_participant_failure_aborts_and_is_reported():
    # Participante caído na votação -> aborta sem expor dado parcial.
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
    # Os nós saudáveis NÃO expuseram o valor (atomicidade preservada).
    assert s_ok1.exists("a") is False
    assert s_ok2.exists("a") is False


def test_retry_after_excluding_failed_node_succeeds():
    """Espelha o que o nó faz: exclui a réplica morta, repete e commita."""
    s1, s3 = KeyValueStore(), KeyValueStore()
    live = [StoreParticipant(1, s1), StoreParticipant(3, s3)]  # nó 2 excluído
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(live, PUT, "a", "1")
    assert res.status == COMMITTED
    assert s1.get("a") == (True, "1") and s3.get("a") == (True, "1")


def test_no_quorum_aborts_without_touching_stores():
    """Com 2 de 3 nós mortos, o líder sozinho deve recusar a escrita."""
    s1 = KeyValueStore()
    alone = [StoreParticipant(1, s1)]              # 1 participante; maioria = 2
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(alone, PUT, "a", "1", min_participants=2)
    assert res.status == ABORTED
    assert "quorum" in res.reason
    assert s1.exists("a") is False                 # nem chegou a preparar


def test_quorum_satisfied_commits():
    # 2 de 3 vivos = maioria -> a escrita passa.
    stores, parts = make_cluster(2)
    tpc = TwoPhaseCommit(LamportClock())
    res = tpc.execute(parts, PUT, "a", "1", min_participants=2)
    assert res.status == COMMITTED


def test_termination_decision_rules():
    """Regras da terminação cooperativa para participantes em dúvida."""
    # Alguém viu COMMIT -> a decisão foi commit; terminar commitando.
    assert termination_decision(["UNKNOWN", "COMMIT"]) == "COMMIT"
    # Peers viram o abort -> abortar.
    assert termination_decision(["ABORT", "UNKNOWN"]) == "ABORT"
    # Ninguém sabe nada (coordenador morreu antes de decidir) -> presumed abort.
    assert termination_decision(["UNKNOWN", "UNKNOWN"]) == "ABORT"
    assert termination_decision([]) == "ABORT"


def test_in_doubt_participant_resolves_from_peer():
    """Ponta a ponta: coordenador morre depois de commitar em SÓ UM
    participante; o que ficou preso descobre a decisão pelo peer."""
    s1, s2 = KeyValueStore(), KeyValueStore()
    # Fase 1 nos dois participantes.
    assert s1.prepare("tx9", PUT, "k", "v", 1)[0]
    assert s2.prepare("tx9", PUT, "k", "v", 1)[0]
    # O coordenador decidiu COMMIT mas só alcançou s1 antes de morrer.
    s1.commit("tx9", 2)
    # s2 está em dúvida: chave travada, valor invisível.
    assert s2.exists("k") is False
    assert s2.is_locked_by_other("k", "other-tx")
    # Terminação: s2 consulta o peer e aplica a decisão.
    outcome = termination_decision([s1.decision_of("tx9")])
    assert outcome == "COMMIT"
    s2.commit("tx9", 3)
    assert s2.get("k") == (True, "v")                   # consistente com s1
    assert not s2.is_locked_by_other("k", "other-tx")   # lock liberado


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("2pc: OK")

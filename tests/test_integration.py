"""Teste de integração com cluster simulado (sem gRPC).

Valida o fluxo COMPLETO de tolerância a falhas — escrita replicada, queda
de réplica, queda do líder, eleição e recuperação por transferência de
estado — usando os módulos reais de lógica, mas com a "rede" simulada em
memória (nó morto = exceção, como um peer gRPC caído). Isso torna o cenário
determinístico e executável em milissegundos.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.lamport import LamportClock
from distdb.store import KeyValueStore, PUT
from distdb.coordinator import TwoPhaseCommit, COMMITTED
from distdb.election import run_election


class SimNode:
    """Um nó simulado: store + relógio + flag vivo/morto."""

    def __init__(self, node_id):
        self.node_id = node_id
        self.store = KeyValueStore()
        self.clock = LamportClock()
        self.alive = True

    # Interface de participante do 2PC (lança exceção quando "morto",
    # exatamente como um peer gRPC inalcançável).
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
    """Orquestra os nós simulados como o node.py faz com os reais."""

    def __init__(self, ids):
        self.nodes = {i: SimNode(i) for i in ids}
        self.ids = sorted(ids)
        self.leader = max(self.ids)  # maior id começa como líder

    def live_ids(self):
        return [i for i in self.ids if self.nodes[i].alive]

    def write(self, key, value):
        """Líder coordena o 2PC sobre o cohort vivo, repetindo após falhas."""
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
            excluded.update(res.failed_nodes)   # exclui os mortos e repete
        return res

    def kill(self, node_id):
        self.nodes[node_id].alive = False

    def elect(self):
        """Roda o Bully a partir de cada nó vivo; maior id vivo vira líder."""
        for nid in sorted(self.live_ids()):
            def send(peer, _self=nid):
                return self.nodes[peer].alive
            became, _ = run_election(nid, self.ids, send)
            if became:
                self.leader = nid
        return self.leader

    def sync(self, node_id):
        """Nó recuperado puxa o estado commitado do líder (state transfer)."""
        leader_state = self.nodes[self.leader].store.snapshot_dict()
        self.nodes[node_id].store.replace_all(leader_state)
        self.nodes[node_id].alive = True


def test_full_fault_tolerance_flow():
    c = SimCluster([1, 2, 3])
    assert c.leader == 3

    # 1) Escrita normal replica atomicamente nos três nós.
    res = c.write("a", "1")
    assert res.status == COMMITTED
    for i in (1, 2, 3):
        assert c.nodes[i].store.get("a") == (True, "1")

    # 2) Uma réplica morre; escritas seguem commitando no cohort vivo.
    c.kill(2)
    res = c.write("b", "2")
    assert res.status == COMMITTED
    assert c.nodes[1].store.get("b") == (True, "2")
    assert c.nodes[3].store.get("b") == (True, "2")
    assert c.nodes[2].store.exists("b") is False  # nó 2 perdeu (está morto)

    # 3) O líder morre; sobreviventes elegem o maior id vivo (nó 1).
    c.kill(3)
    new_leader = c.elect()
    assert new_leader == 1
    res = c.write("c", "3")
    assert res.status == COMMITTED
    assert c.nodes[1].store.get("c") == (True, "3")

    # 4) Nó 2 volta e sincroniza o estado com o novo líder.
    c.sync(2)
    assert c.nodes[2].store.get("a") == (True, "1")
    assert c.nodes[2].store.get("b") == (True, "2")
    assert c.nodes[2].store.get("c") == (True, "3")


if __name__ == "__main__":
    test_full_fault_tolerance_flow()
    print("integration: OK")

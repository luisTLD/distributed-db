"""Testes da eleição Bully e do detector de falhas.

Validam a regra "maior id vivo vence" em todos os casos (vence direto,
desiste, vence porque os maiores morreram) e o comportamento do detector
(timeout e suspeita imediata) — sem precisar de rede, pois o envio de
mensagens é injetado como função.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.election import run_election, higher_ids
from distdb.failure_detector import FailureDetector


def test_higher_ids():
    assert higher_ids(2, [1, 2, 3, 4]) == [3, 4]
    assert higher_ids(4, [1, 2, 3, 4]) == []


def test_highest_node_wins_immediately():
    # Nó 3 no cluster {1,2,3} não tem ninguém maior -> vira líder direto.
    became, answered = run_election(3, [1, 2, 3], send_election=lambda p: True)
    assert became is True and answered == []


def test_backs_off_when_higher_alive():
    # Nó 1 inicia a eleição; 2 e 3 vivos respondem -> nó 1 desiste.
    became, answered = run_election(1, [1, 2, 3], send_election=lambda p: True)
    assert became is False
    assert set(answered) == {2, 3}


def test_wins_when_all_higher_dead():
    # Nó 1 com todos os maiores inalcançáveis -> vence a eleição.
    became, answered = run_election(1, [1, 2, 3], send_election=lambda p: False)
    assert became is True and answered == []


def test_failure_detector_marks_dead_after_timeout():
    # Peer silencioso além do timeout deve ser considerado morto.
    fd = FailureDetector([1, 2, 3], timeout=2.0)
    fd.record_alive(1, now=10.0)
    fd.record_alive(2, now=10.0)
    assert set(fd.alive_nodes(now=11.0)) == {1, 2}
    # Em t=13, vistos pela última vez em t=10 -> 3s > timeout de 2s -> mortos.
    assert fd.alive_nodes(now=13.0) == []


def test_failure_detector_record_dead_immediate():
    # RPC que falhou marca o nó como suspeito NA HORA, sem esperar timeout.
    fd = FailureDetector([1, 2], timeout=10.0)
    fd.record_alive(1, now=0.0)
    assert fd.is_alive(1, now=1.0)
    fd.record_dead(1)
    assert not fd.is_alive(1, now=1.0)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("election+fd: OK")

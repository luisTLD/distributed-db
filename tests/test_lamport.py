"""Testes do relógio de Lamport."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.lamport import LamportClock


def test_tick_is_monotonic():
    # Regra 1: cada evento local avança o contador em 1.
    c = LamportClock()
    assert c.tick() == 1
    assert c.tick() == 2
    assert c.value == 2


def test_update_takes_max_plus_one():
    # Regra 2: ao receber carimbo t, o relógio vira max(local, t) + 1.
    c = LamportClock(start=5)
    assert c.update(3) == 6      # max(5,3)+1
    assert c.update(20) == 21    # max(6,20)+1
    assert c.update(0) == 22


def test_happens_before():
    # Se A causou B (mensagem de A para B), então C(A) < C(B).
    a = LamportClock()
    b = LamportClock()
    ta = a.tick()          # evento em A
    tb = b.update(ta)      # B recebe a mensagem de A
    assert tb > ta         # ordem causal preservada


if __name__ == "__main__":
    test_tick_is_monotonic()
    test_update_takes_max_plus_one()
    test_happens_before()
    print("lamport: OK")

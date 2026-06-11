import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.lamport import LamportClock


def test_tick_is_monotonic():
    c = LamportClock()
    assert c.tick() == 1
    assert c.tick() == 2
    assert c.value == 2


def test_update_takes_max_plus_one():
    c = LamportClock(start=5)
    assert c.update(3) == 6      # max(5,3)+1
    assert c.update(20) == 21    # max(6,20)+1
    assert c.update(0) == 22


def test_happens_before():
    a = LamportClock()
    b = LamportClock()
    ta = a.tick()          # event on A
    tb = b.update(ta)      # B receives message from A
    assert tb > ta         # causal order preserved


if __name__ == "__main__":
    test_tick_is_monotonic()
    test_update_takes_max_plus_one()
    test_happens_before()
    print("lamport: OK")

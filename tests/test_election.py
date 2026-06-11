import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from distdb.election import run_election, higher_ids
from distdb.failure_detector import FailureDetector


def test_higher_ids():
    assert higher_ids(2, [1, 2, 3, 4]) == [3, 4]
    assert higher_ids(4, [1, 2, 3, 4]) == []


def test_highest_node_wins_immediately():
    # node 3 in a {1,2,3} cluster has no higher peers -> becomes leader
    became, answered = run_election(3, [1, 2, 3], send_election=lambda p: True)
    assert became is True and answered == []


def test_backs_off_when_higher_alive():
    # node 1 runs election; node 2 and 3 are alive and answer -> node 1 loses
    became, answered = run_election(1, [1, 2, 3], send_election=lambda p: True)
    assert became is False
    assert set(answered) == {2, 3}


def test_wins_when_all_higher_dead():
    # node 1; higher peers all unreachable -> node 1 becomes leader
    became, answered = run_election(1, [1, 2, 3], send_election=lambda p: False)
    assert became is True and answered == []


def test_failure_detector_marks_dead_after_timeout():
    fd = FailureDetector([1, 2, 3], timeout=2.0)
    fd.record_alive(1, now=10.0)
    fd.record_alive(2, now=10.0)
    assert set(fd.alive_nodes(now=11.0)) == {1, 2}
    # at t=13 node 1/2 last seen at 10 -> 3s > 2s timeout -> dead
    assert fd.alive_nodes(now=13.0) == []


def test_failure_detector_record_dead_immediate():
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

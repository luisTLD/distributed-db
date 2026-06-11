"""Bully leader-election algorithm -- manual implementation.

The node with the highest id that is alive must become the leader (coordinator
of writes). When a node notices the leader is gone it runs an election:

1. It sends an ``ELECTION`` message to every node with a **higher** id.
2. If no higher node answers, it wins and announces itself as the new leader
   (``COORDINATOR``/``Announce``) to everyone else.
3. If some higher node answers ("I'm alive, back off"), this node drops out and
   waits -- the higher node will run its own election and eventually announce.

This module contains only the decision logic so it can be unit-tested without
any networking. The actual message sending and the COORDINATOR broadcast live
in :mod:`distdb.node`.
"""

from __future__ import annotations

from typing import Callable, List, Tuple


def higher_ids(node_id: int, all_ids: List[int]) -> List[int]:
    return sorted(i for i in all_ids if i > node_id)


def run_election(node_id: int, all_ids: List[int],
                 send_election: Callable[[int], bool]) -> Tuple[bool, List[int]]:
    """Run one round of the Bully election from *node_id*'s point of view.

    *send_election(peer_id)* must return True if that higher-id peer is alive and
    answered the ELECTION message, False if it is unreachable.

    Returns ``(became_leader, answering_peers)``. ``became_leader`` is True when
    no higher node answered, meaning this node should announce itself leader.
    """
    answered: List[int] = []
    for peer in higher_ids(node_id, all_ids):
        try:
            if send_election(peer):
                answered.append(peer)
        except Exception:
            # Treat an unreachable higher peer as "did not answer".
            continue
    became_leader = len(answered) == 0
    return became_leader, answered

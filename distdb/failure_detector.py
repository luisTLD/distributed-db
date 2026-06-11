"""Heartbeat-based failure detector (membership / replica control).

Every node periodically sends a ``Heartbeat`` RPC to every other node. This
component records the last moment each peer was seen and decides who is alive.

It is the basis for the system's fault tolerance:
* the **leader** only includes *alive* replicas in a 2PC transaction cohort, and
  brings recovered replicas back up to date via state transfer;
* **replicas** watch the *leader*; if it stops answering heartbeats they trigger
  a Bully election.

Time is injected (``now``) so the logic is deterministic and unit-testable.
"""

from __future__ import annotations

import threading
from typing import Dict, List


class FailureDetector:
    def __init__(self, peer_ids: List[int], timeout: float = 3.0) -> None:
        self._timeout = timeout
        self._last_seen: Dict[int, float] = {pid: 0.0 for pid in peer_ids}
        self._suspected: Dict[int, bool] = {pid: True for pid in peer_ids}
        self._lock = threading.Lock()

    def record_alive(self, node_id: int, now: float) -> None:
        with self._lock:
            self._last_seen[node_id] = now
            self._suspected[node_id] = False

    def record_dead(self, node_id: int) -> None:
        """Immediately suspect a node (e.g. an RPC to it just failed)."""
        with self._lock:
            self._suspected[node_id] = True

    def is_alive(self, node_id: int, now: float) -> bool:
        with self._lock:
            if self._suspected.get(node_id, True):
                # Was suspected; only trust it again after a fresh heartbeat.
                return (now - self._last_seen.get(node_id, 0.0)) <= self._timeout \
                    and not self._suspected.get(node_id, True)
            return (now - self._last_seen.get(node_id, 0.0)) <= self._timeout

    def evaluate(self, now: float) -> None:
        """Move peers past the timeout into the suspected set."""
        with self._lock:
            for pid, seen in self._last_seen.items():
                if (now - seen) > self._timeout:
                    self._suspected[pid] = True

    def alive_nodes(self, now: float) -> List[int]:
        return [pid for pid in self._last_seen if self.is_alive(pid, now)]

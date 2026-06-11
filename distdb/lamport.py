"""Lamport logical clock (manual implementation).

Each process keeps a monotonically increasing counter. The rules are:

* before a local event / sending a message: ``clock += 1``;
* on receiving a message carrying timestamp ``t``:
  ``clock = max(clock, t) + 1``.

This gives the happened-before guarantee: if event *a* causally precedes event
*b*, then ``C(a) < C(b)``. We use it to order operations across the cluster and
to timestamp transactions.
"""

from __future__ import annotations

import threading


class LamportClock:
    def __init__(self, start: int = 0) -> None:
        self._value = start
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        """Advance the clock for a local event and return the new value."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_timestamp: int) -> int:
        """Merge an incoming timestamp and return the new clock value."""
        with self._lock:
            self._value = max(self._value, int(received_timestamp)) + 1
            return self._value

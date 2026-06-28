"""Relógio lógico de Lamport."""

from __future__ import annotations

import threading


class LamportClock:
    def __init__(self, start: int = 0) -> None:
        self._value = start
        self._lock = threading.Lock()   # várias threads usam o mesmo relógio

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        """Regra 1: avança o relógio para um evento local e devolve o valor."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_timestamp: int) -> int:
        """Regra 2: funde um carimbo recebido e devolve o novo valor."""
        with self._lock:
            self._value = max(self._value, int(received_timestamp)) + 1
            return self._value

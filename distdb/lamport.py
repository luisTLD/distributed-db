"""Relógio lógico de Lamport — implementação manual.

Em um sistema distribuído não há relógio físico confiável comum a todas as
máquinas. O relógio de Lamport dá uma ORDEM aos eventos do cluster sem
depender de hora de parede: se o evento A causou o evento B, então
C(A) < C(B) (relação happened-before).

Cada processo mantém um contador inteiro com as duas regras clássicas:
  1. antes de um evento local / envio de mensagem: clock += 1  (tick)
  2. ao receber uma mensagem com carimbo t: clock = max(clock, t) + 1 (update)

No projeto, todo RPC carrega esse carimbo; ele data as transações no WAL e o
last_commit_ts (usado na eleição) é derivado dele.
"""

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

"""Detector de falhas por heartbeat.

Nenhum nó consegue "ver" diretamente que outro morreu — só dá para inferir
pela ausência de respostas. Este componente transforma silêncio em decisão:
registra o último instante em que cada peer foi visto (heartbeats chegam a
cada ~1s) e marca como SUSPEITO quem ficou mudo além do timeout — ou
imediatamente, quando um RPC para o nó falha. É a base da tolerância a
falhas:
  * o LÍDER monta o cohort do 2PC só com réplicas vivas;
  * as RÉPLICAS percebem a morte do líder e disparam a eleição.

O tempo é injetado por parâmetro (now), o que torna a lógica determinística
e testável sem esperas reais.
"""

from __future__ import annotations

import threading
from typing import Dict, List


class FailureDetector:
    def __init__(self, peer_ids: List[int], timeout: float = 3.0) -> None:
        self._timeout = timeout
        self._last_seen: Dict[int, float] = {pid: 0.0 for pid in peer_ids}
        # Todos começam suspeitos até o primeiro heartbeat chegar.
        self._suspected: Dict[int, bool] = {pid: True for pid in peer_ids}
        self._lock = threading.Lock()

    def record_alive(self, node_id: int, now: float) -> None:
        """Heartbeat recebido: o peer está vivo agora."""
        with self._lock:
            self._last_seen[node_id] = now
            self._suspected[node_id] = False

    def record_dead(self, node_id: int) -> None:
        """Suspeita imediata (ex.: um RPC para esse nó acabou de falhar)."""
        with self._lock:
            self._suspected[node_id] = True

    def is_alive(self, node_id: int, now: float) -> bool:
        with self._lock:
            if self._suspected.get(node_id, True):
                # Estava suspeito: só volta a ser confiável com heartbeat novo.
                return (now - self._last_seen.get(node_id, 0.0)) <= self._timeout \
                    and not self._suspected.get(node_id, True)
            return (now - self._last_seen.get(node_id, 0.0)) <= self._timeout

    def evaluate(self, now: float) -> None:
        """Varre os peers e move para suspeito quem estourou o timeout."""
        with self._lock:
            for pid, seen in self._last_seen.items():
                if (now - seen) > self._timeout:
                    self._suspected[pid] = True

    def alive_nodes(self, now: float) -> List[int]:
        return [pid for pid in self._last_seen if self.is_alive(pid, now)]

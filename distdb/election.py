"""Eleição de líder pelo algoritmo Bully (lógica de decisão)."""

from __future__ import annotations

from typing import Callable, List, Tuple


def higher_ids(node_id: int, all_ids: List[int]) -> List[int]:
    """Ids maiores que o meu (os únicos que preciso desafiar)."""
    return sorted(i for i in all_ids if i > node_id)


def run_election(node_id: int, all_ids: List[int],
                 send_election: Callable[[int], bool]) -> Tuple[bool, List[int]]:
    """Roda uma rodada de eleição do ponto de vista de node_id.

    send_election(peer_id) deve devolver True se o peer de id maior está vivo
    e respondeu ao ELECTION; False se está inalcançável.

    Devolve (virei_lider, quem_respondeu): virei_lider=True quando nenhum nó
    maior respondeu — este nó deve se anunciar líder.
    """
    answered: List[int] = []
    for peer in higher_ids(node_id, all_ids):
        try:
            if send_election(peer):
                answered.append(peer)
        except Exception:
            # Peer maior inalcançável conta como "não respondeu".
            continue
    became_leader = len(answered) == 0
    return became_leader, answered

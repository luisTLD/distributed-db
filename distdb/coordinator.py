"""Coordenador do Two-Phase Commit e regra de terminação."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import List, Protocol, Tuple

COMMITTED = "COMMITTED"
ABORTED = "ABORTED"


class Participant(Protocol):
    """Interface mínima que o coordenador exige de um participante."""
    node_id: int

    def prepare(self, tx_id: str, op: str, key: str, value: str, timestamp: int) -> Tuple[bool, str]:
        ...

    def commit(self, tx_id: str, timestamp: int) -> bool:
        ...

    def abort(self, tx_id: str, timestamp: int) -> bool:
        ...


@dataclass
class TxResult:
    """Resultado de uma transação: status, motivo e estatísticas de votos."""
    status: str
    tx_id: str
    timestamp: int
    reason: str = ""
    failed_nodes: List[int] = field(default_factory=list)
    yes_votes: int = 0
    cohort_size: int = 0


class TwoPhaseCommit:
    def __init__(self, clock) -> None:
        self._clock = clock          # relógio de Lamport do coordenador

    def execute(self, participants: List[Participant], op: str, key: str,
                value: str = "", min_participants: int = 0) -> TxResult:
        tx_id = uuid.uuid4().hex[:12]            # id único da transação
        ts = self._clock.tick()

        # Checagem de QUÓRUM (controle de réplicas / anti split-brain):
        # recusa a transação se há menos participantes vivos que a maioria.
        # Um líder isolado numa partição minoritária não consegue commitar.
        if len(participants) < min_participants:
            return TxResult(
                ABORTED, tx_id, ts,
                reason=(f"no quorum: only {len(participants)} live participant(s), "
                        f"majority of {min_participants} required"),
                yes_votes=0, cohort_size=len(participants))

        votes_yes = 0
        failed_nodes: List[int] = []
        abort_reason = ""

        # ---------------- Fase 1: votação ----------------
        for p in participants:
            try:
                vote, reason = p.prepare(tx_id, op, key, value, ts)
                if vote:
                    votes_yes += 1
                else:
                    abort_reason = abort_reason or f"node {p.node_id}: {reason}"
            except Exception as exc:  # falha de transporte/participante
                failed_nodes.append(p.node_id)
                abort_reason = abort_reason or f"node {p.node_id} unreachable: {exc}"

        all_yes = (votes_yes == len(participants)) and not failed_nodes

        # ---------------- Fase 2: decisão ----------------
        decision_ts = self._clock.tick()
        if all_yes:
            for p in participants:
                try:
                    p.commit(tx_id, decision_ts)
                except Exception:
                    # Votou SIM e está durável: ao se recuperar, termina o
                    # commit pelo WAL/terminação. Só sinalizamos a falha.
                    failed_nodes.append(p.node_id)
            return TxResult(COMMITTED, tx_id, decision_ts,
                            yes_votes=votes_yes, cohort_size=len(participants),
                            failed_nodes=failed_nodes)

        # Algum NÃO ou falha: aborta em todos (ninguém aplica nada).
        for p in participants:
            try:
                p.abort(tx_id, decision_ts)
            except Exception:
                pass
        return TxResult(ABORTED, tx_id, decision_ts, reason=abort_reason,
                        failed_nodes=failed_nodes,
                        yes_votes=votes_yes, cohort_size=len(participants))


def termination_decision(peer_decisions: List[str]) -> str:
    """Regra de terminação cooperativa para um participante "em dúvida".

    Ele preparou a transação e nunca soube a decisão (o coordenador morreu).
    Pergunta aos demais participantes o que sabem:

    * se ALGUM peer viu COMMIT  -> a decisão foi commit; aplica também;
    * caso contrário            -> aborta ("presumed abort"). Seguro, pois o
      coordenador só envia COMMIT depois de TODOS os votos SIM; se nenhum
      peer alcançável commitou, nenhum cliente recebeu confirmação.
    """
    if any(d == "COMMIT" for d in peer_decisions):
        return "COMMIT"
    return "ABORT"

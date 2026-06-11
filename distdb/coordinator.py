"""Two-Phase Commit (2PC) coordinator -- manual implementation.

The coordinator drives an atomic write across a set of *participants* (the
leader's own local store plus the live replicas). It is transport agnostic: a
participant is any object exposing ``prepare`` / ``commit`` / ``abort``. In the
unit tests these are local in-memory participants; in production they are
gRPC-backed proxies that call the remote ``Prepare``/``Commit``/``Abort`` RPCs.

Protocol
--------
Phase 1 -- VOTING
    Coordinator sends ``PREPARE(tx, op, key, value)`` to every participant.
    Each participant locks the key, validates, makes the intent durable in its
    write-ahead log, and votes YES or NO. An unreachable participant counts as
    NO (and is reported so the caller can mark it failed).

Phase 2 -- DECISION
    * If *every* participant voted YES -> send ``COMMIT(tx)`` to all. The write
      becomes visible atomically on all of them.
    * If *any* participant voted NO or was unreachable -> send ``ABORT(tx)`` to
      all. No participant exposes the change; locks are released.

This guarantees atomicity (all-or-nothing) and, together with the per-key locks
held between PREPARE and the decision, isolation of conflicting writes.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import List, Protocol, Tuple

COMMITTED = "COMMITTED"
ABORTED = "ABORTED"


class Participant(Protocol):
    node_id: int

    def prepare(self, tx_id: str, op: str, key: str, value: str, timestamp: int) -> Tuple[bool, str]:
        ...

    def commit(self, tx_id: str, timestamp: int) -> bool:
        ...

    def abort(self, tx_id: str, timestamp: int) -> bool:
        ...


@dataclass
class TxResult:
    status: str
    tx_id: str
    timestamp: int
    reason: str = ""
    failed_nodes: List[int] = field(default_factory=list)
    yes_votes: int = 0
    cohort_size: int = 0


class TwoPhaseCommit:
    def __init__(self, clock) -> None:
        self._clock = clock

    def execute(self, participants: List[Participant], op: str, key: str,
                value: str = "", min_participants: int = 0) -> TxResult:
        tx_id = uuid.uuid4().hex[:12]
        ts = self._clock.tick()

        # Quorum check (replica control / split-brain protection): refuse to
        # run a transaction over fewer participants than the required majority.
        # A leader isolated in a minority partition therefore cannot commit
        # writes that the majority side would never see.
        if len(participants) < min_participants:
            return TxResult(
                ABORTED, tx_id, ts,
                reason=(f"no quorum: only {len(participants)} live participant(s), "
                        f"majority of {min_participants} required"),
                yes_votes=0, cohort_size=len(participants))

        votes_yes = 0
        failed_nodes: List[int] = []
        abort_reason = ""

        # ---------------- Phase 1: voting ----------------
        for p in participants:
            try:
                vote, reason = p.prepare(tx_id, op, key, value, ts)
                if vote:
                    votes_yes += 1
                else:
                    abort_reason = abort_reason or f"node {p.node_id}: {reason}"
            except Exception as exc:  # transport / participant failure
                failed_nodes.append(p.node_id)
                abort_reason = abort_reason or f"node {p.node_id} unreachable: {exc}"

        all_yes = (votes_yes == len(participants)) and not failed_nodes

        # ---------------- Phase 2: decision ----------------
        decision_ts = self._clock.tick()
        if all_yes:
            for p in participants:
                try:
                    p.commit(tx_id, decision_ts)
                except Exception as exc:
                    # The participant voted YES (is prepared) and is durable; it
                    # will finish the commit from its WAL on recovery. We still
                    # flag it so the failure detector notices.
                    failed_nodes.append(p.node_id)
            return TxResult(COMMITTED, tx_id, decision_ts,
                            yes_votes=votes_yes, cohort_size=len(participants),
                            failed_nodes=failed_nodes)

        for p in participants:
            try:
                p.abort(tx_id, decision_ts)
            except Exception:
                pass
        return TxResult(ABORTED, tx_id, decision_ts, reason=abort_reason,
                        failed_nodes=failed_nodes,
                        yes_votes=votes_yes, cohort_size=len(participants))


def termination_decision(peer_decisions: List[str]) -> str:
    """Cooperative termination rule for an in-doubt 2PC participant.

    A participant prepared a transaction but never heard the decision (the
    coordinator crashed). It asks the other participants what they know:

    * if ANY peer saw ``COMMIT``  -> the coordinator decided commit; apply it;
    * otherwise (peers saw ABORT or nothing) -> abort ("presumed abort"). This
      is safe because the coordinator only sends COMMIT after every YES vote,
      so if no reachable peer committed, no client was told the write succeeded.
    """
    if any(d == "COMMIT" for d in peer_decisions):
        return "COMMIT"
    return "ABORT"

"""Armazém chave-valor durável: WAL, locks por chave e log de decisões.

É a "memória" de cada nó: concentra tudo que envolve guardar dados com
segurança — o dicionário em memória, a durabilidade em disco (para
sobreviver a quedas) e os locks por chave (exclusão mútua do 2PC). É
independente de rede/gRPC de propósito: assim os testes de unidade
exercitam toda esta lógica sem subir um cluster.

Responsabilidades:
  1. Dados em memória -- um dict com os pares chave/valor JÁ COMMITADOS.
  2. Durabilidade (WAL) -- toda transação grava PREPARE e depois COMMIT ou
     ABORT em um log apêndice no disco ANTES de mexer na memória. Se o
     processo cair, recover() reconstrói o estado exato pelo snapshot + log.
  3. Exclusão mútua -- enquanto uma transação está "preparada" numa chave, a
     chave fica travada; outra transação na mesma chave recebe voto NÃO.
  4. Log de decisões -- guarda o desfecho (COMMIT/ABORT) de cada transação,
     para responder ao protocolo de terminação dos peers.

O 2PC dirige o store pelo trio prepare() / commit() / abort().
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# Operações suportadas.
PUT = "PUT"
UPDATE = "UPDATE"
DELETE = "DELETE"

# Respostas possíveis sobre o desfecho de uma transação (terminação).
DECISION_COMMIT = "COMMIT"
DECISION_ABORT = "ABORT"
DECISION_UNKNOWN = "UNKNOWN"


@dataclass
class PendingOp:
    """Uma operação preparada (fase 1 do 2PC) aguardando a decisão."""
    tx_id: str
    op: str
    key: str
    value: str
    timestamp: int
    prepared_at: float = 0.0     # instante (monotônico) do PREPARE


class KeyValueStore:
    def __init__(self, data_dir: Optional[str] = None, node_name: str = "node") -> None:
        """Com data_dir=None o store fica só em memória (usado nos testes)."""
        self._data: Dict[str, str] = {}
        self._lock = threading.RLock()

        # tx_id -> operação preparada mas ainda sem decisão.
        self._pending: Dict[str, PendingOp] = {}
        # chave -> tx_id que detém o lock daquela chave.
        self._key_locks: Dict[str, str] = {}
        # Carimbo (Lamport) da transação commitada mais recente. Usado na
        # eleição: revela qual réplica tem o estado mais novo.
        self._last_commit_ts: int = 0
        # tx_id -> COMMIT/ABORT. Responde "o que houve com a tx X?" quando um
        # peer roda o protocolo de terminação.
        self._decisions: Dict[str, str] = {}

        self._data_dir = data_dir
        self._wal_path: Optional[str] = None
        self._snapshot_path: Optional[str] = None
        if data_dir is not None:
            os.makedirs(data_dir, exist_ok=True)
            self._wal_path = os.path.join(data_dir, f"{node_name}.wal")
            self._snapshot_path = os.path.join(data_dir, f"{node_name}.snapshot")

    # ------------------------------------------------------------- leituras
    def get(self, key: str) -> Tuple[bool, str]:
        with self._lock:
            if key in self._data:
                return True, self._data[key]
            return False, ""

    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._data

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._data.keys())

    def size(self) -> int:
        with self._lock:
            return len(self._data)

    def items(self) -> List[Tuple[str, str]]:
        with self._lock:
            return list(self._data.items())

    def snapshot_dict(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._data)

    @property
    def last_commit_ts(self) -> int:
        with self._lock:
            return self._last_commit_ts

    # ----------------------------------------------- locks (exclusão mútua)
    def is_locked_by_other(self, key: str, tx_id: str) -> bool:
        with self._lock:
            holder = self._key_locks.get(key)
            return holder is not None and holder != tx_id

    def _acquire_lock(self, key: str, tx_id: str) -> bool:
        # Trava a chave para a transação; falha se outra tx já a detém.
        holder = self._key_locks.get(key)
        if holder is None or holder == tx_id:
            self._key_locks[key] = tx_id
            return True
        return False

    def _release_lock_for_tx(self, tx_id: str) -> None:
        for key in [k for k, owner in self._key_locks.items() if owner == tx_id]:
            del self._key_locks[key]

    # ------------------------------------------------------------ transações
    def prepare(self, tx_id: str, op: str, key: str, value: str, timestamp: int) -> Tuple[bool, str]:
        """Fase 1 do 2PC neste nó. Devolve (votou_sim, motivo).

        Voto SIM significa: chave travada para esta tx, operação válida e
        intenção gravada no WAL — prometo conseguir commitar se mandarem.
        """
        with self._lock:
            # Exclusão mútua: recusa se outra transação detém a chave.
            if not self._acquire_lock(key, tx_id):
                return False, f"key '{key}' locked by another transaction"

            # Validação semântica: UPDATE/DELETE exigem chave existente.
            if op in (UPDATE, DELETE) and key not in self._data:
                self._release_lock_for_tx(tx_id)
                return False, f"key '{key}' does not exist"

            self._pending[tx_id] = PendingOp(tx_id, op, key, value, timestamp,
                                             prepared_at=time.monotonic())
            # Durabilidade ANTES de votar SIM.
            self._append_wal({
                "type": "PREPARE", "tx": tx_id, "op": op,
                "key": key, "value": value, "ts": timestamp,
            })
            return True, "prepared"

    def commit(self, tx_id: str, timestamp: int) -> bool:
        """Fase 2 (commit): torna a operação preparada durável e visível."""
        with self._lock:
            pending = self._pending.get(tx_id)
            if pending is None:
                # Idempotente: commit repetido ou de tx desconhecida.
                return False
            self._append_wal({"type": "COMMIT", "tx": tx_id, "ts": timestamp})
            self._apply(pending)
            self._last_commit_ts = max(self._last_commit_ts, int(timestamp))
            self._decisions[tx_id] = DECISION_COMMIT
            del self._pending[tx_id]
            self._release_lock_for_tx(tx_id)
            return True

    def abort(self, tx_id: str, timestamp: int) -> bool:
        """Fase 2 (abort): descarta a operação preparada e libera o lock."""
        with self._lock:
            self._append_wal({"type": "ABORT", "tx": tx_id, "ts": timestamp})
            self._pending.pop(tx_id, None)
            self._decisions[tx_id] = DECISION_ABORT
            self._release_lock_for_tx(tx_id)
            return True

    # ------------------------------------- suporte ao protocolo de terminação
    def decision_of(self, tx_id: str) -> str:
        """O que este nó sabe sobre o desfecho de uma transação."""
        with self._lock:
            return self._decisions.get(tx_id, DECISION_UNKNOWN)

    def stale_pending(self, max_age: float, now: Optional[float] = None) -> List[PendingOp]:
        """Transações preparadas há mais de max_age segundos SEM decisão.

        São as transações "em dúvida": o coordenador caiu (ou se isolou)
        entre o PREPARE e o COMMIT/ABORT. O dono resolve via terminação
        (pergunta a decisão aos peers).
        """
        if now is None:
            now = time.monotonic()
        with self._lock:
            return [op for op in self._pending.values()
                    if op.prepared_at and (now - op.prepared_at) > max_age]

    def _apply(self, op: PendingOp) -> None:
        # Efetiva a operação no dicionário em memória.
        if op.op in (PUT, UPDATE):
            self._data[op.key] = op.value
        elif op.op == DELETE:
            self._data.pop(op.key, None)

    # -------------------------------------- controle de réplicas / sync
    def replace_all(self, items: Dict[str, str], last_commit_ts: int = 0) -> None:
        """Substitui TODO o conteúdo (transferência de estado no reingresso)."""
        with self._lock:
            self._data = dict(items)
            self._pending.clear()
            self._key_locks.clear()
            self._last_commit_ts = max(self._last_commit_ts, int(last_commit_ts))
            self.take_snapshot()                 # torna o novo estado durável

    # ------------------------------------------------------- persistência
    def _append_wal(self, record: dict) -> None:
        # Acrescenta um registro ao log e força a ida ao disco (fsync).
        if self._wal_path is None:
            return
        with open(self._wal_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def take_snapshot(self) -> None:
        """Persiste o estado atual e zera o WAL (compactação do log)."""
        if self._snapshot_path is None:
            return
        with self._lock:
            # Escreve num .tmp e renomeia: troca atômica, nunca corrompe.
            tmp = self._snapshot_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"format": 2, "data": self._data,
                           "last_commit_ts": self._last_commit_ts}, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._snapshot_path)
            if self._wal_path and os.path.exists(self._wal_path):
                open(self._wal_path, "w").close()

    def recover(self) -> None:
        """Reconstrói o estado pós-queda: snapshot + releitura do WAL."""
        if self._data_dir is None:
            return
        with self._lock:
            self._data.clear()
            self._pending.clear()
            self._key_locks.clear()
            self._decisions.clear()
            self._last_commit_ts = 0

            # 1) Carrega o snapshot (aceita o formato antigo, só o dict).
            if self._snapshot_path and os.path.exists(self._snapshot_path):
                with open(self._snapshot_path, encoding="utf-8") as fh:
                    loaded = json.load(fh)
                if isinstance(loaded, dict) and loaded.get("format") == 2 \
                        and isinstance(loaded.get("data"), dict):
                    self._data = loaded["data"]
                    self._last_commit_ts = int(loaded.get("last_commit_ts", 0))
                else:
                    self._data = loaded

            if not (self._wal_path and os.path.exists(self._wal_path)):
                return

            # 2) Relê o WAL classificando cada transação.
            prepared: Dict[str, PendingOp] = {}
            committed: List[str] = []
            decided = set()
            with open(self._wal_path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    if rec["type"] == "PREPARE":
                        prepared[rec["tx"]] = PendingOp(
                            rec["tx"], rec["op"], rec["key"], rec["value"], rec["ts"])
                    elif rec["type"] == "COMMIT":
                        committed.append(rec["tx"])
                        decided.add(rec["tx"])
                        self._decisions[rec["tx"]] = DECISION_COMMIT
                        self._last_commit_ts = max(self._last_commit_ts,
                                                   int(rec.get("ts", 0)))
                    elif rec["type"] == "ABORT":
                        decided.add(rec["tx"])
                        self._decisions[rec["tx"]] = DECISION_ABORT

            # 3) Reaplica SOMENTE o que teve COMMIT durável.
            for tx in committed:
                if tx in prepared:
                    self._apply(prepared[tx])

            # Transações "em dúvida" (preparadas sem decisão no log) são
            # descartadas com segurança: nunca ficaram visíveis a clientes.

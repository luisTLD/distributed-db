"""Durable key-value store with a write-ahead log and per-key locks.

This module is the heart of every node's local persistence and is completely
independent of gRPC, so it can be unit-tested in isolation.

Responsibilities
----------------
1. **In-memory data** -- a plain ``dict`` holding committed key/value pairs.

2. **Durability (write-ahead log)** -- every transaction first appends a
   ``PREPARE`` record, then either a ``COMMIT`` or an ``ABORT`` record, to an
   append-only log on disk *before* the in-memory state changes. If the process
   crashes, :meth:`recover` rebuilds the exact committed state by replaying the
   snapshot plus the log. This is what makes the 2PC participant crash-safe.

3. **Mutual exclusion (per-key locks)** -- while a transaction is *prepared* on
   a key, that key is locked. A concurrent transaction that wants the same key
   cannot prepare and the coordinator aborts it. This is the distributed
   concurrency-control / mutual-exclusion mechanism.

The store is transaction oriented: the 2PC participant drives it through
``prepare`` / ``commit`` / ``abort``.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

PUT = "PUT"
UPDATE = "UPDATE"
DELETE = "DELETE"

# Durable decisions a node can report about a transaction (termination protocol)
DECISION_COMMIT = "COMMIT"
DECISION_ABORT = "ABORT"
DECISION_UNKNOWN = "UNKNOWN"


@dataclass
class PendingOp:
    tx_id: str
    op: str
    key: str
    value: str
    timestamp: int
    prepared_at: float = 0.0     # monotonic time when PREPARE was accepted


class KeyValueStore:
    def __init__(self, data_dir: Optional[str] = None, node_name: str = "node") -> None:
        """If *data_dir* is None the store is purely in-memory (used by tests)."""
        self._data: Dict[str, str] = {}
        self._lock = threading.RLock()

        # tx_id -> PendingOp that has been prepared but not yet committed/aborted
        self._pending: Dict[str, PendingOp] = {}
        # key -> tx_id that currently holds the lock on that key
        self._key_locks: Dict[str, str] = {}
        # Lamport timestamp of the newest committed transaction. Used by the
        # election winner to detect that a peer has fresher state than its own
        # (replica control: a stale rejoining node must not impose old data).
        self._last_commit_ts: int = 0
        # tx_id -> COMMIT/ABORT. Lets this node answer "what happened to tx X?"
        # when a peer runs the 2PC termination protocol (coordinator crashed
        # between PREPARE and the decision).
        self._decisions: Dict[str, str] = {}

        self._data_dir = data_dir
        self._wal_path: Optional[str] = None
        self._snapshot_path: Optional[str] = None
        if data_dir is not None:
            os.makedirs(data_dir, exist_ok=True)
            self._wal_path = os.path.join(data_dir, f"{node_name}.wal")
            self._snapshot_path = os.path.join(data_dir, f"{node_name}.snapshot")

    # ------------------------------------------------------------------ reads
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

    # ----------------------------------------------------------- locking (mutex)
    def is_locked_by_other(self, key: str, tx_id: str) -> bool:
        with self._lock:
            holder = self._key_locks.get(key)
            return holder is not None and holder != tx_id

    def _acquire_lock(self, key: str, tx_id: str) -> bool:
        holder = self._key_locks.get(key)
        if holder is None or holder == tx_id:
            self._key_locks[key] = tx_id
            return True
        return False

    def _release_lock_for_tx(self, tx_id: str) -> None:
        for key in [k for k, owner in self._key_locks.items() if owner == tx_id]:
            del self._key_locks[key]

    # ------------------------------------------------------------ transactions
    def prepare(self, tx_id: str, op: str, key: str, value: str, timestamp: int) -> Tuple[bool, str]:
        """Phase 1 of 2PC on this node.

        Returns ``(vote_yes, reason)``. A YES vote means: the key was locked for
        this transaction, the operation is valid, and a PREPARE record was made
        durable. The node now promises it *can* commit when told to.
        """
        with self._lock:
            # Concurrency control: refuse if another tx holds the key.
            if not self._acquire_lock(key, tx_id):
                return False, f"key '{key}' locked by another transaction"

            # Semantic validation: UPDATE/DELETE require an existing key.
            if op in (UPDATE, DELETE) and key not in self._data:
                self._release_lock_for_tx(tx_id)
                return False, f"key '{key}' does not exist"

            self._pending[tx_id] = PendingOp(tx_id, op, key, value, timestamp,
                                             prepared_at=time.monotonic())
            self._append_wal({
                "type": "PREPARE", "tx": tx_id, "op": op,
                "key": key, "value": value, "ts": timestamp,
            })
            return True, "prepared"

    def commit(self, tx_id: str, timestamp: int) -> bool:
        """Phase 2 (commit): make the prepared operation durable and visible."""
        with self._lock:
            pending = self._pending.get(tx_id)
            if pending is None:
                # Idempotent: a commit we have already applied (or never saw).
                return False
            self._append_wal({"type": "COMMIT", "tx": tx_id, "ts": timestamp})
            self._apply(pending)
            self._last_commit_ts = max(self._last_commit_ts, int(timestamp))
            self._decisions[tx_id] = DECISION_COMMIT
            del self._pending[tx_id]
            self._release_lock_for_tx(tx_id)
            return True

    def abort(self, tx_id: str, timestamp: int) -> bool:
        """Phase 2 (abort): discard the prepared operation and free the lock."""
        with self._lock:
            self._append_wal({"type": "ABORT", "tx": tx_id, "ts": timestamp})
            self._pending.pop(tx_id, None)
            self._decisions[tx_id] = DECISION_ABORT
            self._release_lock_for_tx(tx_id)
            return True

    # --------------------------------------- termination protocol support
    def decision_of(self, tx_id: str) -> str:
        """What this node knows about a transaction's outcome."""
        with self._lock:
            return self._decisions.get(tx_id, DECISION_UNKNOWN)

    def stale_pending(self, max_age: float, now: Optional[float] = None) -> List[PendingOp]:
        """Prepared transactions stuck without a decision for *max_age* seconds.

        These are 'in-doubt' transactions: the coordinator crashed (or got
        partitioned) between PREPARE and COMMIT/ABORT. The owner node resolves
        them with the termination protocol (ask the peers for the decision).
        """
        if now is None:
            now = time.monotonic()
        with self._lock:
            return [op for op in self._pending.values()
                    if op.prepared_at and (now - op.prepared_at) > max_age]

    def _apply(self, op: PendingOp) -> None:
        if op.op in (PUT, UPDATE):
            self._data[op.key] = op.value
        elif op.op == DELETE:
            self._data.pop(op.key, None)

    # -------------------------------------------------- replica control / sync
    def replace_all(self, items: Dict[str, str], last_commit_ts: int = 0) -> None:
        """Overwrite the whole dataset (used by state transfer on rejoin)."""
        with self._lock:
            self._data = dict(items)
            self._pending.clear()
            self._key_locks.clear()
            self._last_commit_ts = max(self._last_commit_ts, int(last_commit_ts))
            self.take_snapshot()

    # ----------------------------------------------------------- persistence
    def _append_wal(self, record: dict) -> None:
        if self._wal_path is None:
            return
        with open(self._wal_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def take_snapshot(self) -> None:
        """Persist current state and truncate the WAL (log compaction)."""
        if self._snapshot_path is None:
            return
        with self._lock:
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
        """Rebuild committed state after a crash: snapshot + replayed WAL."""
        if self._data_dir is None:
            return
        with self._lock:
            self._data.clear()
            self._pending.clear()
            self._key_locks.clear()
            self._decisions.clear()
            self._last_commit_ts = 0

            if self._snapshot_path and os.path.exists(self._snapshot_path):
                with open(self._snapshot_path, encoding="utf-8") as fh:
                    loaded = json.load(fh)
                if isinstance(loaded, dict) and loaded.get("format") == 2 \
                        and isinstance(loaded.get("data"), dict):
                    self._data = loaded["data"]
                    self._last_commit_ts = int(loaded.get("last_commit_ts", 0))
                else:  # old snapshot format: the whole file is the data dict
                    self._data = loaded

            if not (self._wal_path and os.path.exists(self._wal_path)):
                return

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

            # Replay only transactions that reached a durable COMMIT decision.
            for tx in committed:
                if tx in prepared:
                    self._apply(prepared[tx])

            # In-doubt transactions (prepared, no decision logged) are safely
            # discarded -- the coordinator never received our YES acknowledgement
            # or never decided, so the data was never made visible to clients.

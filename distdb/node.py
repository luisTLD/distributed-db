"""Distributed database node (gRPC server + background coordination).

This module is the *transport layer*: it wires the pure-logic components
(:mod:`distdb.store`, :mod:`distdb.coordinator`, :mod:`distdb.election`,
:mod:`distdb.failure_detector`, :mod:`distdb.lamport`) onto gRPC.

Every node runs the same code. Roles are dynamic:

* the node with the highest id that is alive is the **leader** (it coordinates
  all writes through Two-Phase Commit);
* the others are **replicas / participants** (they store data, vote in 2PC, and
  watch the leader).

Background threads provide fault tolerance:

* ``_heartbeat_loop``  -- pings every peer; updates the failure detector; if the
  leader is unreachable, starts a Bully election.
* ``_monitor_loop``    -- ages out silent peers and, on a replica, syncs state
  from the leader once after (re)joining.

Run a node with::

    python -m distdb.node --id 1
    python -m distdb.node --id 2
    python -m distdb.node --id 3
"""

from __future__ import annotations

import argparse
import threading
import time
from concurrent import futures
from typing import Dict, List, Optional

import grpc

from distdb.generated import database_pb2 as pb2
from distdb.generated import database_pb2_grpc as pb2_grpc

from distdb import config
from distdb.lamport import LamportClock
from distdb.store import KeyValueStore, PUT, UPDATE, DELETE
from distdb.coordinator import TwoPhaseCommit, COMMITTED, termination_decision
from distdb.election import run_election
from distdb.failure_detector import FailureDetector

HEARTBEAT_INTERVAL = 1.0     # seconds between heartbeats
FAILURE_TIMEOUT = 3.0        # mark a peer dead after this much silence
RPC_TIMEOUT = 1.0            # fast-fail timeout for coordination RPCs
TX_RPC_TIMEOUT = 2.5         # timeout for 2PC participant RPCs
MAX_WRITE_RETRIES = 3        # retries after excluding a freshly-failed replica
PENDING_TX_TIMEOUT = 8.0     # in-doubt tx older than this triggers termination

_OP_TO_PB = {PUT: pb2.OP_PUT, UPDATE: pb2.OP_UPDATE, DELETE: pb2.OP_DELETE}
_PB_TO_OP = {v: k for k, v in _OP_TO_PB.items()}


# ---------------------------------------------------------------------------
#  2PC participant proxies (what the coordinator drives)
# ---------------------------------------------------------------------------
class LocalParticipant:
    """The leader's own store, exposed as a 2PC participant."""

    def __init__(self, node_id: int, store: KeyValueStore):
        self.node_id = node_id
        self._store = store

    def prepare(self, tx_id, op, key, value, timestamp):
        return self._store.prepare(tx_id, op, key, value, timestamp)

    def commit(self, tx_id, timestamp):
        return self._store.commit(tx_id, timestamp)

    def abort(self, tx_id, timestamp):
        return self._store.abort(tx_id, timestamp)


class RemoteParticipant:
    """A replica reached over gRPC, exposed as a 2PC participant."""

    def __init__(self, node_id: int, stub):
        self.node_id = node_id
        self._stub = stub

    def prepare(self, tx_id, op, key, value, timestamp):
        resp = self._stub.Prepare(
            pb2.PrepareRequest(tx_id=tx_id, op=_OP_TO_PB[op], key=key,
                               value=value, timestamp=timestamp),
            timeout=TX_RPC_TIMEOUT)
        return resp.vote_commit, resp.reason

    def commit(self, tx_id, timestamp):
        self._stub.Commit(pb2.DecisionRequest(tx_id=tx_id, timestamp=timestamp),
                          timeout=TX_RPC_TIMEOUT)
        return True

    def abort(self, tx_id, timestamp):
        self._stub.Abort(pb2.DecisionRequest(tx_id=tx_id, timestamp=timestamp),
                         timeout=TX_RPC_TIMEOUT)
        return True


# ---------------------------------------------------------------------------
#  Node
# ---------------------------------------------------------------------------
class Node(pb2_grpc.DatabaseServiceServicer):
    def __init__(self, node_id: int, nodes: List[config.NodeInfo], data_dir: str):
        self.node_id = node_id
        self.cluster: Dict[int, config.NodeInfo] = config.cluster_map(nodes)
        self.all_ids = sorted(self.cluster.keys())
        self.peer_ids = [i for i in self.all_ids if i != node_id]
        self.me = self.cluster[node_id]
        # Majority quorum: writes only commit when at least this many
        # participants (leader included) are alive (split-brain protection).
        self.majority = len(self.all_ids) // 2 + 1

        self.clock = LamportClock()
        self.store = KeyValueStore(data_dir=data_dir, node_name=f"node{node_id}")
        self.store.recover()
        self.fd = FailureDetector(self.peer_ids, timeout=FAILURE_TIMEOUT)
        self.tpc = TwoPhaseCommit(self.clock)

        self.leader_id: Optional[int] = None
        self._leader_lock = threading.Lock()
        self._election_lock = threading.Lock()
        self._in_election = False
        self._synced_leader: Optional[int] = None

        self._stub_cache: Dict[int, object] = {}
        self._running = True

        # Leader-side concurrency control (first level of mutual exclusion):
        # writes to the same key are serialized here, so concurrent client
        # requests queue up instead of aborting each other in the 2PC prepare.
        # Striped locks keep memory bounded regardless of how many keys exist.
        self._write_locks = [threading.Lock() for _ in range(64)]

    # ------------------------------------------------------------- helpers
    def is_leader(self) -> bool:
        with self._leader_lock:
            return self.leader_id == self.node_id

    def _set_leader(self, leader_id: int) -> None:
        with self._leader_lock:
            changed = self.leader_id != leader_id
            self.leader_id = leader_id
        if changed:
            self._log(f"leader is now node {leader_id}")

    def leader_address(self) -> str:
        with self._leader_lock:
            lid = self.leader_id
        return self.cluster[lid].address if lid in self.cluster else ""

    def _stub(self, peer_id: int):
        stub = self._stub_cache.get(peer_id)
        if stub is None:
            channel = grpc.insecure_channel(self.cluster[peer_id].address)
            stub = pb2_grpc.DatabaseServiceStub(channel)
            self._stub_cache[peer_id] = stub
        return stub

    def _log(self, msg: str) -> None:
        role = "LEADER" if self.is_leader() else "REPLICA"
        print(f"[node {self.node_id}][{role}][clock {self.clock.value}] {msg}", flush=True)

    # ============================================================ Client API
    def Put(self, request, context):
        return self._client_write(PUT, request.key, request.value, request.timestamp)

    def Update(self, request, context):
        return self._client_write(UPDATE, request.key, request.value, request.timestamp)

    def Delete(self, request, context):
        return self._client_write(DELETE, request.key, "", request.timestamp)

    def _client_write(self, op, key, value, client_ts):
        self.clock.update(client_ts)
        if not self.is_leader():
            # Redirect the client to the current leader.
            return pb2.WriteResponse(status=pb2.NOT_LEADER,
                                     message="not the leader",
                                     timestamp=self.clock.value,
                                     leader_address=self.leader_address())
        res = self._coordinate_write(op, key, value)
        if res.status == COMMITTED:
            return pb2.WriteResponse(status=pb2.OK,
                                     message=f"{op} committed (cohort={res.cohort_size})",
                                     timestamp=res.timestamp)
        status = pb2.KEY_ABSENT if "does not exist" in res.reason else pb2.ABORTED
        return pb2.WriteResponse(status=status,
                                 message=f"{op} aborted: {res.reason}",
                                 timestamp=res.timestamp)

    def _write_lock_for(self, key: str) -> threading.Lock:
        return self._write_locks[hash(key) % len(self._write_locks)]

    def _coordinate_write(self, op, key, value):
        """Drive 2PC across the live cohort, retrying past freshly-dead replicas.

        Mutual exclusion happens at two levels:
        1. here, the leader serializes concurrent writes to the same key
           (striped locks), so simultaneous client requests queue up instead of
           aborting each other;
        2. during PREPARE, every participant takes a per-key lock in its own
           store -- the distributed guarantee that still protects the data even
           across leader changes or duplicated coordinators.
        """
        with self._write_lock_for(key):
            excluded: set = set()
            last_res = None
            for _ in range(MAX_WRITE_RETRIES):
                now = time.monotonic()
                live_ids = [pid for pid in self.peer_ids
                            if self.fd.is_alive(pid, now) and pid not in excluded]
                participants = [LocalParticipant(self.node_id, self.store)]
                for pid in live_ids:
                    participants.append(RemoteParticipant(pid, self._stub(pid)))

                res = self._tpc_with_logging(participants, op, key, value)
                last_res = res
                for pid in res.failed_nodes:
                    self.fd.record_dead(pid)
                    excluded.add(pid)
                if res.status == COMMITTED:
                    return res
                # Retry only if the abort was caused by a replica failure (not a
                # semantic NO like "key does not exist").
                if not res.failed_nodes:
                    return res
            return last_res

    def _tpc_with_logging(self, participants, op, key, value):
        res = self.tpc.execute(participants, op, key, value,
                               min_participants=self.majority)
        verb = "COMMIT" if res.status == COMMITTED else "ABORT"
        self._log(f"2PC {verb} tx={res.tx_id} {op} {key}={value!r} "
                  f"votes={res.yes_votes}/{res.cohort_size} failed={res.failed_nodes}")
        return res

    # ---- reads (any node can serve committed data) ----
    def Get(self, request, context):
        ts = self.clock.update(request.timestamp)
        found, value = self.store.get(request.key)
        return pb2.GetResponse(status=pb2.OK, found=found, value=value, timestamp=ts)

    def Exists(self, request, context):
        ts = self.clock.update(request.timestamp)
        return pb2.ExistsResponse(exists=self.store.exists(request.key), timestamp=ts)

    def ListKeys(self, request, context):
        ts = self.clock.update(request.timestamp)
        return pb2.ListKeysResponse(keys=self.store.keys(), timestamp=ts)

    def Size(self, request, context):
        ts = self.clock.update(request.timestamp)
        return pb2.SizeResponse(size=self.store.size(), timestamp=ts)

    def GetAll(self, request, context):
        ts = self.clock.update(request.timestamp)
        items = [pb2.KeyValue(key=k, value=v) for k, v in self.store.items()]
        return pb2.GetAllResponse(items=items, timestamp=ts)

    # ========================================================= 2PC participant
    def Prepare(self, request, context):
        ts = self.clock.update(request.timestamp)
        op = _PB_TO_OP[request.op]
        vote, reason = self.store.prepare(request.tx_id, op, request.key,
                                          request.value, ts)
        self._log(f"PREPARE tx={request.tx_id} {op} {request.key} -> "
                  f"{'YES' if vote else 'NO'} ({reason})")
        return pb2.VoteResponse(tx_id=request.tx_id, vote_commit=vote,
                                reason=reason, timestamp=self.clock.value)

    def Commit(self, request, context):
        ts = self.clock.update(request.timestamp)
        ok = self.store.commit(request.tx_id, ts)
        self._log(f"COMMIT tx={request.tx_id} applied={ok}")
        return pb2.AckResponse(ack=ok, timestamp=self.clock.value)

    def Abort(self, request, context):
        ts = self.clock.update(request.timestamp)
        self.store.abort(request.tx_id, ts)
        self._log(f"ABORT tx={request.tx_id}")
        return pb2.AckResponse(ack=True, timestamp=self.clock.value)

    def QueryDecision(self, request, context):
        # Termination protocol: a peer stuck with an in-doubt transaction asks
        # what we know about it (we answer from our durable decision log).
        self.clock.update(request.timestamp)
        decision = self.store.decision_of(request.tx_id)
        return pb2.DecisionInfo(tx_id=request.tx_id, decision=decision,
                                timestamp=self.clock.value)

    # ========================================================== Coordination
    def Heartbeat(self, request, context):
        ts = self.clock.update(request.timestamp)
        self.fd.record_alive(request.node_id, time.monotonic())
        with self._leader_lock:
            my_leader = self.leader_id if self.leader_id is not None else -1
        return pb2.HeartbeatResponse(alive=True, node_id=self.node_id,
                                     leader_id=my_leader, timestamp=ts)

    def Election(self, request, context):
        # A lower-id node started an election. Answer (I'm alive) and, per Bully,
        # start my own election because I outrank the sender.
        ts = self.clock.update(request.timestamp)
        self._log(f"received ELECTION from node {request.node_id}; taking over")
        threading.Thread(target=self.start_election, daemon=True).start()
        return pb2.ElectionResponse(alive=True, node_id=self.node_id, timestamp=ts)

    def Announce(self, request, context):
        ts = self.clock.update(request.timestamp)
        self._set_leader(request.leader_id)
        return pb2.AckResponse(ack=True, timestamp=ts)

    def WhoIsLeader(self, request, context):
        self.clock.update(request.timestamp)
        with self._leader_lock:
            lid = self.leader_id if self.leader_id is not None else -1
        return pb2.LeaderInfo(leader_id=lid, leader_address=self.leader_address(),
                              timestamp=self.clock.value)

    # ============================================================== Recovery
    def SyncState(self, request, context):
        ts = self.clock.update(request.timestamp)
        items = [pb2.KeyValue(key=k, value=v) for k, v in self.store.items()]
        self._log(f"SyncState -> node {request.node_id} ({len(items)} keys)")
        return pb2.SyncResponse(items=items, timestamp=ts,
                                last_commit_ts=self.store.last_commit_ts)

    # ============================================================ Background
    def start(self):
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        # Kick off an initial election so the cluster converges on a leader.
        threading.Thread(target=self._bootstrap, daemon=True).start()

    def _bootstrap(self):
        time.sleep(HEARTBEAT_INTERVAL * 1.5)  # let heartbeats discover peers
        with self._leader_lock:
            known = self.leader_id
        if known is None:
            self.start_election()

    def _heartbeat_loop(self):
        while self._running:
            for pid in self.peer_ids:
                try:
                    ts = self.clock.tick()
                    with self._leader_lock:
                        lid = self.leader_id if self.leader_id is not None else -1
                    resp = self._stub(pid).Heartbeat(
                        pb2.HeartbeatRequest(node_id=self.node_id, leader_id=lid,
                                             timestamp=ts),
                        timeout=RPC_TIMEOUT)
                    self.clock.update(resp.timestamp)
                    self.fd.record_alive(pid, time.monotonic())
                    # Learn the leader from a peer if we don't have one.
                    if resp.leader_id > 0:
                        with self._leader_lock:
                            unknown = self.leader_id is None
                        if unknown:
                            self._set_leader(resp.leader_id)
                except grpc.RpcError:
                    self.fd.record_dead(pid)
            time.sleep(HEARTBEAT_INTERVAL)

    def _monitor_loop(self):
        while self._running:
            now = time.monotonic()
            self.fd.evaluate(now)
            with self._leader_lock:
                lid = self.leader_id
            # If the leader is gone, elect a new one.
            if lid is not None and lid != self.node_id and not self.fd.is_alive(lid, now):
                self._log(f"leader {lid} appears down -> starting election")
                self._set_leader_none()
                self.start_election()
            else:
                # Replica catch-up: sync once from a (new) leader.
                if lid is not None and lid != self.node_id and self._synced_leader != lid:
                    self._sync_from_leader(lid)
            # 2PC termination protocol: resolve in-doubt transactions whose
            # coordinator never sent a decision (it crashed mid-protocol).
            self._resolve_stale_transactions()
            time.sleep(HEARTBEAT_INTERVAL)

    def _resolve_stale_transactions(self):
        """Unblock transactions stuck between PREPARE and the decision.

        Classic 2PC blocks forever if the coordinator dies after PREPARE: the
        participants hold the key locks and cannot decide alone. We resolve it
        cooperatively: ask every reachable peer what it knows about the
        transaction (``QueryDecision``). If anyone saw COMMIT we commit too;
        otherwise we abort (presumed abort) and release the locks.
        """
        for op in self.store.stale_pending(PENDING_TX_TIMEOUT):
            decisions = []
            for pid in self.peer_ids:
                try:
                    ts = self.clock.tick()
                    resp = self._stub(pid).QueryDecision(
                        pb2.DecisionQuery(tx_id=op.tx_id, node_id=self.node_id,
                                          timestamp=ts),
                        timeout=RPC_TIMEOUT)
                    self.clock.update(resp.timestamp)
                    decisions.append(resp.decision)
                except grpc.RpcError:
                    continue
            outcome = termination_decision(decisions)
            ts = self.clock.tick()
            if outcome == "COMMIT":
                self.store.commit(op.tx_id, ts)
            else:
                self.store.abort(op.tx_id, ts)
            self._log(f"termination protocol: in-doubt tx={op.tx_id} "
                      f"({op.op} {op.key}) resolved -> {outcome} "
                      f"(peer answers: {decisions or 'none'})")

    def _set_leader_none(self):
        with self._leader_lock:
            self.leader_id = None

    # ------------------------------------------------------------ election
    def start_election(self):
        with self._election_lock:
            if self._in_election:
                return
            self._in_election = True
        try:
            self._log("starting Bully election")
            became_leader, answered = run_election(
                self.node_id, self.all_ids, send_election=self._send_election)
            if became_leader:
                # Replica control: before taking over, make sure we are not
                # imposing stale data on the cluster. A node that rejoined
                # after being down may win the election (highest id) while a
                # peer holds newer committed state -- adopt that state first.
                self._adopt_freshest_state()
                self._set_leader(self.node_id)
                self._announce_leadership()
                self._log("won election -> I am the leader")
            else:
                self._log(f"higher node(s) {answered} alive; awaiting announcement")
        finally:
            with self._election_lock:
                self._in_election = False

    def _send_election(self, peer_id: int) -> bool:
        try:
            ts = self.clock.tick()
            resp = self._stub(peer_id).Election(
                pb2.ElectionRequest(node_id=self.node_id, timestamp=ts),
                timeout=RPC_TIMEOUT)
            self.clock.update(resp.timestamp)
            return resp.alive
        except grpc.RpcError:
            self.fd.record_dead(peer_id)
            return False

    def _announce_leadership(self):
        for pid in self.peer_ids:
            try:
                ts = self.clock.tick()
                self._stub(pid).Announce(
                    pb2.CoordinatorMsg(leader_id=self.node_id, timestamp=ts),
                    timeout=RPC_TIMEOUT)
            except grpc.RpcError:
                self.fd.record_dead(pid)

    # ------------------------------------------------------------ state sync
    def _sync_from_leader(self, leader_id: int):
        try:
            ts = self.clock.tick()
            resp = self._stub(leader_id).SyncState(
                pb2.SyncRequest(node_id=self.node_id, timestamp=ts),
                timeout=TX_RPC_TIMEOUT)
            self.clock.update(resp.timestamp)
            self.store.replace_all({kv.key: kv.value for kv in resp.items},
                                   resp.last_commit_ts)
            self._synced_leader = leader_id
            self._log(f"synced {len(resp.items)} keys from leader {leader_id}")
        except grpc.RpcError as exc:
            self._log(f"state sync from leader {leader_id} failed: {exc.code()}")

    def _adopt_freshest_state(self):
        """Pull state from any live peer with a newer committed transaction.

        Called by the election winner *before* announcing leadership. Because
        writes are replicated synchronously to every live node, any node that
        stayed up has the complete committed state; comparing ``last_commit_ts``
        (Lamport) tells us whether a peer saw commits we missed while down.
        """
        now = time.monotonic()
        best_ts = self.store.last_commit_ts
        best_items, best_peer = None, None
        for pid in self.peer_ids:
            if not self.fd.is_alive(pid, now):
                continue
            try:
                ts = self.clock.tick()
                resp = self._stub(pid).SyncState(
                    pb2.SyncRequest(node_id=self.node_id, timestamp=ts),
                    timeout=TX_RPC_TIMEOUT)
                self.clock.update(resp.timestamp)
                if resp.last_commit_ts > best_ts:
                    best_ts = resp.last_commit_ts
                    best_items = {kv.key: kv.value for kv in resp.items}
                    best_peer = pid
            except grpc.RpcError:
                self.fd.record_dead(pid)
        if best_items is not None:
            self.store.replace_all(best_items, best_ts)
            self._log(f"adopted fresher state from node {best_peer} "
                      f"({len(best_items)} keys, last_commit_ts={best_ts})")


def serve():
    parser = argparse.ArgumentParser(description="Distributed DB node")
    parser.add_argument("--id", type=int, required=True, help="this node's id")
    parser.add_argument("--peers", type=str, default=None,
                        help='cluster spec, e.g. "1=127.0.0.1:50051,2=127.0.0.1:50052"')
    parser.add_argument("--data-dir", type=str, default="data",
                        help="directory for WAL/snapshot files")
    args = parser.parse_args()

    nodes = config.parse_peers(args.peers) if args.peers else config.DEFAULT_CLUSTER
    node = Node(args.id, nodes, data_dir=args.data_dir)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_DatabaseServiceServicer_to_server(node, server)
    server.add_insecure_port(f"[::]:{node.me.port}")
    server.start()
    node._log(f"listening on {node.me.address}")
    node.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node._running = False
        server.stop(0)


if __name__ == "__main__":
    serve()

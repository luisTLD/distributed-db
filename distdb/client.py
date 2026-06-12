"""Cliente de linha de comando do banco distribuído.

A porta de entrada do usuário: sem ele, só daria para falar com o cluster
escrevendo código gRPC na mão. Ele esconde a complexidade do cluster —
descobre o líder, segue redirecionamentos e sobrevive a nós fora do ar.

É tolerante a falhas no caminho de leitura e de escrita:
  * mantém conexão com todos os nós;
  * ESCRITAS precisam ir ao líder — se contatar uma réplica, ela responde
    NOT_LEADER com o endereço certo e o cliente refaz lá (redirecionamento);
  * se um nó está fora do ar, tenta o próximo (failover);
  * LEITURAS podem ser atendidas por qualquer nó.

Mantém o próprio relógio de Lamport, ordenando causalmente suas operações
com as dos servidores.

Uso:
    python -m distdb.client                 # modo interativo (prompt db>)
    python -m distdb.client --demo          # demonstração roteirizada
    python -m distdb.client put user Luis   # comando único
    python -m distdb.client get user
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

import grpc

from distdb.generated import database_pb2 as pb2
from distdb.generated import database_pb2_grpc as pb2_grpc

from distdb import config
from distdb.lamport import LamportClock


class ClusterClient:
    def __init__(self, nodes: List[config.NodeInfo]):
        self.nodes = nodes
        self.clock = LamportClock()
        # Um stub gRPC por nó, criado uma única vez.
        self._stubs = {n.node_id: pb2_grpc.DatabaseServiceStub(
            grpc.insecure_channel(n.address)) for n in nodes}
        self._addr_stub = {n.address: self._stubs[n.node_id] for n in nodes}
        self._leader_id: Optional[int] = None    # último líder conhecido

    # ------------------------------------------------------------- auxiliares
    def _order(self) -> List[int]:
        """Ordem de tentativa: líder conhecido primeiro, depois os demais."""
        ids = [n.node_id for n in self.nodes]
        if self._leader_id in ids:
            ids = [self._leader_id] + [i for i in ids if i != self._leader_id]
        return ids

    def _stub_for_address(self, address: str):
        return self._addr_stub.get(address)

    # --------------------------------------------------------------- escritas
    def _write(self, kind: str, key: str, value: str = "") -> str:
        # Tenta cada nó na ordem; dentro de um nó, segue redirecionamentos.
        last_err = "no nodes reachable"
        for node_id in self._order():
            stub = self._stubs[node_id]
            current_id = node_id          # id do nó que `stub` aponta agora
            for _ in range(len(self.nodes) + 1):
                ts = self.clock.tick()
                try:
                    if kind == "delete":
                        resp = stub.Delete(pb2.DeleteRequest(key=key, timestamp=ts), timeout=5)
                    else:
                        req = pb2.WriteRequest(key=key, value=value, timestamp=ts)
                        resp = (stub.Put(req, timeout=5) if kind == "put"
                                else stub.Update(req, timeout=5))
                    self.clock.update(resp.timestamp)
                    if resp.status == pb2.NOT_LEADER:
                        # Não era o líder: sigo o redirecionamento informado.
                        nxt = self._stub_for_address(resp.leader_address)
                        if nxt is None or nxt is stub:
                            break        # líder desconhecido: tenta outro nó
                        stub = nxt
                        current_id = self._id_for_address(resp.leader_address)
                        self._leader_id = current_id
                        continue
                    # Sucesso: memoriza quem de fato atendeu como líder.
                    self._leader_id = current_id
                    return resp.message
                except grpc.RpcError as exc:
                    last_err = f"node {current_id} unreachable ({exc.code().name})"
                    break  # failover: próximo nó da lista
        return f"ERROR: {last_err}"

    def _id_for_address(self, address: str) -> Optional[int]:
        for n in self.nodes:
            if n.address == address:
                return n.node_id
        return None

    def put(self, key, value):    return self._write("put", key, value)
    def update(self, key, value): return self._write("update", key, value)
    def delete(self, key):        return self._write("delete", key)

    # --------------------------------------------------------------- leituras
    def _read(self, fn):
        # Leituras valem em qualquer nó: tenta na ordem até um responder.
        last_err = "no nodes reachable"
        for node_id in self._order():
            try:
                return fn(self._stubs[node_id])
            except grpc.RpcError as exc:
                last_err = f"node {node_id} unreachable ({exc.code().name})"
        raise RuntimeError(last_err)

    def get(self, key) -> str:
        def fn(stub):
            ts = self.clock.tick()
            r = stub.Get(pb2.GetRequest(key=key, timestamp=ts), timeout=5)
            self.clock.update(r.timestamp)
            return r.value if r.found else "(not found)"
        return self._read(fn)

    def exists(self, key) -> bool:
        def fn(stub):
            ts = self.clock.tick()
            r = stub.Exists(pb2.KeyRequest(key=key, timestamp=ts), timeout=5)
            self.clock.update(r.timestamp)
            return r.exists
        return self._read(fn)

    def list_keys(self) -> List[str]:
        def fn(stub):
            ts = self.clock.tick()
            r = stub.ListKeys(pb2.EmptyRequest(timestamp=ts), timeout=5)
            self.clock.update(r.timestamp)
            return list(r.keys)
        return self._read(fn)

    def size(self) -> int:
        def fn(stub):
            ts = self.clock.tick()
            r = stub.Size(pb2.EmptyRequest(timestamp=ts), timeout=5)
            self.clock.update(r.timestamp)
            return r.size
        return self._read(fn)

    def get_all(self):
        def fn(stub):
            ts = self.clock.tick()
            r = stub.GetAll(pb2.EmptyRequest(timestamp=ts), timeout=5)
            self.clock.update(r.timestamp)
            return [(kv.key, kv.value) for kv in r.items]
        return self._read(fn)

    def who_is_leader(self) -> str:
        def fn(stub):
            ts = self.clock.tick()
            r = stub.WhoIsLeader(pb2.EmptyRequest(timestamp=ts), timeout=5)
            self.clock.update(r.timestamp)
            return f"leader = node {r.leader_id} ({r.leader_address})"
        return self._read(fn)


# ------------------------------------------------------------ interface (UI)
HELP = """commands:
  put <key> <value>     store / overwrite a key (2PC across replicas)
  update <key> <value>  update an existing key
  delete <key>          delete a key
  get <key>             read a key
  exists <key>          check if a key exists
  list                  list all keys
  size                  number of keys
  all                   dump all key/value pairs
  leader                show the current leader
  help                  show this help
  quit                  exit
"""


def run_command(client: ClusterClient, parts: List[str]) -> bool:
    # Interpreta UMA linha de comando; devolve False para encerrar.
    cmd = parts[0].lower()
    try:
        if cmd in ("quit", "exit"):
            return False
        elif cmd == "help":
            print(HELP)
        elif cmd == "put" and len(parts) >= 3:
            print(client.put(parts[1], " ".join(parts[2:])))
        elif cmd == "update" and len(parts) >= 3:
            print(client.update(parts[1], " ".join(parts[2:])))
        elif cmd == "delete" and len(parts) == 2:
            print(client.delete(parts[1]))
        elif cmd == "get" and len(parts) == 2:
            print(client.get(parts[1]))
        elif cmd == "exists" and len(parts) == 2:
            print(client.exists(parts[1]))
        elif cmd == "list":
            print(client.list_keys())
        elif cmd == "size":
            print(client.size())
        elif cmd == "all":
            for k, v in client.get_all():
                print(f"  {k} = {v}")
        elif cmd == "leader":
            print(client.who_is_leader())
        else:
            print("unknown / malformed command; type 'help'")
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
    return True


def interactive(client: ClusterClient):
    # Prompt db> em loop até quit/Ctrl+C.
    print("distributed-db client. type 'help' for commands.")
    print(client.who_is_leader())
    while True:
        try:
            line = input("db> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        if not run_command(client, line.split()):
            break


def demo(client: ClusterClient):
    # Sequência pronta de operações — útil na apresentação.
    print("== scripted demo ==")
    print(client.who_is_leader())
    print("put user Luis       ->", client.put("user", "Luis"))
    print("put lang Python      ->", client.put("lang", "Python"))
    print("get user             ->", client.get("user"))
    print("update user Carlos   ->", client.update("user", "Carlos"))
    print("get user             ->", client.get("user"))
    print("exists lang          ->", client.exists("lang"))
    print("update missing X     ->", client.update("missing", "X"))  # aborta
    print("list                 ->", client.list_keys())
    print("size                 ->", client.size())
    print("delete lang          ->", client.delete("lang"))
    print("all                  ->", client.get_all())


def main():
    parser = argparse.ArgumentParser(description="Distributed DB client")
    parser.add_argument("--peers", type=str, default=None)
    parser.add_argument("--demo", action="store_true", help="roda a demo roteirizada")
    parser.add_argument("command", nargs="*", help="comando único (ex.: put k v)")
    args = parser.parse_args()

    nodes = config.parse_peers(args.peers) if args.peers else config.DEFAULT_CLUSTER
    client = ClusterClient(nodes)

    if args.demo:
        demo(client)
    elif args.command:
        run_command(client, args.command)
    else:
        interactive(client)


if __name__ == "__main__":
    main()

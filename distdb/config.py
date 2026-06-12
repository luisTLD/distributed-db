"""Topologia do cluster: ids e endereços dos nós.

Todos os módulos precisam saber quem são os nós do cluster (heartbeat,
eleição, 2PC e o cliente dependem disso); este arquivo centraliza essa
informação e o parsing da opção --peers.

Define NodeInfo (id + host + porta), o cluster padrão de 3 nós em localhost
(para testes na mesma máquina) e o parse_peers() que lê a topologia da
linha de comando — é assim que o sistema roda em várias máquinas da rede
local (basta trocar 127.0.0.1 pelos IPs reais).

Os ids precisam ser únicos: a eleição Bully usa "maior id vivo vence".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class NodeInfo:
    node_id: int
    host: str
    port: int

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


# Cluster padrão: 3 nós em localhost. O nó 3 nasce líder natural (maior id).
DEFAULT_CLUSTER: List[NodeInfo] = [
    NodeInfo(1, "127.0.0.1", 50051),
    NodeInfo(2, "127.0.0.1", 50052),
    NodeInfo(3, "127.0.0.1", 50053),
]


def parse_peers(spec: str) -> List[NodeInfo]:
    """Converte "1=127.0.0.1:50051,2=127.0.0.1:50052" em [NodeInfo, ...]."""
    nodes: List[NodeInfo] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        ident, addr = part.split("=")
        host, port = addr.rsplit(":", 1)
        nodes.append(NodeInfo(int(ident), host, int(port)))
    return nodes


def cluster_map(nodes: List[NodeInfo]) -> Dict[int, NodeInfo]:
    """Indexa a lista de nós por id (acesso rápido: cluster[id])."""
    return {n.node_id: n for n in nodes}

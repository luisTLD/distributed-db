"""Cluster configuration.

A node is identified by a small integer ``id`` and reachable at ``host:port``.
The Bully election uses the integer id (the highest live id wins), so ids must
be unique across the cluster.

The default cluster has three nodes running on localhost. Override it from the
command line (``--peers``) or by editing :data:`DEFAULT_CLUSTER` to run the
nodes on different machines of a local network (just replace ``127.0.0.1`` with
each machine's LAN IP).
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


# Default 3-node cluster (ids 1, 2, 3). Node 3 starts as the natural leader
# because it has the highest id.
DEFAULT_CLUSTER: List[NodeInfo] = [
    NodeInfo(1, "127.0.0.1", 50051),
    NodeInfo(2, "127.0.0.1", 50052),
    NodeInfo(3, "127.0.0.1", 50053),
]


def parse_peers(spec: str) -> List[NodeInfo]:
    """Parse a cluster spec like ``"1=127.0.0.1:50051,2=127.0.0.1:50052"``."""
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
    return {n.node_id: n for n in nodes}

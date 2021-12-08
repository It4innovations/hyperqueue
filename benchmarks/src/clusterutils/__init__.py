import dataclasses

from .node_list import NodeList


@dataclasses.dataclass(frozen=True)
class ClusterInfo:
    node_list: NodeList
    monitor_nodes: bool = False

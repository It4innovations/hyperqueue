import dataclasses
from pathlib import Path

from .node_list import NodeList


@dataclasses.dataclass(frozen=True)
class ClusterInfo:
    workdir: Path
    node_list: NodeList
    monitor_nodes: bool = False

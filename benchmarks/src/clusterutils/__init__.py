import dataclasses
from pathlib import Path

from .node_list import NodeList


@dataclasses.dataclass
class ClusterInfo:
    workdir: Path
    node_list: NodeList
    monitor_nodes: bool = False

    def __post_init__(self):
        self.workdir = self.workdir.absolute()

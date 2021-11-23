from pathlib import Path
from typing import List

import dataclasses

from ..cluster import ClusterInfo
from ..cluster.cluster_helper import ClusterHelper, StartProcessArgs


@dataclasses.dataclass
class HqClusterInfo:
    cluster: ClusterInfo
    binary: Path
    worker_count: int

    def __post_init__(self):
        self.binary = self.binary.absolute()


class HqEnvironment:
    def __init__(self, info: HqClusterInfo):
        self.info = info
        self.cluster = ClusterHelper(self.info.cluster)
        self.binary_path = self.info.binary.absolute()
        assert self.binary_path.is_file()

        self.server_dir = self.info.cluster.workdir / "hq"

        node_list = self.info.cluster.node_list
        self.nodes = self.info.cluster.node_list.resolve()
        assert self.nodes

        self.worker_nodes = self.nodes if node_list.is_localhost() else self.nodes[1:]
        assert self.worker_nodes

    def start(self):
        self.start_server()
        self.start_workers(self.worker_nodes[:self.info.worker_count])
        self.cluster.start_monitoring(self.cluster.active_nodes)
        self.cluster.commit()

    def start_server(self):
        self.cluster.start_processes([StartProcessArgs(
            args=self._shared_args() + ["server", "start"],
            host=self.nodes[0],
            name="server",
            workdir=self.server_dir / "server"
        )])

    def start_workers(self, nodes: List[str]):
        worker_processes = [
            StartProcessArgs(
                args=self._shared_args() + ["worker", "start"],
                host=node,
                name=f"worker-{index}",
                workdir=self.server_dir / f"worker-{index}"
            )
            for (index, node) in enumerate(nodes)
        ]

        self.cluster.start_processes(worker_processes)

    def stop(self):
        self.cluster.stop(True)

    def _shared_args(self) -> List[str]:
        return [self.binary_path, "--server-dir", self.server_dir]

from pathlib import Path
from typing import List

import dataclasses

from ..utils import wait_until
from ..cluster import ClusterInfo
from ..cluster.cluster_helper import ClusterHelper, StartProcessArgs
from ..cluster.profiler import NativeProfiler


class ProfileMode:
    def __init__(self, server=False, workers=False):
        self.server = server
        self.workers = workers


@dataclasses.dataclass
class HqClusterInfo:
    cluster: ClusterInfo
    binary: Path
    worker_count: int
    profile_mode: ProfileMode = dataclasses.field(default_factory=lambda: ProfileMode())

    def __post_init__(self):
        self.binary = self.binary.resolve()


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
        self._wait_for_server_start()

        self.start_workers(self.worker_nodes[:self.info.worker_count])
        self.cluster.start_monitoring(self.cluster.active_nodes)
        self.cluster.commit()

    def start_server(self):
        workdir = self.server_dir / "server"
        args = self._profile_args(
            self._shared_args() + ["server", "start"],
            self.info.profile_mode.server,
            workdir / "flamegraph.svg"
        )

        self.cluster.start_processes([StartProcessArgs(
            args=args,
            host=self.nodes[0],
            name="server",
            workdir=workdir
        )])

    def start_workers(self, nodes: List[str]):
        worker_processes = []
        for (index, node) in enumerate(nodes):
            workdir = self.server_dir / f"worker-{index}"
            args = self._profile_args(
                self._shared_args() + ["worker", "start"],
                self.info.profile_mode.workers,
                workdir / "flamegraph.svg"
            )
            worker_processes.append(StartProcessArgs(
                args=args,
                host=node,
                name=f"worker-{index}",
                workdir=workdir
            ))

        self.cluster.start_processes(worker_processes)

    def stop(self):
        self.cluster.stop(use_sigint=True)

    def _profile_args(self, args: List[str], profile: bool, path: Path) -> List[str]:
        if not profile:
            return args
        profiler = NativeProfiler()
        assert profiler.is_available()
        return profiler.profile(args, path)

    def _shared_args(self) -> List[str]:
        return [str(self.binary_path), "--server-dir", str(self.server_dir)]

    def _wait_for_server_start(self):
        wait_until(lambda: (self.server_dir / "hq-current" / "access.json").is_file())

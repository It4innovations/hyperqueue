import json
import logging
import subprocess
from pathlib import Path
from typing import List, Dict, Any

import dataclasses

from . import Environment
from ..clusterutils import ClusterInfo
from ..clusterutils.cluster_helper import ClusterHelper, StartProcessArgs
from ..clusterutils.profiler import NativeProfiler
from ..utils.timing import wait_until


class ProfileMode:
    def __init__(self, server=False, workers=False, frequency=99):
        self.server = server
        self.workers = workers
        self.frequency = frequency


@dataclasses.dataclass
class HqClusterInfo:
    cluster: ClusterInfo
    binary: Path
    worker_count: int
    profile_mode: ProfileMode = dataclasses.field(default_factory=lambda: ProfileMode())

    def __post_init__(self):
        self.binary = self.binary.resolve()


class HqEnvironment(Environment):
    def __init__(self, info: HqClusterInfo):
        self.info = info
        self.cluster = ClusterHelper(self.info.cluster)
        self.binary_path = self.info.binary.absolute()
        assert self.binary_path.is_file()

        self.server_dir = self.info.cluster.workdir / "hq"

        node_list = self.info.cluster.node_list
        self.nodes = self.info.cluster.node_list.resolve()
        assert self.nodes

        assert self.info.worker_count > 0
        self.worker_nodes = self.nodes if node_list.is_localhost() else self.nodes[1:]
        if len(self.worker_nodes) < self.info.worker_count:
            raise Exception(f"Asked for {self.info.worker_count} workers, but only {len(self.worker_nodes)} "
                            "node(s) are available")

        self.state = "initial"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def name(self) -> str:
        return "hq"

    def create_environment_key(self) -> Dict[str, Any]:
        return {
            "worker-count": self.info.worker_count
        }

    def create_metadata_key(self) -> Dict[str, Any]:
        return {
            "binary": self.info.binary
        }

    def start(self):
        assert self.state == "initial"

        logging.info("Starting HQ server")
        self.start_server()
        self._wait_for_server_start()
        logging.info("HQ server started")

        self.start_workers(self.worker_nodes[:self.info.worker_count])
        self.cluster.start_monitoring(self.cluster.active_nodes, observe_processes=True)
        self.cluster.commit()

        self._wait_for_workers(self.info.worker_count)
        logging.info(f"{self.info.worker_count} HQ worker(s) connected")

        self.state = "started"

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
        assert self.state == "started"
        self.cluster.stop(use_sigint=True)
        self.state = "stopped"

    def submit(self, args: List[str]):
        return subprocess.run(self._shared_args() + args, check=True)

    def _profile_args(self, args: List[str], profile: bool, path: Path) -> List[str]:
        if not profile:
            return args
        profiler = NativeProfiler()
        assert profiler.is_available()
        return profiler.profile(args, path, frequency=self.info.profile_mode.frequency)

    def _shared_args(self) -> List[str]:
        return [str(self.binary_path), "--server-dir", str(self.server_dir)]

    def _wait_for_server_start(self):
        wait_until(lambda: (self.server_dir / "hq-current" / "access.json").is_file())

    def _wait_for_workers(self, count: int):
        def get_worker_count():
            output = subprocess.check_output(self._shared_args() + ["--output-type", "json", "worker", "list"])
            return len(json.loads(output)) == count

        wait_until(lambda: get_worker_count())

import json
import logging
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import List, Optional, Dict

import dataclasses

from . import Environment
from ..clusterutils import ClusterInfo
from ..clusterutils.cluster_helper import ClusterHelper, StartProcessArgs
from ..clusterutils.profiler import NativeProfiler
from ..utils import check_file_exists
from ..utils.timing import wait_until


class ProfileMode:
    def __init__(self, server=False, workers=False, frequency=99):
        self.server = server
        self.workers = workers
        self.frequency = frequency


@dataclasses.dataclass(frozen=True)
class HqWorkerConfig:
    cpus: Optional[int] = None
    node: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class HqClusterInfo:
    cluster: ClusterInfo
    binary: Path
    workers: List[HqWorkerConfig]
    profile_mode: ProfileMode = dataclasses.field(default_factory=lambda: ProfileMode())


def assign_workers(
        workers: List[HqWorkerConfig],
        nodes: List[str]
) -> Dict[str, List[HqWorkerConfig]]:
    round_robin_node = 1

    node_assignments = defaultdict(list)
    for (index, worker) in enumerate(workers):
        node = worker.node
        if node is not None:
            if not (0 <= node < len(nodes)):
                raise Exception(f"Invalid node assignment. Worker {index} wants to be on node "
                                f"{node}, but there are only {len(nodes)} nodes")
        else:
            node = round_robin_node
            round_robin_node = (round_robin_node + 1) % len(nodes)
            if round_robin_node == 0:
                logging.warning(
                    f"There are more workers ({len(workers)}) than worker nodes ({len(nodes) - 1})")
        node_assignments[nodes[node]].append(worker)
    return node_assignments


class HqEnvironment(Environment):
    def __init__(self, info: HqClusterInfo):
        self.info = info
        self.cluster = ClusterHelper(self.info.cluster)
        self.binary_path = self.info.binary.absolute()
        check_file_exists(self.binary_path)

        self.server_dir = self.info.cluster.workdir / "hq"

        self.nodes = self.info.cluster.node_list.resolve()
        assert self.nodes

        self.worker_count = len(self.info.workers)
        assert self.worker_count > 0
        self.worker_assignment = assign_workers(self.info.workers, self.nodes)
        logging.debug(f"Worker assignment: {self.worker_assignment}")

        self.state = "initial"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def workdir(self) -> Path:
        return self.info.cluster.workdir

    def start(self):
        assert self.state == "initial"

        logging.info("Starting HQ server")
        self.start_server()
        self._wait_for_server_start()
        logging.info("HQ server started")

        self.start_workers(self.worker_assignment)
        self.cluster.start_monitoring(self.cluster.active_nodes, observe_processes=True)
        self.cluster.commit()

        self._wait_for_workers(self.worker_count)
        logging.info(f"{self.worker_count} HQ worker(s) connected")

        self.state = "started"

    def start_server(self):
        workdir = self.server_dir / "server"

        args = StartProcessArgs(
            args=self._shared_args() + ["server", "start"],
            hostname=self.nodes[0],
            name="server",
            workdir=workdir,
        )

        if self.info.profile_mode.server:
            self._profile_args(args, workdir / "flamegraph.svg")

        self.cluster.start_processes([args])

    def start_workers(self, worker_assignment: Dict[str, List[HqWorkerConfig]]):
        worker_processes = []

        items = sorted(worker_assignment.items(), key=lambda item: item[0])
        for (node, workers) in items:
            for (worker_index, worker) in enumerate(workers):
                worker_index = f"worker-{node}-{worker_index}"
                workdir = self.server_dir / worker_index
                args = self._shared_args() + ["worker", "start"]
                if worker.cpus is not None:
                    args += ["--cpus", str(worker.cpus)]
                args = StartProcessArgs(
                    args=args,
                    hostname=node,
                    name=worker_index,
                    workdir=workdir,
                )

                if self.info.profile_mode.workers:
                    self._profile_args(args, workdir / "flamegraph.svg")
                worker_processes.append(args)

        self.cluster.start_processes(worker_processes)

    def stop(self):
        assert self.state == "started"
        self.cluster.stop(use_sigint=True)
        self.state = "stopped"

    def submit(self, args: List[str]):
        return subprocess.run(self._shared_args() + args, check=True)

    def _profile_args(self, args: StartProcessArgs, output_path: Path):
        profiler = NativeProfiler()
        assert profiler.is_available()

        args.args = profiler.profile(args.args, output_path,
                                     frequency=self.info.profile_mode.frequency)
        args.metadata["flamegraph"] = str(output_path.absolute())

    def _shared_args(self) -> List[str]:
        return [str(self.binary_path), "--server-dir", str(self.server_dir)]

    def _wait_for_server_start(self):
        wait_until(lambda: (self.server_dir / "hq-current" / "access.json").is_file())

    def _wait_for_workers(self, count: int):
        def get_worker_count():
            output = subprocess.check_output(
                self._shared_args() + ["--output-type", "json", "worker", "list"])
            return len(json.loads(output)) == count

        wait_until(lambda: get_worker_count())
import datetime
import logging
import os
import socket
import subprocess
from contextlib import closing
from pathlib import Path
import time
from typing import Any, Dict, Optional, List

import dataclasses
from cluster.cluster import HOSTNAME
from dask.distributed import Client

from . import Environment, EnvironmentDescriptor
from .utils import EnvStateManager, assign_workers, sanity_check_nodes
from ..clusterutils import ClusterInfo
from ..clusterutils.cluster_helper import ClusterHelper, StartProcessArgs
from ..clusterutils.node_list import Local
from ..utils import ensure_directory

# <hq-root>/benchmarks
BENCHMARKS_DIR = Path(__file__).absolute().parent.parent.parent


@dataclasses.dataclass(frozen=True)
class DaskWorkerConfig:
    """
    One Dask worker corresponds to one node.
    """

    processes: int
    threads_per_process: int
    node: Optional[int] = None
    init_cmd: List[str] = dataclasses.field(default_factory=list)

    @staticmethod
    def create(count: int, n_processes: int = 1, **kwargs) -> "DaskWorkerConfig":
        assert count % n_processes == 0
        return DaskWorkerConfig(
            processes=n_processes, threads_per_process=count // n_processes, **kwargs
        )


@dataclasses.dataclass(frozen=True)
class DaskClusterInfo(EnvironmentDescriptor):
    cluster_info: ClusterInfo
    workers: List[DaskWorkerConfig]
    # Additional information that describes the environment
    environment_params: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def create_environment(self, workdir: Path) -> Environment:
        return DaskEnvironment(self, workdir)

    def name(self) -> str:
        return "dask"

    def parameters(self) -> Dict[str, Any]:
        params = dict(worker_count=len(self.workers))
        params.update(self.environment_params)
        return params

    def metadata(self) -> Dict[str, Any]:
        return {}


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class DaskEnvironment(Environment, EnvStateManager):
    def __init__(self, info: DaskClusterInfo, workdir: Path):
        super().__init__()
        self.info = info
        self._workdir = workdir.resolve()
        self.cluster = ClusterHelper(info.cluster_info, workdir=self.workdir)
        self.client: Optional[Client] = None
        self.dask_dir = ensure_directory(self.workdir / "dask")

        self.nodes = self.info.cluster_info.node_list.resolve()
        sanity_check_nodes(self.nodes)

        worker_nodes = (
            self.nodes
            if isinstance(self.info.cluster_info.node_list, Local)
            else self.nodes[1:]
        )
        if not worker_nodes:
            raise Exception("No worker nodes are available")

        self.worker_count = sum(w.processes for w in self.info.workers)
        assert self.worker_count > 0
        self.worker_assignment = assign_workers(self.info.workers, worker_nodes)
        logging.debug(f"Worker assignment: {self.worker_assignment}")

    @property
    def workdir(self) -> Path:
        return self._workdir

    def start(self):
        self.state_start()
        logging.info("Starting Dask cluster")

        server_port = find_free_port()

        # Make sure to kill all previously running Dask instances
        subprocess.run(["killall", "dask"], check=False)

        self._start_scheduler(server_port)

        server_address = f"{HOSTNAME}:{server_port}"
        self._start_workers(self.worker_assignment, server_address)

        self.client = Client(server_address)
        self.client.wait_for_workers(n_workers=self.worker_count)
        time.sleep(1)
        assert len(self.client.scheduler_info()["workers"]) == self.worker_count

        self.cluster.start_monitoring(
            self.info.cluster_info.node_list.resolve(), observe_processes=True
        )
        self.cluster.commit()
        logging.info("Dask cluster started")

    def _start_scheduler(self, server_port: int):
        scheduler = StartProcessArgs(
            args=[
                "dask",
                "scheduler",
                "--no-dashboard",
                "--host",
                HOSTNAME,
                "--port",
                str(server_port),
            ],
            workdir=ensure_directory(self.dask_dir / "server"),
            hostname=HOSTNAME,
            name="server",
            env=self._get_shared_envs(),
        )
        self.cluster.start_processes([scheduler])

    def _start_workers(
        self, worker_assignment: Dict[str, List[DaskWorkerConfig]], server_address: str
    ):
        worker_processes = []

        items = sorted(worker_assignment.items(), key=lambda item: item[0])
        for node, workers in items:
            for worker_index, worker in enumerate(workers):
                worker: DaskWorkerConfig = worker

                worker_index = f"worker-{node}-{worker_index}"
                workdir = self.dask_dir / worker_index
                args = StartProcessArgs(
                    args=[
                        "dask",
                        "worker",
                        "--nworkers",
                        str(worker.processes),
                        "--nthreads",
                        str(worker.threads_per_process),
                        "--no-dashboard",
                        "--local-directory",
                        "/tmp",
                        server_address,
                    ],
                    workdir=workdir,
                    hostname=node,
                    name="worker",
                    env=self._get_shared_envs(),
                    init_cmd=worker.init_cmd,
                )
                worker_processes.append(args)

        self.cluster.start_processes(worker_processes)

    def stop(self):
        self.state_stop()

        logging.info("Waiting for Dask cluster to shutdown")
        self.client.shutdown()
        self.cluster.wait_for_process_end(
            lambda p: p.key == "server" or p.key.startswith("worker"),
            duration=datetime.timedelta(seconds=5),
        )
        self.cluster.stop(use_sigint=True)

    def get_client(self) -> Client:
        return self.client

    def _get_shared_envs(self) -> Dict[str, str]:
        pythonpath = os.environ.get("PYTHONPATH", "")
        if len(pythonpath) > 0:
            pythonpath += ":"
        env = dict(PYTHONPATH=f"{pythonpath}{BENCHMARKS_DIR}")

        virtualenv = os.environ.get("VIRTUAL_ENV")
        if virtualenv is not None:
            env["VIRTUAL_ENV"] = os.path.abspath(virtualenv)
        return env

import logging
import socket
import subprocess
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, Optional, List

import dataclasses
from cluster.cluster import HOSTNAME
from dask.distributed import Client

from . import Environment, EnvironmentDescriptor
from .utils import EnvStateManager
from ..clusterutils import ClusterInfo
from ..clusterutils.cluster_helper import ClusterHelper, StartProcessArgs
from ..clusterutils.node_list import Local
from ..utils import ensure_directory


@dataclasses.dataclass(frozen=True)
class DaskWorkerConfig:
    """
    One Dask worker corresponds to one node.
    """

    processes: int
    threads_per_process: int


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

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def workdir(self) -> Path:
        return self._workdir

    def start(self):
        self.state_start()
        logging.info("Starting Dask cluster")

        assert isinstance(self.info.cluster_info.node_list, Local)

        worker_count = len(self.info.workers)
        assert worker_count == 1

        server_port = find_free_port()
        dask_dir = ensure_directory(self.workdir / "dask")

        # Make sure to kill all previously running Dask instances
        subprocess.run(["killall", "dask"], check=False)

        server_address = f"{HOSTNAME}:{server_port}"
        scheduler = StartProcessArgs(
            args=["dask", "scheduler", "--no-dashboard", "--host", HOSTNAME, "--port", str(server_port)],
            workdir=ensure_directory(dask_dir / "server"),
            hostname=HOSTNAME,
            name="server",
        )
        self.cluster.start_processes([scheduler])

        workers = [
            StartProcessArgs(
                args=[
                    "dask",
                    "worker",
                    "--nworkers",
                    str(worker.processes),
                    "--nthreads",
                    str(worker.threads_per_process),
                    "--no-dashboard",
                    "--resources",
                    f"cores={worker.threads_per_process}",
                    server_address,
                ],
                workdir=ensure_directory(dask_dir / f"worker-{index}"),
                hostname=HOSTNAME,
                name="worker",
            )
            for (index, worker) in enumerate(self.info.workers)
        ]
        self.cluster.start_processes(workers)

        self.client = Client(server_address)
        self.client.wait_for_workers(n_workers=worker_count)

        self.cluster.start_monitoring(self.info.cluster_info.node_list.resolve(), observe_processes=True)
        self.cluster.commit()
        logging.info("Dask cluster started")

    def stop(self):
        self.state_stop()
        self.client.shutdown()
        self.cluster.stop(use_sigint=True)

    def get_client(self) -> Client:
        return self.client

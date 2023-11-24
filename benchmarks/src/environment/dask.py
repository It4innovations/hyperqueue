import dataclasses
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List

from . import Environment, EnvironmentDescriptor
from .utils import EnvStateManager
from ..clusterutils import ClusterInfo

from dask.distributed import Client, LocalCluster

from ..clusterutils.cluster_helper import ClusterHelper
from ..clusterutils.node_list import Local


@dataclasses.dataclass(frozen=True)
class DaskWorkerConfig:
    cores: int


@dataclasses.dataclass(frozen=True)
class DaskClusterInfo(EnvironmentDescriptor):
    cluster_info: ClusterInfo
    workers: List[DaskWorkerConfig]

    def create_environment(self, workdir: Path) -> Environment:
        return DaskEnvironment(self, workdir)

    def name(self) -> str:
        return "dask"

    def parameters(self) -> Dict[str, Any]:
        return {}

    def metadata(self) -> Dict[str, Any]:
        return {}


class DaskEnvironment(Environment, EnvStateManager):
    def __init__(self, info: DaskClusterInfo, workdir: Path):
        super().__init__()
        self.info = info
        self._workdir = workdir.resolve()
        self.cluster = ClusterHelper(info.cluster_info, workdir=self.workdir)
        self.local_cluster: Optional[LocalCluster] = None
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

        self.local_cluster = LocalCluster(
            n_workers=len(self.info.workers), threads_per_worker=self.info.workers[0].cores, dashboard_address=None
        )
        self.client = self.local_cluster.get_client()
        self.cluster.start_monitoring(self.info.cluster_info.node_list.resolve())
        self.cluster.commit()
        logging.info("Dask cluster started")

    def stop(self):
        self.state_stop()

        self.client.close()

        assert self.local_cluster
        self.local_cluster.close()

        self.cluster.stop(use_sigint=True)

    def get_client(self) -> Client:
        return self.client

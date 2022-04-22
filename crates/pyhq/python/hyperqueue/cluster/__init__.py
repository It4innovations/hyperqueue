import dataclasses
import multiprocessing
from pathlib import Path
from typing import Optional

from ..client import Client
from ..ffi.cluster import Cluster


@dataclasses.dataclass
class WorkerConfig:
    # If None, use all available cores
    cores: Optional[int] = None


class LocalCluster:
    def __init__(
        self,
        server_dir: Optional[Path] = None,
        worker_config: Optional[WorkerConfig] = None,
    ):
        self.cluster = Cluster(server_dir)
        if worker_config is not None:
            self.start_worker(worker_config)

    def start_worker(self, config: WorkerConfig = None):
        config = config if config is not None else WorkerConfig()
        cores = config.cores or multiprocessing.cpu_count()
        self.cluster.add_worker(cores)

    def client(self) -> Client:
        return Client(self.cluster.server_dir)

    def stop(self):
        self.cluster.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

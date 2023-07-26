import dataclasses
import multiprocessing
from pathlib import Path
from typing import Optional

from ..client import Client
from ..ffi.cluster import Cluster


@dataclasses.dataclass
class WorkerConfig:
    """
    Configuration of a worker spawned by a local cluster.
    """

    # If None, use all available cores
    cores: Optional[int] = None


class LocalCluster:
    """
    Represents a local deployed HyperQueue infrastructure.

    You can use `LocalCluster` to quickly spin up a HyperQueue server along with a set of workers
    locally.

    The cluster can be used as a context manager. It will be stopped when the context ends:
    ```python
    with LocalCluster() as cluster:
        client = cluster.client()
        ...
    # The cluster was stopped
    ```
    """

    def __init__(
        self,
        server_dir: Optional[Path] = None,
        worker_config: Optional[WorkerConfig] = None,
    ):
        """
        :param server_dir: Server directory where will the cluster store its files.
        :param worker_config: Configuration of workers spawned in the cluster.
        """
        self.cluster = Cluster(server_dir)
        if worker_config is not None:
            self.start_worker(worker_config)

    def start_worker(self, config: WorkerConfig = None):
        """
        Adds a new worker with the given `config` to the cluster.
        """
        config = config if config is not None else WorkerConfig()
        cores = config.cores or multiprocessing.cpu_count()
        self.cluster.add_worker(cores)

    def client(self, **client_args) -> Client:
        """
        Creates a client connected to this cluster.
        """
        return Client(self.cluster.server_dir, **client_args)

    def stop(self):
        """
        Stops the server and all workers of this cluster.
        """
        self.cluster.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

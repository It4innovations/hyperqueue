from pathlib import Path
from typing import Optional

from ..ffi.cluster import Cluster


class LocalCluster:
    def __init__(self, server_dir: Optional[Path] = None):
        self.cluster = Cluster(server_dir)
        print(self.cluster.server_dir)

    def stop(self):
        self.cluster.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

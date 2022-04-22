from typing import Optional

from . import ffi


class HqClusterContext:
    """
    Opaque class returned from `cluster_start`.
    Should be passed to FFI methods that require it.
    """


class Cluster:
    def __init__(self, directory: Optional[str] = None):
        self.directory = directory
        self.ctx: HqClusterContext = ffi.cluster_start(directory)

    def add_worker(self, cores: int):
        self.ctx.add_worker(cores)

    @property
    def server_dir(self) -> str:
        return self.ctx.server_dir

    def stop(self):
        return self.ctx.stop()

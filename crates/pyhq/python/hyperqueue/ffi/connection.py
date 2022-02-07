from typing import Optional, Sequence

from .. import hyperqueue as ffi
from ..job import JobId
from .protocol import JobDescription


class HqContext:
    """
    Opaque class returned from `connect_to_server`.
    Should be passed to FFI methods that require it.
    """


class Connection:
    def __init__(self, directory: Optional[str] = None):
        self.ctx: HqContext = ffi.connect_to_server(directory)

    def submit_job(self, job_description: JobDescription) -> JobId:
        return ffi.submit_job(self.ctx, job_description)

    def wait_for_jobs(self, job_ids: Sequence[JobId]) -> int:
        """Blocks until jobs are finished. Returns the number of failed tasks"""
        return ffi.wait_for_jobs(self.ctx, job_ids)

    def stop_server(self):
        return ffi.stop_server(self.ctx)


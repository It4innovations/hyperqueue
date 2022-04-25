from typing import Dict, List, Optional, Sequence

from . import JobId, TaskId, ffi
from .protocol import JobDescription


class HqClientContext:
    """
    Opaque class returned from `connect_to_server`.
    Should be passed to FFI methods that require it.
    """


class ClientConnection:
    def __init__(self, directory: Optional[str] = None):
        self.ctx: HqClientContext = ffi.connect_to_server(directory)

    def submit_job(self, job_description: JobDescription) -> JobId:
        return ffi.submit_job(self.ctx, job_description)

    def wait_for_jobs(self, job_ids: Sequence[JobId], callback) -> List[JobId]:
        """Blocks until jobs are finished. Returns the number of failed tasks"""
        return ffi.wait_for_jobs(self.ctx, job_ids, callback)

    def stop_server(self):
        return ffi.stop_server(self.ctx)

    def get_error_messages(
        self, job_ids: Sequence[JobId]
    ) -> Dict[JobId, Dict[TaskId, str]]:
        return ffi.get_error_messages(self.ctx, job_ids)

import dataclasses
from typing import Dict, List, Optional, Sequence

from . import JobId, TaskId, ffi
from .protocol import JobDescription


class HqClientContext:
    """
    Opaque class returned from `connect_to_server`.
    Should be passed to FFI methods that require it.
    """


@dataclasses.dataclass(frozen=True)
class FailedTaskContext:
    error: str
    cwd: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]


TaskFailureMap = Dict[JobId, Dict[TaskId, FailedTaskContext]]


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

    def get_failed_tasks(self, job_ids: Sequence[JobId]) -> TaskFailureMap:
        jobs = ffi.get_failed_tasks(self.ctx, job_ids)
        return {
            job_id: {
                task_id: FailedTaskContext(**data)
                for (task_id, data) in task_data.items()
            }
            for (job_id, task_data) in jobs.items()
        }

    def forget_job(self, job_id: JobId):
        return ffi.forget_job(self.ctx, job_id)

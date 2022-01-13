from typing import Optional

from .. import hyperqueue as ffi
from ..job import Job, JobId
from .protocol import JobDescription
from .utils import task_config_to_task_desc


class HqContext:
    """
    Opaque class returned from `connect_to_server`.
    Should be passed to FFI methods that require it.
    """


def connect_to_server(directory: Optional[str] = None) -> HqContext:
    return ffi.connect_to_server(directory)


def stop_server(ctx: HqContext):
    return ffi.stop_server(ctx)


def submit_job(ctx: HqContext, job: Job) -> JobId:
    configs = job.build()
    job = JobDescription(tasks=[task_config_to_task_desc(config) for config in configs])
    return ffi.submit_job(ctx, job)

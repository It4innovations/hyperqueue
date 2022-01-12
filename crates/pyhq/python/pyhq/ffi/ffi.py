import dataclasses
from typing import List, Optional

from .. import pyhq as ffi


class HqContext:
    """
    Opaque class returned from `connect_to_server`.
    Should be passed to FFI methods that require it.
    """


@dataclasses.dataclass()
class TaskDescription:
    args: List[str]


@dataclasses.dataclass
class JobDescription:
    tasks: List[TaskDescription]


JobId = int


def connect_to_server(directory: Optional[str] = None) -> HqContext:
    return ffi.connect_to_server(directory)


def stop_server(ctx: HqContext):
    return ffi.stop_server(ctx)


def submit_job(ctx: HqContext, job: JobDescription) -> JobId:
    job = dataclasses.asdict(job)
    return ffi.submit_job(ctx, job)

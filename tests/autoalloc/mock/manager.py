import dataclasses
import datetime
import enum
from abc import ABC
from subprocess import Popen
from typing import Dict, Optional

from ...conftest import HqEnv
from .handler import CommandResponse, MockInput, response, response_error


class Manager(ABC):
    async def handle_submit(self, input: MockInput) -> CommandResponse:
        return response_error()

    async def handle_status(self, input: MockInput) -> CommandResponse:
        return response_error()

    async def handle_delete(self, input: MockInput) -> Optional[CommandResponse]:
        return response_error()


class WrappedManager(Manager):
    def __init__(self, inner: Manager):
        self.inner = inner

    async def handle_submit(self, input: MockInput) -> CommandResponse:
        return await self.inner.handle_submit(input)

    async def handle_status(self, input: MockInput) -> CommandResponse:
        return await self.inner.handle_status(input)

    async def handle_delete(self, input: MockInput) -> Optional[CommandResponse]:
        return await self.inner.handle_delete(input)


class JobStatus(enum.Enum):
    Queued = enum.auto()
    Running = enum.auto()
    Finished = enum.auto()
    Failed = enum.auto()


@dataclasses.dataclass(frozen=True)
class JobData:
    @staticmethod
    def running() -> "JobData":
        return JobData(
            status=JobStatus.Running,
            stime=now(),
            qtime=now() - datetime.timedelta(seconds=1),
            mtime=now(),
        )

    @staticmethod
    def queued() -> "JobData":
        return JobData(
            status=JobStatus.Queued,
            qtime=now() - datetime.timedelta(seconds=1),
        )

    @staticmethod
    def finished() -> "JobData":
        return JobData(
            status=JobStatus.Finished,
            stime=now(),
            qtime=now() - datetime.timedelta(seconds=1),
            mtime=now() + datetime.timedelta(seconds=1),
            exit_code=0,
        )

    @staticmethod
    def failed() -> "JobData":
        return JobData(
            status=JobStatus.Failed,
            stime=now(),
            qtime=now() - datetime.timedelta(seconds=1),
            mtime=now() + datetime.timedelta(seconds=1),
            exit_code=1,
        )

    status: JobStatus
    qtime: Optional[datetime.datetime] = None
    stime: Optional[datetime.datetime] = None
    mtime: Optional[datetime.datetime] = None
    exit_code: Optional[int] = None


def now() -> datetime.datetime:
    return datetime.datetime.now()


class DefaultManager(Manager):
    def __init__(self):
        self.job_counter = 0
        # None = job is purposely missing
        self.jobs: Dict[str, Optional[JobData]] = {}
        self.deleted_jobs = set()

    async def handle_submit(self, _input: MockInput) -> CommandResponse:
        # By default, create a new job
        job_id = self.job_id(self.job_counter)
        self.job_counter += 1

        # The state of this job could already have been set before manually
        if job_id not in self.jobs:
            self.jobs[job_id] = JobData.queued()
        return response(stdout=self.submit_response(job_id))

    async def handle_status(self, input: MockInput) -> CommandResponse:
        raise NotImplementedError

    async def handle_delete(self, input: MockInput) -> Optional[CommandResponse]:
        job_id = input.arguments[0]
        assert job_id in self.jobs
        self.deleted_jobs.add(job_id)
        return None

    def job_id(self, index: int) -> str:
        return f"{index + 1}.job"

    def set_job_data(self, job_id: str, status: Optional[JobData]):
        self.jobs[job_id] = status

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        raise NotImplementedError

    def submit_response(self, job_id: str) -> str:
        raise NotImplementedError

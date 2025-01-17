import dataclasses
import datetime
import enum
from abc import ABC
from typing import List, Optional, Dict

from .command import CommandInput

JobId = str


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


class Manager(ABC):
    """
    Simulates the behavior of an allocation manager.
    Ideally, it should be agnostic of the actual manager implementation (PBS/Slurm),
    that's why it receives and returns domain information (e.g. job IDs), and not
    the raw command-line parameters.
    """

    async def handle_submit(self, input: CommandInput) -> JobId:
        raise NotImplementedError

    async def handle_status(self, input: CommandInput, job_ids: List[JobId]) -> Dict[JobId, JobData]:
        raise NotImplementedError

    async def handle_delete(self, input: CommandInput, job_id: JobId):
        raise NotImplementedError

    def set_job_data(self, job_id: str, status: Optional[JobData]):
        raise NotImplementedError


class WrappedManager(Manager):
    def __init__(self, inner: Manager):
        self.inner = inner

    async def handle_submit(self, input: CommandInput) -> JobId:
        return await self.inner.handle_submit(input)

    async def handle_status(self, input: CommandInput, job_ids: List[JobId]) -> Dict[JobId, JobData]:
        return await self.inner.handle_status(input, job_ids)

    async def handle_delete(self, input: CommandInput, job_id: JobId):
        return await self.inner.handle_delete(input, job_id)

    def set_job_data(self, job_id: str, status: Optional[JobData]):
        return self.set_job_data(job_id, status)


class ManagerException(BaseException):
    """
    An exception that should be propagated as an error response from a manager,
    and should not be thrown in tests.
    """

    pass


def now() -> datetime.datetime:
    return datetime.datetime.now()


class DefaultManager(Manager):
    def __init__(self):
        self.job_counter = 0
        # None = job is purposely missing
        self.jobs: Dict[str, Optional[JobData]] = {}
        self.deleted_jobs = set()

    async def handle_submit(self, input: CommandInput) -> JobId:
        # By default, create a new job
        job_id = self.job_id(self.job_counter)
        self.job_counter += 1

        # The state of this job could already have been set before manually
        if job_id not in self.jobs:
            self.jobs[job_id] = JobData.queued()
        return job_id

    async def handle_status(self, input: CommandInput, job_ids: List[JobId]) -> Dict[JobId, JobData]:
        return {job_id: self.jobs.get(job_id) for job_id in self.jobs}

    async def handle_delete(self, input: CommandInput, job_id: JobId):
        assert job_id in self.jobs
        self.deleted_jobs.add(job_id)

    def set_job_data(self, job_id: str, status: Optional[JobData]):
        self.jobs[job_id] = status

    def job_id(self, index: int) -> str:
        return f"{index + 1}.job"

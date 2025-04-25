from subprocess import Popen

import dataclasses
import datetime
import enum
from abc import ABC
from typing import List, Optional, Dict, Literal

from ...conftest import HqEnv

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


class ManagerException(BaseException):
    """
    An exception that should be propagated as an error response from a manager,
    and should not be thrown in tests.
    """

    pass


def now() -> datetime.datetime:
    return datetime.datetime.now()


def default_job_id(index: int = 0) -> str:
    return f"{index + 1}.job"


@dataclasses.dataclass
class CommandInput:
    command: str
    arguments: List[str]
    cwd: str


@dataclasses.dataclass
class CommandOutput:
    stdout: str = ""
    stderr: str = ""
    code: int = 0


def response(stdout="", stderr="", code=0) -> CommandOutput:
    return CommandOutput(stdout=stdout, stderr=stderr, code=code)


def response_error(stdout="", stderr="", code=1):
    return response(stdout=stdout, stderr=stderr, code=code)


CommandType = Literal["submit", "status", "delete"]


class ManagerAdapter(ABC):
    """Adapts CLI inputs to domain data and domain data to CLI outputs."""

    def parse_command_type(self, input: CommandInput) -> CommandType:
        raise NotImplementedError

    def format_submit_output(self, job_id: JobId) -> CommandOutput:
        raise NotImplementedError

    def parse_status_job_ids(self, input: CommandInput) -> List[JobId]:
        raise NotImplementedError

    def format_status_output(self, job_data: Dict[JobId, JobData]) -> CommandOutput:
        raise NotImplementedError

    def start_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        raise NotImplementedError


class CommandHandler:
    """
    Implements manager logic on the level of CLI, reading command-line arguments
    and returning an exit status and standard output.
    """

    def __init__(self, adapter: ManagerAdapter):
        self.adapter = adapter

        self.job_counter = 0
        # None = job is purposely missing
        self.jobs: Dict[str, Optional[JobData]] = {}
        self.deleted_jobs = set()

    async def handle_command(self, input: CommandInput) -> CommandOutput:
        cmd_type = self.adapter.parse_command_type(input)
        if cmd_type == "submit":
            return await self.handle_cli_submit(input)
        elif cmd_type == "status":
            return await self.handle_cli_status(input)
        elif cmd_type == "delete":
            return await self.handle_cli_delete(input)
        raise Exception(f"Input command {input} not handled")

    async def handle_cli_submit(self, input: CommandInput) -> CommandOutput:
        job_id = await self.handle_submit()
        return self.adapter.format_submit_output(job_id)

    async def handle_submit(self) -> JobId:
        # By default, create a new job
        job_id = default_job_id(self.job_counter)
        self.job_counter += 1

        # The state of this job could already have been set before manually
        if job_id not in self.jobs:
            self.jobs[job_id] = JobData.queued()
        return job_id

    async def handle_cli_status(self, input: CommandInput) -> CommandOutput:
        job_ids = self.adapter.parse_status_job_ids(input)
        job_data = await self.handle_status(job_ids)
        return self.adapter.format_status_output(job_data)

    async def handle_status(self, job_ids: List[JobId]) -> Dict[JobId, Optional[JobData]]:
        return {job_id: self.jobs.get(job_id) for job_id in job_ids}

    async def handle_cli_delete(self, input: CommandInput) -> CommandOutput:
        assert len(input.arguments) == 1
        await self.handle_delete(input.arguments[0])
        return response()

    async def handle_delete(self, job_id: JobId):
        assert job_id in self.jobs
        self.deleted_jobs.add(job_id)

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        self.set_job_data(allocation_id, JobData.running())
        return self.adapter.start_worker(hq_env, allocation_id)

    def set_job_data(self, job_id: str, status: Optional[JobData]):
        self.jobs[job_id] = status

import abc
import dataclasses
from abc import ABC
from subprocess import Popen
from typing import Dict, Generic, List, Optional, TypeVar

from aiohttp.web_request import Request

from ...conftest import HqEnv


@dataclasses.dataclass
class CommandResponse:
    stdout: str = ""
    stderr: str = ""
    code: int = 0


def response(stdout="", stderr="", code=0) -> CommandResponse:
    return CommandResponse(stdout=stdout, stderr=stderr, code=code)


def response_error(stdout="", stderr="", code=1):
    return response(stdout=stdout, stderr=stderr, code=code)


class CommandHandler(ABC):
    @abc.abstractmethod
    async def handle_command(
        self, request: Request, cmd: str
    ) -> Optional[CommandResponse]:
        return response_error()


@dataclasses.dataclass
class MockInput:
    arguments: List[str]
    cwd: str


async def extract_mock_input(request) -> MockInput:
    data = await request.json()
    arguments = data["arguments"]
    assert isinstance(arguments, list)
    if len(arguments) > 0:
        assert isinstance(arguments[0], str)
    cwd = data["cwd"]
    assert isinstance(cwd, str)
    return MockInput(arguments=arguments, cwd=cwd)


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


JobType = TypeVar("JobType")


class DefaultManager(Manager, Generic[JobType]):
    def __init__(self):
        self.job_counter = 0
        # None = job is purposely missing
        self.jobs: Dict[str, Optional[JobType]] = {}
        self.deleted_jobs = set()

    async def handle_submit(self, _input: MockInput) -> CommandResponse:
        # By default, create a new job
        job_id = self.job_id(self.job_counter)
        self.job_counter += 1

        # The state of this job could already have been set before manually
        if job_id not in self.jobs:
            self.jobs[job_id] = self.queue_job_state()
        return response(stdout=job_id)

    async def handle_status(self, input: MockInput) -> CommandResponse:
        raise NotImplementedError

    async def handle_delete(self, input: MockInput) -> Optional[CommandResponse]:
        job_id = input.arguments[0]
        assert job_id in self.jobs
        self.deleted_jobs.add(job_id)
        return None

    def job_id(self, index: int) -> str:
        return f"{index + 1}.job"

    def set_job_status(self, job_id: str, status: Optional[JobType]):
        self.jobs[job_id] = status

    def queue_job_state(self) -> JobType:
        raise NotImplementedError

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        raise NotImplementedError

import abc
import dataclasses
from abc import ABC
from typing import List, Optional

from aiohttp.web_request import Request


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


class ManagerCommandHandler(ABC):
    async def handle_submit(self, input: MockInput) -> CommandResponse:
        return response_error()

    async def handle_status(self, input: MockInput) -> CommandResponse:
        return response_error()

    async def handle_delete(self, input: MockInput) -> Optional[CommandResponse]:
        return response_error()


class WrappedManagerCommandHandler(ManagerCommandHandler):
    def __init__(self, inner: ManagerCommandHandler):
        self.inner = inner

    async def handle_submit(self, input: MockInput) -> CommandResponse:
        return await self.inner.handle_submit(input)

    async def handle_status(self, input: MockInput) -> CommandResponse:
        return await self.inner.handle_status(input)

    async def handle_delete(self, input: MockInput) -> Optional[CommandResponse]:
        return await self.inner.handle_delete(input)

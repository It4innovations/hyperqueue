import dataclasses
from typing import List, Optional

from aiohttp.web_request import Request


class CommandException(BaseException):
    pass


@dataclasses.dataclass
class CommandResponse:
    stdout: str = ""
    stderr: str = ""
    code: int = 0


def response(stdout="", stderr="", code=0) -> CommandResponse:
    return CommandResponse(stdout=stdout, stderr=stderr, code=code)


def response_error(stdout="", stderr="", code=1):
    return response(stdout=stdout, stderr=stderr, code=code)


class CommandHandler:
    async def handle_command(
        self, request: Request, cmd: str
    ) -> Optional[CommandResponse]:
        try:
            return response_error()
        except CommandException as e:
            return response_error(code=1, stderr=str(e))


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

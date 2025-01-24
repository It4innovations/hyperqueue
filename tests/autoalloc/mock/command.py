import dataclasses
from abc import ABC
from subprocess import Popen

from aiohttp.web_request import Request
from typing import List

from ...conftest import HqEnv


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


class CommandHandler(ABC):
    """
    Implements manager logic on the level of CLI, reading command-line arguments
    and returning an exit status and standard output.
    """

    async def handle_command(self, command: CommandInput) -> CommandOutput:
        raise NotImplementedError

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        raise NotImplementedError


async def extract_mock_input(request: Request, cmd: str) -> CommandInput:
    data = await request.json()
    arguments = data["arguments"]
    assert isinstance(arguments, list)
    if len(arguments) > 0:
        assert isinstance(arguments[0], str)
    cwd = data["cwd"]
    assert isinstance(cwd, str)
    return CommandInput(command=cmd, arguments=arguments, cwd=cwd)

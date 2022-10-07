from typing import List

from aiohttp import web
from aiohttp.web_request import Request


class CommandHandler:
    async def handle_command(self, request: Request, cmd: str):
        return response_exit()


def response_ok(stdout=""):
    return {"action": "ok", "stdout": stdout}


def response_exit(code=1):
    return {"action": "error", "code": code}


async def extract_arguments(request) -> List[str]:
    data = await request.json()
    arguments = data["arguments"]
    assert isinstance(arguments, list)
    if len(arguments) > 0:
        assert isinstance(arguments[0], str)
    return arguments


class PbsCommandHandler(CommandHandler):
    async def handle_command(self, request: Request, cmd: str):
        arguments = await extract_arguments(request)

        if cmd == "qsub":
            await self.inspect_qsub(arguments)
            response = await self.handle_qsub(arguments)
        else:
            raise Exception(f"Unknown PBS command {cmd}")

        return web.json_response(response)

    async def handle_qsub(self, arguments: List[str]):
        return response_ok()

    async def inspect_qsub(self, arguments: List[str]):
        pass

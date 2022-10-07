from typing import Optional

from aiohttp.web_request import Request

from .handler import (
    CommandHandler,
    CommandResponse,
    DefaultManager,
    Manager,
    extract_mock_input,
)


class SlurmCommandAdapter(CommandHandler):
    def __init__(self, handler: Manager):
        self.handler = handler

    async def handle_command(
        self, request: Request, cmd: str
    ) -> Optional[CommandResponse]:
        input = await extract_mock_input(request)

        if cmd == "sbatch":
            return await self.handler.handle_submit(input)
        elif cmd == "scontrol":
            return await self.handler.handle_status(input)
        elif cmd == "scancel":
            return await self.handler.handle_delete(input)
        else:
            raise Exception(f"Unknown Slurm command {cmd}")


def adapt_slurm(handler: Manager) -> CommandHandler:
    return SlurmCommandAdapter(handler)


class SlurmManager(DefaultManager):
    pass

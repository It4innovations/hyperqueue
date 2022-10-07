import asyncio
import logging
import queue
import sys
import threading
from pathlib import Path
from typing import Optional

from aiohttp import web
from jinja2 import Environment, FileSystemLoader

from ...conftest import HqEnv
from ...utils.io import find_free_port
from .handler import CommandHandler, CommandResponse, response

TEMPLATE_DIR = Path(__file__).absolute().parent / "template"
REDIRECTOR_TEMPLATE = "redirector.jinja"


def prepare_redirector(path: Path, port: int):
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template(REDIRECTOR_TEMPLATE)
    rendered = template.render(python=sys.executable, port=port)

    with open(path, "w") as f:
        f.write(rendered)


class MockJobManager:
    """
    Creates a mock job manager that intercepts job manager (PBS) in the given `hq_env` and passes
    them to the given `handler`.
    """

    def __init__(
        self,
        hq_env: HqEnv,
        handler: CommandHandler,
        mocked_commands=("qsub", "qstat", "qdel", "sbatch", "scontrol", "scancel"),
    ):
        self.handler = handler

        self.port = find_free_port()

        # Prepare redirector
        redirector_path = hq_env.mock.directory / "redirector.py"
        logging.debug(f"Wrote redirector to {redirector_path} with port {self.port}")
        prepare_redirector(redirector_path, self.port)

        # Link selected commands to redirector
        for cmd in mocked_commands:
            hq_env.mock.mock(cmd, redirector_path)

        self.bg_server: Optional[BackgroundServer] = None

    def __enter__(self):
        assert self.bg_server is None
        self.bg_server = BackgroundServer(self.handler, self.port)

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.bg_server is not None
        self.bg_server.stop()
        self.bg_server = None


class BackgroundServer:
    """
    Runs an async HTTP server in the background.
    The server intercepts POST requests to /{cmd} and then calls `handle_{cmd}` on the given
    `handler`.
    """

    def __init__(self, handler: CommandHandler, port: int):
        self.handler = handler

        async def handle_request(request):
            command = request.match_info["cmd"]
            logging.info(f"Received request: {request}, command: {command}")
            resp = await self.handler.handle_command(request, command)
            if resp is None:
                resp = response()
            assert isinstance(resp, CommandResponse)

            resp = {
                "stdout": resp.stdout,
                "stderr": resp.stderr,
                "code": resp.code,
            }
            return web.json_response(resp)

        self.app = web.Application()
        self.app.add_routes(
            [
                web.post("/{cmd}", handle_request),
            ]
        )

        self.stop_event = asyncio.Event()
        self.queue = queue.SimpleQueue()
        self.thread = threading.Thread(target=self.run, args=(self.app, port))
        self.thread.start()
        self.event_loop = self.queue.get()
        logging.info("Server started")

    def run(self, app, port):
        async def body():
            logging.info(f"Starting mock server on port {port}")
            runner = web.AppRunner(app)
            await runner.setup()

            site = web.TCPSite(runner, host="0.0.0.0", port=port, reuse_port=True)
            try:
                await site.start()
                self.queue.put(asyncio.get_running_loop())
                await self.stop_event.wait()
                logging.info("Server thread received stop message")
            finally:
                await site.stop()
                await runner.cleanup()
            logging.info("Server thread stopping")

        loop = asyncio.new_event_loop()
        loop.run_until_complete(body())
        loop.close()

    def stop(self):
        logging.info("Stopping server")
        self.event_loop.call_soon_threadsafe(lambda: self.stop_event.set())
        self.thread.join()
        logging.info("Server stopped")

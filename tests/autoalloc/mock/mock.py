import sys

import asyncio
import logging
import queue
import random
import string
import threading
from aiohttp import web
from pathlib import Path
from typing import Optional

from .command import CommandOutput, response_error, extract_mock_input, CommandHandler
from .manager import ManagerException
from ...conftest import HqEnv


def prepare_redirector(path: Path, port: int, key: str):
    """
    This is a small Python program that intercepts command-line arguments and sends
    them as a POST request to a localhost server at the specified `port`. It then reacts accordingly
    based on the response, either print something to stdout or exit with the specified exit code.
    """
    python = sys.executable
    redirector_code = f"""#!{python}

import os.path
import sys

import requests

PORT = {port}
KEY = "{key}"

arguments = sys.argv

program = os.path.basename(arguments[0])
response = requests.post(f"http://localhost:{{PORT}}/{{program}}", json={{
    "arguments": arguments[1:],
    "cwd": os.getcwd(),
}}, headers={{"HQ_TEST_KEY": KEY}}).json()

print(response["stdout"], flush=True)
print(response["stderr"], file=sys.stderr, flush=True)
exit(response["code"])
"""

    with open(path, "w") as f:
        f.write(redirector_code)


class MockJobManager:
    """
    Creates a mock job manager that intercepts job manager (PBS/Slurm) in the given `hq_env` and
    passes them to the given `handler`.
    """

    def __init__(
        self,
        hq_env: HqEnv,
        handler: CommandHandler,
        mocked_commands=("qsub", "qstat", "qdel", "sbatch", "scontrol", "scancel"),
    ):
        self.handler: CommandHandler = handler

        # Make sure that we're not communicating with a different test instance that could target
        # the same port.
        self.key = generate_key()

        # Prepare redirector path
        self.redirector_path = hq_env.mock.directory / "redirector.py"
        self.mocked_commands = mocked_commands
        self.hq_env = hq_env

        self.bg_server: Optional[BackgroundServer] = None

    def __enter__(self):
        assert self.bg_server is None
        self.bg_server = BackgroundServer(handler=self.handler, key=self.key)
        prepare_redirector(self.redirector_path, port=self.bg_server.port, key=self.key)
        # Link selected commands to redirector
        for cmd in self.mocked_commands:
            self.hq_env.mock.redirect_program_to_binary(cmd, self.redirector_path)

        logging.debug(f"Wrote redirector to {self.redirector_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.bg_server is not None
        self.bg_server.stop()
        self.bg_server = None


def generate_key() -> str:
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(20))


class BackgroundServer:
    """
    Runs an async HTTP server in the background.
    The server intercepts POST requests to /{cmd} and then calls `handle_{cmd}` on the given
    `handler`.
    """

    def __init__(self, handler: CommandHandler, key: str):
        self.exception_queue = queue.SimpleQueue()

        start_signal = queue.SimpleQueue()
        self.thread = threading.Thread(target=self.run, args=(start_signal, self.exception_queue, key, handler))
        self.thread.start()

        # Receive data from server once it starts
        (event_loop, stop_signal, port) = start_signal.get(timeout=5)
        self.event_loop = event_loop
        self.stop_signal = stop_signal
        self.port = port
        logging.info("Server started")

    def run(
        self, start_signal: queue.SimpleQueue, exception_queue: queue.SimpleQueue, key: str, handler: CommandHandler
    ):
        stop_signal: Optional[asyncio.Queue] = None

        async def handle_request(request):
            command = request.match_info["cmd"]
            request_key = request.headers["HQ_TEST_KEY"]
            assert request_key == key

            logging.info(f"Received request: {request}, command: {command}")
            input = await extract_mock_input(request, command)
            try:
                resp = await handler.handle_command(input)
            except ManagerException as e:
                # This exception should not be propagated within tests, and should be returned
                # in stderr of the manager response instead.
                resp = response_error(stderr=str(e))
            assert isinstance(resp, CommandOutput)

            resp = {
                "stdout": resp.stdout,
                "stderr": resp.stderr,
                "code": resp.code,
            }
            return web.json_response(resp)

        @web.middleware
        async def handle_error(request, handler):
            """
            Make sure that if any exception is thrown, we propagate it to the tests.
            """
            nonlocal stop_signal
            try:
                return await handler(request)
            except BaseException as e:
                stop_signal.put_nowait(e)
                return response_error()

        app = web.Application(middlewares=[handle_error])
        app.add_routes(
            [
                web.post("/{cmd}", handle_request),
            ]
        )

        async def body():
            """
            On Python <3.10, asyncio.Queue cannot be created outside an event loop.
            That is why it is created in such a complicated way here.
            """
            nonlocal stop_signal
            stop_signal = asyncio.Queue()

            logging.info("Starting mock server")
            runner = web.AppRunner(app)
            await runner.setup()

            site = web.TCPSite(runner, host="0.0.0.0", port=0)
            try:
                await site.start()
                port = runner.addresses[0][1]
                logging.info(f"Mock server started on port {port}")

                start_signal.put((asyncio.get_running_loop(), stop_signal, port))
                exc = await stop_signal.get()
                if exc is not None:
                    raise exc
                logging.info("Server thread received stop message")
            finally:
                await site.stop()
                await runner.cleanup()
            logging.info("Server thread stopping")

        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(body())
            loop.close()
        except BaseException as e:
            exception_queue.put_nowait(e)

    def stop(self):
        logging.info("Stopping server")
        self.event_loop.call_soon_threadsafe(lambda: self.stop_signal.put_nowait(None))
        self.thread.join(5)

        try:
            exc = self.exception_queue.get_nowait()
            logging.info("Server stopped with an exception")
            raise exc
        except queue.Empty:
            logging.info("Server stopped")

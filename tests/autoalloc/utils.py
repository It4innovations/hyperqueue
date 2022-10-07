from queue import Queue
from typing import List, Optional

from ..conftest import HqEnv
from .mock.handler import Manager, MockInput, WrappedManager


def program_code_store_args_json(path: str) -> str:
    """
    Creates program code that stores its cmd arguments as JSON into the specified `path`.
    """
    return f"""
import sys
import json

with open("{path}", "w") as f:
    f.write(json.dumps(sys.argv))
"""


def extract_script_args(script: str, prefix: str) -> List[str]:
    return [
        line[len(prefix) :].strip()
        for line in script.splitlines(keepends=False)
        if line.startswith(prefix)
    ]


def extract_script_commands(script: str) -> str:
    """
    Returns all non-empty lines as text from `script` that do not start with a bash comment (`#`).
    """
    return "\n".join(
        line.strip()
        for line in script.splitlines(keepends=False)
        if not line.startswith("#") and line.strip()
    )


def add_queue(
    hq_env: HqEnv,
    manager="pbs",
    name: Optional[str] = "foo",
    backlog=1,
    workers_per_alloc=1,
    additional_worker_args: List[str] = None,
    additional_args=None,
    time_limit="1h",
    dry_run=False,
    **kwargs,
) -> str:
    args = ["alloc", "add", manager]
    if not dry_run:
        args.append("--no-dry-run")
    if name is not None:
        args.extend(["--name", name])
    args.extend(
        [
            "--backlog",
            str(backlog),
            "--workers-per-alloc",
            str(workers_per_alloc),
        ]
    )
    if time_limit is not None:
        args.extend(["--time-limit", time_limit])
    if additional_worker_args is not None:
        args.extend(additional_worker_args)
    if additional_args is not None:
        args.append("--")
        args.extend(additional_args.split(" "))

    return hq_env.command(args, **kwargs)


def prepare_tasks(hq_env: HqEnv, count=1000):
    hq_env.command(["submit", f"--array=0-{count}", "sleep", "1"])


def remove_queue(hq_env: HqEnv, queue_id: int, force=False, **kwargs):
    args = ["alloc", "remove", str(queue_id)]
    if force:
        args.append("--force")
    return hq_env.command(args, **kwargs)


class ExtractSubmitScriptPath(WrappedManager):
    def __init__(self, queue: Queue, inner: Manager):
        super().__init__(inner=inner)
        self.queue = queue

    async def handle_submit(self, input: MockInput):
        script_path = input.arguments[0]
        self.queue.put(script_path)
        return await super().handle_submit(input)

from queue import Queue
from typing import List, Literal, Optional, Union

from ..conftest import HqEnv
from .mock.handler import MockInput
from .mock.manager import Manager, WrappedManager


def extract_script_args(script: str, prefix: str) -> List[str]:
    return [line[len(prefix) :].strip() for line in script.splitlines(keepends=False) if line.startswith(prefix)]


def extract_script_commands(script: str) -> str:
    """
    Returns all non-empty lines as text from `script` that do not start with a bash comment (`#`).
    """
    return "\n".join(
        line.strip() for line in script.splitlines(keepends=False) if not line.startswith("#") and line.strip()
    )


ManagerType = Union[Literal["pbs"], Literal["slurm"]]


def add_queue(
    hq_env: HqEnv,
    manager: ManagerType,
    name: Optional[str] = "foo",
    backlog=1,
    workers_per_alloc=1,
    additional_worker_args: List[str] = None,
    additional_args=None,
    time_limit="1h",
    worker_time_limit: Optional[str] = None,
    dry_run=False,
    start_cmd: Optional[str] = None,
    stop_cmd: Optional[str] = None,
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
    if worker_time_limit is not None:
        args.extend(["--worker-time-limit", worker_time_limit])
    if start_cmd is not None:
        args.extend(["--worker-start-cmd", start_cmd])
    if stop_cmd is not None:
        args.extend(["--worker-stop-cmd", stop_cmd])
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


class ManagerQueue:
    def __init__(self):
        self.queue = Queue()

    def put(self, value):
        self.queue.put(value)

    def get(self):
        return self.queue.get(timeout=5)


class ExtractSubmitScriptPath(WrappedManager):
    def __init__(self, queue: ManagerQueue, inner: Manager):
        super().__init__(inner=inner)
        self.queue = queue

    async def handle_submit(self, input: MockInput):
        script_path = input.arguments[0]
        self.queue.put(script_path)
        return await super().handle_submit(input)


def pause_queue(hq_env: HqEnv, queue_id: int, **kwargs):
    args = ["alloc", "pause", str(queue_id)]
    return hq_env.command(args, **kwargs)


def resume_queue(hq_env: HqEnv, queue_id: int, **kwargs):
    args = ["alloc", "resume", str(queue_id)]
    return hq_env.command(args, **kwargs)

from queue import Queue
from subprocess import Popen
from typing import List, Literal, Optional, Union

from .mock.manager import CommandHandler, CommandInput, ManagerAdapter
from ..utils.wait import TimeoutException, wait_until, DEFAULT_TIMEOUT

from ..conftest import HqEnv


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
    name: Optional[str] = None,
    backlog=1,
    max_workers_per_alloc=1,
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
            "--max-workers-per-alloc",
            str(max_workers_per_alloc),
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


class CommQueue:
    def __init__(self):
        self.queue = Queue()

    def put(self, value):
        self.queue.put(value)

    def get(self):
        return self.queue.get(timeout=5)


class ExtractSubmitScriptPath(CommandHandler):
    def __init__(self, adapter: ManagerAdapter):
        super().__init__(adapter)
        self.queue = CommQueue()

    async def handle_cli_submit(self, input: CommandInput):
        script_path = input.arguments[0]
        self.queue.put(script_path)
        return await super().handle_cli_submit(input)

    def get_script_path(self) -> str:
        return self.queue.get()


def pause_queue(hq_env: HqEnv, queue_id: int, **kwargs):
    args = ["alloc", "pause", str(queue_id)]
    return hq_env.command(args, **kwargs)


def resume_queue(hq_env: HqEnv, queue_id: int, **kwargs):
    args = ["alloc", "resume", str(queue_id)]
    return hq_env.command(args, **kwargs)


def wait_for_alloc(hq_env: HqEnv, state: str, allocation_id: str, timeout=DEFAULT_TIMEOUT):
    """
    Wait until an allocation has the given `state`.
    Assumes a single allocation queue.
    """

    last_table = None

    def wait():
        nonlocal last_table

        last_table = hq_env.command(["alloc", "info", "1"], as_table=True)
        for index in range(len(last_table)):
            if (
                last_table.get_column_value("ID")[index] == allocation_id
                and last_table.get_column_value("State")[index] == state
            ):
                return True
        return False

    try:
        wait_until(wait, timeout_s=timeout)
    except TimeoutException as e:
        if last_table is not None:
            raise Exception(f"{e}, most recent table:\n{last_table}")
        raise e


def start_server_with_quick_refresh(hq_env: HqEnv) -> Popen:
    return hq_env.start_server(
        env={
            "HQ_AUTOALLOC_REFRESH_INTERVAL_MS": "100",
            "HQ_AUTOALLOC_MAX_SCHEDULE_DELAY_MS": "100",
            "HQ_AUTOALLOC_SCHEDULE_TICK_INTERVAL_MS": "100",
        }
    )

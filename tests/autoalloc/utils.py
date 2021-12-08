from typing import List, Optional

from ..conftest import HqEnv
from ..utils.wait import wait_until


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


def add_queue(
    hq_env: HqEnv,
    manager="pbs",
    name: Optional[str] = "foo",
    backlog=1,
    workers_per_alloc=1,
    additional_args=None,
    time_limit=None,
) -> str:
    args = ["alloc", "add", manager]
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
    if additional_args is not None:
        args.append("--")
        args.extend(additional_args.split(" "))

    return hq_env.command(args)


def prepare_tasks(hq_env: HqEnv, count=1000):
    hq_env.command(["submit", f"--array=0-{count}", "sleep", "1"])


def wait_for_event(hq_env: HqEnv, state: str):
    """
    Wait for auto allocation event with the given `state` to appear.
    """

    def wait():
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        return state in table.get_column_value("Event")

    wait_until(wait)


def remove_queue(hq_env: HqEnv, queue_id: int):
    args = ["alloc", "remove", str(queue_id)]
    return hq_env.command(args)

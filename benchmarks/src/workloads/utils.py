import datetime
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Callable, TYPE_CHECKING

from ..environment import Environment
from ..environment.hq import HqEnvironment
from ..environment.snake import SnakeEnvironment
from ..utils import activate_cwd
from ..utils.timing import Timings
from .workload import WorkloadExecutionResult

if TYPE_CHECKING:
    import distributed


def measure_hq_tasks(
    env: Environment,
    command: List[str],
    task_count: int,
    cpus_per_task=1,
    stdout="none",
    resources: Optional[Dict[str, int]] = None,
    workdir: Optional[Path] = None,
    additional_args: Optional[List[str]] = None,
) -> WorkloadExecutionResult:
    assert isinstance(env, HqEnvironment)

    args = [
        "submit",
        "--array",
        f"1-{task_count}",
        "--stderr",
        "none",
        "--wait",
        "--cpus",
        str(cpus_per_task),
    ]
    if stdout is not None:
        args.extend(["--stdout", stdout])

    resources = resources or {}
    for name, amount in resources.items():
        args += ["--resource", f"{name}={amount}"]
    if additional_args is not None:
        args.extend(additional_args)

    args += [
        "--",
        *command,
    ]

    if workdir is None:
        workdir = env.workdir
    with activate_cwd(workdir):
        env.submit(args)

    # Calculate the real duration of the job
    result = env.submit(
        ["job", "info", "last", "--output-mode", "json"], measured=False
    )
    metadata = json.loads(result.stdout.decode())[0]
    info = metadata["info"]["task_stats"]
    assert info["canceled"] == 0
    assert info["failed"] == 0
    assert info["running"] == 0
    assert info["waiting"] == 0
    assert info["finished"] == task_count
    start = parse_hq_time(metadata["started_at"])
    end = parse_hq_time(metadata["finished_at"])
    duration = end - start

    return create_result(duration.total_seconds())


def parse_hq_time(input: str) -> datetime.datetime:
    """
    Parses e.g. `'2024-06-16T20:01:19.44240123Z`.
    """
    return datetime.datetime.strptime(input[:26], "%Y-%m-%dT%H:%M:%S.%f")


def measure_snake_tasks(
    env: Environment, command: str, task_count: int, cpus_per_task=1
) -> WorkloadExecutionResult:
    assert isinstance(env, SnakeEnvironment)

    args = f"""rule all:
    input:
        expand("{env.workdir}/{{sample}}", sample=range({task_count}))

rule benchmark:
    output:
        "{{sample}}"
    shell:
        "{command}"
"""
    logging.debug(f"[Snake] Submitting {args}")

    timer = Timings()
    with timer.time():
        env.submit(args, cpus_per_task)
    return create_result(timer.duration())


def measure_dask_tasks(
    env: Environment, submit_fn: Callable[["distributed.Client"], None]
) -> WorkloadExecutionResult:
    from ..environment.dask import DaskEnvironment

    assert isinstance(env, DaskEnvironment)
    logging.debug("[Dask] Submitting")

    timer = Timings()
    with timer.time():
        submit_fn(env.get_client())
    return create_result(timer.duration())


def create_result(duration: float) -> WorkloadExecutionResult:
    return WorkloadExecutionResult(duration=duration)

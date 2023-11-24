import logging
from typing import Dict, List, Optional, Callable

from ..environment import Environment
from ..environment.hq import HqEnvironment
from ..environment.snake import SnakeEnvironment
from ..utils import activate_cwd
from ..utils.timing import Timings
from .workload import WorkloadExecutionResult


def measure_hq_tasks(
    env: Environment,
    command: List[str],
    task_count: int,
    cpus_per_task=1,
    resources: Optional[Dict[str, int]] = None,
) -> WorkloadExecutionResult:
    assert isinstance(env, HqEnvironment)

    args = [
        "submit",
        "--array",
        f"1-{task_count}",
        "--stdout",
        "none",
        "--stderr",
        "none",
        "--wait",
        "--cpus",
        str(cpus_per_task),
    ]

    resources = resources or {}
    for name, amount in resources.items():
        args += ["--resource", f"{name}={amount}"]

    args += [
        "--",
        *command,
    ]

    timer = Timings()
    with activate_cwd(env.workdir):
        with timer.time():
            env.submit(args)
    return create_result(timer.duration())


def measure_snake_tasks(env: Environment, command: str, task_count: int, cpus_per_task=1) -> WorkloadExecutionResult:
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


def measure_dask_tasks(env: Environment, submit_fn: Callable[["distributed.Client"], None]) -> WorkloadExecutionResult:
    from ..environment.dask import DaskEnvironment

    assert isinstance(env, DaskEnvironment)
    logging.debug(f"[Dask] Submitting")

    timer = Timings()
    with timer.time():
        submit_fn(env.get_client())
    return create_result(timer.duration())


def create_result(duration: float) -> WorkloadExecutionResult:
    return WorkloadExecutionResult(duration=duration)

import logging
from typing import Dict, List, Optional

from ..environment import Environment
from ..environment.hq import HqEnvironment
from ..environment.snake import SnakeEnvironment
from ..environment.merlin import MerlinEnvironment
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
    for (name, amount) in resources.items():
        args += ["--resource", f"{name}={amount}"]

    args += [
        "--",
        *command,
    ]

    logging.debug(f"[HQ] Submitting {' '.join(args)}")

    timer = Timings()
    with activate_cwd(env.workdir):
        with timer.time():
            env.submit(args)
    return result(timer.duration())


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
    return result(timer.duration())


def measure_merlin_tasks(
    env: Environment, file_function, command: str, task_count: int
) -> WorkloadExecutionResult:
    assert isinstance(env, MerlinEnvironment)

    args = f"""study:
    - name: step_1
      description: benchmark
      run:
          cmd: {command}
"""
    logging.debug(f"[Merlin] Submitting {args}")

    timer = Timings()
    with timer.time():
        file_function()
        env.submit(args)
    return result(timer.duration())


def result(duration: float) -> WorkloadExecutionResult:
    return WorkloadExecutionResult(duration=duration)

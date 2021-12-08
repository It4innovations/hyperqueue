import logging
from typing import Dict, List, Optional

from ..environment import Environment
from ..environment.hq import HqEnvironment
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


def result(duration: float) -> WorkloadExecutionResult:
    return WorkloadExecutionResult(duration=duration)

import logging
from typing import List

from .workload import WorkloadExecutionResult
from ..environment import Environment
from ..environment.hq import HqEnvironment
from ..utils import activate_cwd
from ..utils.timing import Timings


def measure_hq_tasks(
        env: Environment,
        command: List[str],
        task_count: int,
        cpus_per_task=1
) -> WorkloadExecutionResult:
    assert isinstance(env, HqEnvironment)

    args = [
        "submit",
        "--array", f"1-{task_count}",
        "--stdout", "none",
        "--stderr", "none",
        "--wait",
        "--cpus", str(cpus_per_task),
        "--",
        *command
    ]
    logging.debug(f"[HQ] Submitting {' '.join(args)}")

    timer = Timings()
    with activate_cwd(env.workdir):
        with timer.time():
            env.submit(args)
    return result(timer.duration())


def result(duration: float) -> WorkloadExecutionResult:
    return WorkloadExecutionResult(duration=duration)

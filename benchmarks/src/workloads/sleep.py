from abc import ABC
from typing import Any, Dict

from ..environment.hq import HqEnvironment
from ..environment.snake import SnakeEnvironment
from ..environment.merlin import MerlinEnvironment
from .utils import measure_hq_tasks, measure_snake_tasks, measure_merlin_tasks
from .workload import Workload, WorkloadExecutionResult


class Sleep(Workload, ABC):
    def __init__(self, task_count: int, sleep_duration=0):
        self.task_count = task_count
        self.sleep_duration = sleep_duration

    def parameters(self) -> Dict[str, Any]:
        return dict(task_count=self.task_count, duration=self.sleep_duration)

    def name(self) -> str:
        return "sleep"


class SleepHQ(Sleep):
    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        return measure_hq_tasks(
            env, ["sleep", str(self.sleep_duration)], task_count=self.task_count
        )


class SleepSnake(Sleep):
    def execute(self, env: SnakeEnvironment) -> WorkloadExecutionResult:
        return measure_snake_tasks(
            env,
            f"sleep {self.sleep_duration}; echo '' > {{output}}",
            task_count=self.task_count,
        )


class SleepMerlin(Sleep):
    def execute(self, env: MerlinEnvironment) -> WorkloadExecutionResult:
        # Function that creates csv file with IDs that will be used in command
        def sleep_samples():
            results = [str(self.sleep_duration) + "\n" for i in range(self.task_count)]
            results = "".join(results)
            with open(env.merlinsamples, "w") as f:
                f.write(results)

        return measure_merlin_tasks(
            env,
            sleep_samples,
            f"sleep '$(ID)'",
            task_count=self.task_count,
        )

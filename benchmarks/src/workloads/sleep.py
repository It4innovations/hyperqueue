from abc import ABC
from typing import Any, Dict

from ..environment.hq import HqEnvironment
from .utils import measure_hq_tasks, measure_snake_tasks
from .workload import Workload, WorkloadExecutionResult
from ..environment.snake import SnakeEnvironment


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
    def execute(self, env: SnakeEnvironment, task_count: int, sleep_duration=0) -> WorkloadExecutionResult:
        return measure_snake_tasks(
            env, f"sleep {sleep_duration}; echo '' > {{output}}", task_count=task_count
        )

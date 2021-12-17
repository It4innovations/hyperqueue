from abc import ABC
from typing import Any, Dict

from ..environment.hq import HqEnvironment
from ..environment.snake import SnakeEnvironment
from .utils import measure_hq_tasks, measure_snake_tasks
from .workload import Workload, WorkloadExecutionResult


class Write(Workload, ABC):
    def __init__(self, task_count: int):
        self.task_count = task_count

    def parameters(self) -> Dict[str, Any]:
        return dict(task_count=self.task_count)

    def name(self) -> str:
        return "write"


class WriteHQ(Write):
    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        return measure_hq_tasks(
            env,
            ["echo", "benchmark"],
            task_count=self.task_count,
            stdout=True,
            stderr=False,
        )


class WriteSnake(Write):
    def execute(self, env: SnakeEnvironment) -> WorkloadExecutionResult:
        return measure_snake_tasks(
            env,
            f"echo benchmark > {{output}}",
            task_count=self.task_count,
        )

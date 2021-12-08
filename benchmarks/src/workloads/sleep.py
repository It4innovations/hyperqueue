from abc import ABC
from typing import Any, Dict

from ..environment.hq import HqEnvironment
from .utils import measure_hq_tasks
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

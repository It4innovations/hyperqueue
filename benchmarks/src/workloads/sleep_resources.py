from abc import ABC
from typing import Any, Dict

from ..environment.hq import HqEnvironment
from .utils import measure_hq_tasks
from .workload import Workload, WorkloadExecutionResult


class SleepWithResources(Workload, ABC):
    def __init__(self, task_count: int, resources: Dict[str, Any], sleep_duration=0):
        self.task_count = task_count
        self.resources = resources
        self.sleep_duration = sleep_duration

    def parameters(self) -> Dict[str, Any]:
        return dict(
            task_count=self.task_count,
            resources=self.resources,
            duration=self.sleep_duration,
        )

    def name(self) -> str:
        return "sleep-with-resources"


class SleepWithResourcesHQ(SleepWithResources):
    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        return measure_hq_tasks(
            env,
            ["sleep", str(self.sleep_duration)],
            task_count=self.task_count,
            resources=self.resources,
            stdout=False,
        )

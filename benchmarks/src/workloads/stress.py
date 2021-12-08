import multiprocessing
from abc import ABC
from typing import Any, Dict, Optional

from ..environment import Environment
from ..utils import is_binary_available
from .utils import measure_hq_tasks
from .workload import Workload, WorkloadExecutionResult


class Stress(Workload, ABC):
    def __init__(
        self,
        task_count: int,
        cpu_count: Optional[int] = None,
        stress_duration=1,
    ):
        self.task_count = task_count
        self.cpu_count = cpu_count
        self.stress_duration = stress_duration

    def name(self) -> str:
        return "stress"

    def parameters(self) -> Dict[str, Any]:
        return dict(
            task_count=self.task_count,
            cpu_count=self.cpu_count,
            duration=self.stress_duration,
        )

    def execute(self, env: Environment) -> WorkloadExecutionResult:
        assert is_binary_available("stress")

        cpu_count = self.cpu_count or multiprocessing.cpu_count()
        return self.compute(
            env,
            task_count=self.task_count,
            cpu_count=cpu_count,
            stress_duration=self.stress_duration,
        )

    def compute(
        self, env: Environment, task_count: int, cpu_count: int, stress_duration: int
    ) -> WorkloadExecutionResult:
        raise NotImplementedError


class StressHQ(Stress):
    def compute(
        self, env: Environment, task_count: int, cpu_count: int, stress_duration: int
    ) -> WorkloadExecutionResult:
        return measure_hq_tasks(
            env,
            ["stress", "--cpu", str(cpu_count), "--timeout", str(stress_duration)],
            task_count=task_count,
            cpus_per_task=cpu_count,
        )

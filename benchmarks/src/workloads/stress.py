import multiprocessing
from typing import Optional

from ..environment import Environment
from ..utils import is_binary_available
from .utils import measure_hq_tasks
from .workload import Workload, WorkloadExecutionResult


class Stress(Workload):
    def __init__(self):
        assert is_binary_available("stress")

    def execute(
        self,
        env: Environment,
        task_count: int,
        cpu_count: Optional[int] = None,
        stress_duration=1,
    ) -> WorkloadExecutionResult:
        cpu_count = cpu_count or multiprocessing.cpu_count()
        return self.compute(
            env,
            task_count=task_count,
            cpu_count=cpu_count,
            stress_duration=stress_duration,
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

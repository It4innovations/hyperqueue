from ..environment.hq import HqEnvironment
from .utils import measure_hq_tasks
from .workload import Workload, WorkloadExecutionResult


class SleepWithResources(Workload):
    pass


class SleepWithResourcesHQ(SleepWithResources):
    def execute(
        self, env: HqEnvironment, task_count: int, sleep_duration=0, resources=None
    ) -> WorkloadExecutionResult:
        return measure_hq_tasks(
            env, ["sleep", str(sleep_duration)],
            task_count=task_count,
            resources=resources
        )

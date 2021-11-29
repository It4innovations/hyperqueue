from .utils import measure_hq_tasks
from .workload import Workload, WorkloadExecutionResult
from ..environment.hq import HqEnvironment


class Sleep(Workload):
    pass


class SleepHQ(Sleep):
    def execute(
            self,
            env: HqEnvironment,
            task_count: int,
            sleep_duration=0
    ) -> WorkloadExecutionResult:
        return measure_hq_tasks(env, ["sleep", str(sleep_duration)], task_count=task_count)

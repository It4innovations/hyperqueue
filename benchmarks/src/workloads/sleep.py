from .workload import Workload, WorkloadExecutionResult, result
from ..environment import Environment
from ..environment.hq import HqEnvironment
from ..utils.timing import Timings


class Sleep(Workload):
    def name(self) -> str:
        return "sleep"

    def compute(self, env: Environment, task_count: int, sleep_duration=0) -> WorkloadExecutionResult:
        raise NotImplementedError()


class SleepHQ(Sleep):
    def compute(self, env: HqEnvironment, task_count: int, sleep_duration=0) -> WorkloadExecutionResult:
        assert isinstance(env, HqEnvironment)

        timer = Timings()
        with timer.time():
            env.submit([
                "submit",
                "--array", f"1-{task_count}",
                "--stdout", "none",
                "--stderr", "none",
                "--wait",
                "--",
                "sleep", str(int(sleep_duration))
            ])
        return result(timer.duration())

import shutil
import time
from abc import ABC
from typing import Any, Dict

from .utils import measure_hq_tasks, measure_snake_tasks, measure_dask_tasks
from .workload import Workload, WorkloadExecutionResult
from ..environment.dask import DaskEnvironment
from ..environment.hq import HqEnvironment
from ..environment.snake import SnakeEnvironment


class Sleep(Workload, ABC):
    def __init__(self, task_count: int, sleep_duration: float = 0.0):
        self.task_count = task_count
        self.sleep_duration = sleep_duration
        self.sleep_bin = shutil.which("sleep")
        assert self.sleep_bin is not None

    def parameters(self) -> Dict[str, Any]:
        return dict(task_count=self.task_count, duration=self.sleep_duration)

    def name(self) -> str:
        return "sleep"


class SleepHQ(Sleep):
    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        return measure_hq_tasks(
            env, [self.sleep_bin, str(self.sleep_duration)], task_count=self.task_count
        )


class SleepSnake(Sleep):
    def execute(self, env: SnakeEnvironment) -> WorkloadExecutionResult:
        return measure_snake_tasks(
            env,
            f"{self.sleep_bin} {self.sleep_duration}; echo '' > {{output}}",
            task_count=self.task_count,
        )


def sleep(duration: float):
    time.sleep(float(duration))


class SleepDask(Sleep):
    def execute(self, env: DaskEnvironment) -> WorkloadExecutionResult:
        from distributed import Client

        def run(client: Client):
            tasks = client.map(
                sleep, [self.sleep_duration] * self.task_count, pure=False
            )
            client.gather(tasks)

        return measure_dask_tasks(env, run)


class SleepDaskSpawn(Sleep):
    def name(self) -> str:
        return "sleep-spawn"

    def execute(self, env: DaskEnvironment) -> WorkloadExecutionResult:
        from distributed import Client

        def run(client: Client):
            def sleep(duration: float):
                import subprocess

                subprocess.run([self.sleep_bin, str(duration)])

            tasks = [
                client.submit(sleep, self.sleep_duration, pure=False)
                for _ in range(self.task_count)
            ]
            client.gather(tasks)

        return measure_dask_tasks(env, run)

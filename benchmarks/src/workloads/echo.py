from abc import ABC
from typing import Any, Dict

from ..environment.hq import HqEnvironment
from ..environment.snake import SnakeEnvironment
from ..environment.merlin import MerlinEnvironment
from .utils import measure_hq_tasks, measure_snake_tasks, measure_merlin_tasks
from .workload import Workload, WorkloadExecutionResult


class Echo(Workload, ABC):
    def __init__(self, task_count: int, text="0"):
        self.task_count = task_count
        self.text = text

    def parameters(self) -> Dict[str, Any]:
        return dict(task_count=self.task_count, text=self.text)

    def name(self) -> str:
        return "echo"


class EchoHQ(Echo):
    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        print(["echo", str(self.text)])
        return measure_hq_tasks(
            env, ["echo", self.text], task_count=self.task_count, stdout=True
        )


class EchoSnake(Echo):
    def execute(self, env: SnakeEnvironment) -> WorkloadExecutionResult:
        return measure_snake_tasks(
            env,
            f"echo {self.text} > {{output}}",
            task_count=self.task_count,
        )


class EchoMerlin(Echo):
    def execute(self, env: MerlinEnvironment) -> WorkloadExecutionResult:
        # Function that creates csv file with IDs that will be used in command
        def sleep_samples():
            results = [str(self.text) + "\n" for i in range(self.task_count)]
            results = "".join(results)
            with open(env.merlinsamples, "w") as f:
                f.write(results)

        return measure_merlin_tasks(
            env,
            sleep_samples,
            "echo '$(ID)'",
            task_count=self.task_count,
        )

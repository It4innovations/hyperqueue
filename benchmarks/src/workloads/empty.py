from typing import Dict, Any

from .utils import measure_dask_tasks
from .workload import Workload, WorkloadExecutionResult
from ..environment.dask import DaskEnvironment


def empty():
    pass


class EmptyDask(Workload):
    def __init__(self, task_count: int):
        self.task_count = task_count

    def name(self) -> str:
        return "empty"

    def parameters(self) -> Dict[str, Any]:
        return {"task_count": self.task_count}

    def execute(self, env: DaskEnvironment) -> WorkloadExecutionResult:
        from distributed import Client

        def run(client: Client):
            tasks = [client.submit(empty, pure=False) for _ in range(self.task_count)]
            client.gather(tasks)

        return measure_dask_tasks(env, run)

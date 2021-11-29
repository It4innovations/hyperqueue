import dataclasses

from ..environment import Environment


@dataclasses.dataclass
class WorkloadExecutionResult:
    duration: float


class Workload:
    def execute(self, env: Environment, *args, **kwargs) -> WorkloadExecutionResult:
        raise NotImplementedError

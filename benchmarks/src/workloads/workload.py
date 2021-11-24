import dataclasses

from ..environment import Environment


@dataclasses.dataclass
class WorkloadExecutionResult:
    duration: float


class Workload:
    def name(self) -> str:
        raise NotImplementedError()

    def execute(self, env: Environment, *args, **kwargs) -> WorkloadExecutionResult:
        return self.compute(env, *args, **kwargs)

    def compute(self, env: Environment, *args, **kwargs) -> WorkloadExecutionResult:
        raise NotImplementedError()


def result(duration: float) -> WorkloadExecutionResult:
    return WorkloadExecutionResult(duration=duration)

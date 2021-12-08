import dataclasses
from typing import Any, Dict

from ..environment import Environment


@dataclasses.dataclass
class WorkloadExecutionResult:
    duration: float


class Workload:
    def name(self) -> str:
        raise NotImplementedError

    def parameters(self) -> Dict[str, Any]:
        raise NotImplementedError

    def execute(self, env: Environment) -> WorkloadExecutionResult:
        raise NotImplementedError

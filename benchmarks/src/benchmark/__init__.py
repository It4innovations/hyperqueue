import dataclasses
from typing import Any, Dict

from ..environment import Environment
from ..workloads import Workload


@dataclasses.dataclass
class BenchmarkInstance:
    environment: Environment
    workload: Workload
    workload_params: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})

from typing import Dict, Any

import dataclasses

from ..environment import Environment
from ..workloads import Workload


@dataclasses.dataclass
class BenchmarkInstance:
    environment: Environment
    workload: Workload
    workload_params: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})

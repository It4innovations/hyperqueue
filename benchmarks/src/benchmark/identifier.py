import dataclasses
from typing import Any, Callable, Dict, List, Optional

from mashumaro import DataClassDictMixin


@dataclasses.dataclass(frozen=True)
class BenchmarkIdentifier(DataClassDictMixin):
    # Name of the workload
    workload: str
    # Environment type
    environment: str
    # Parameters of the benchmark environment (# of workers, etc.)
    environment_params: Dict[str, Any]
    # Number of the benchmark run
    index: int = 0
    # Parameters passed to the workload function
    workload_params: Dict[str, Any] = dataclasses.field(default_factory=dict)
    # Additional metadata describing the benchmark
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def timeout(self) -> Optional[float]:
        return self.metadata.get("timeout")

    def workdir(self) -> Optional[str]:
        return self.metadata.get("workdir")


def repeat_benchmark(
    count: int, create_fn: Callable[[int], BenchmarkIdentifier]
) -> List[BenchmarkIdentifier]:
    return [create_fn(index) for index in range(count)]

import dataclasses
from pathlib import Path

from ..benchmark.identifier import BenchmarkDescriptor
from ..benchmark.result import BenchmarkResult


@dataclasses.dataclass
class BenchmarkContext:
    workdir: Path
    timeout_s: float

    def __post_init__(self):
        self.workdir = self.workdir.resolve()


class BenchmarkExecutor:
    def execute(
        self, benchmark: BenchmarkDescriptor, ctx: BenchmarkContext
    ) -> BenchmarkResult:
        raise NotImplementedError

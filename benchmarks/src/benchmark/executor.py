import dataclasses
import traceback
from pathlib import Path

from ..utils.timing import TimeoutException, with_timeout
from ..workloads.workload import WorkloadExecutionResult
from .identifier import BenchmarkDescriptor
from .result import BenchmarkResult, Failure, Success, Timeout

DEFAULT_TIMEOUT_S = 180.0


@dataclasses.dataclass(frozen=True)
class BenchmarkContext:
    workdir: Path
    timeout_s: float = DEFAULT_TIMEOUT_S


class BenchmarkExecutor:
    def execute(
        self, benchmark: BenchmarkDescriptor, ctx: BenchmarkContext
    ) -> BenchmarkResult:
        return execute_benchmark(benchmark, ctx)


def execute_benchmark(
    descriptor: BenchmarkDescriptor, ctx: BenchmarkContext
) -> BenchmarkResult:
    env = descriptor.env_descriptor.create_environment(ctx.workdir)
    workload = descriptor.workload

    def run() -> WorkloadExecutionResult:
        return workload.execute(env)

    try:
        with env:
            result = with_timeout(run, timeout_s=ctx.timeout_s)
            return Success(duration=result.duration)
    except TimeoutException:
        return Timeout(ctx.timeout_s)
    except BaseException as e:
        return Failure(e, traceback.format_exc())

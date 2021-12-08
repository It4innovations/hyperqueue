import traceback

from ..benchmark.identifier import BenchmarkDescriptor
from ..benchmark.result import BenchmarkResult, Failure, Success, Timeout
from ..utils.timing import TimeoutException, with_timeout
from ..workloads.workload import WorkloadExecutionResult
from .executor import BenchmarkContext, BenchmarkExecutor


class LocalBenchmarkExecutor(BenchmarkExecutor):
    """Executes benchmarks in the current process"""

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
    except BaseException:
        return Failure(traceback.format_exc())

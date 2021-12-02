import traceback

from ..utils.timing import TimeoutException, with_timeout
from ..workloads.workload import WorkloadExecutionResult
from . import BenchmarkInstance
from .result import BenchmarkResult, Failure, Success, Timeout


class BenchmarkExecutor:
    def execute(
        self, benchmark: BenchmarkInstance, timeout_s: float
    ) -> BenchmarkResult:
        return execute_benchmark(benchmark, timeout_s=timeout_s)


DEFAULT_TIMEOUT_S = 180.0


def execute_benchmark(
    benchmark: BenchmarkInstance, timeout_s: float = DEFAULT_TIMEOUT_S
) -> BenchmarkResult:
    env = benchmark.environment

    def run() -> WorkloadExecutionResult:
        return benchmark.workload.execute(env, **benchmark.workload_params)

    try:
        with env:
            result = with_timeout(run, timeout_s=timeout_s)
            return Success(duration=result.duration)
    except TimeoutException:
        return Timeout(timeout_s)
    except BaseException as e:
        return Failure(e, traceback.format_exc())

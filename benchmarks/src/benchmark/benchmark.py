import traceback
from typing import Dict, Any

import dataclasses

from .result import BenchmarkResult, Failure, Timeout, Success
from ..environment import Environment
from ..utils.timing import with_timeout, TimeoutException
from ..workloads import Workload, WorkloadExecutionResult

DEFAULT_TIMEOUT_S = 180.0


@dataclasses.dataclass
class BenchmarkInstance:
    environment: Environment
    workload: Workload
    workload_params: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})


def execute_benchmark(
        benchmark: BenchmarkInstance,
        timeout_s: float = DEFAULT_TIMEOUT_S
) -> BenchmarkResult:
    env = benchmark.environment

    try:
        env.start()

        def run() -> WorkloadExecutionResult:
            return benchmark.workload.execute(env, **benchmark.workload_params)

        try:
            result = with_timeout(run, timeout_s=timeout_s)
            return Success(duration=result.duration)
        finally:
            env.stop()
    except TimeoutException:
        return Timeout(timeout_s)
    except BaseException as e:
        return Failure(e, traceback.format_exc())

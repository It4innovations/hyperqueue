from typing import Any, Dict, List

from .benchmark.identifier import BenchmarkIdentifier, repeat_benchmark
from .build.hq import BuiltBinary


def benchmark(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    return dict(workload=name, workload_params=args)


# Basic benchmarks
def sleep_benchmarks():
    for task_count in (10, 100, 1000, 10000, 100000):
        yield benchmark("sleep", dict(task_count=task_count))


def sleep_resource_benchmarks():
    for task_count in (10, 100, 1000, 10000, 100000):
        yield benchmark("sleep_resources", dict(
            task_count=task_count,
            resources=dict(
                resource1=10
            )
        ))


# HQ environment
def hq_metadata(
        binary: str,
        monitoring=True,
        profile=False,
        timeout=180,
        workers=None,
        **kwargs
) -> Dict[str, Any]:
    return dict(
        monitoring=monitoring,
        timeout=timeout,
        profile=profile,
        hq=dict(binary=binary, workers=workers),
        **kwargs
    )


def hq_environment(worker_count=1, **kwargs) -> Dict[str, Any]:
    return dict(
        environment="hq",
        environment_params=dict(worker_count=worker_count, **kwargs),
    )


def create_basic_hq_benchmarks(
        artifacts: List[BuiltBinary], repeat_count=2
) -> List[BenchmarkIdentifier]:
    identifiers = []
    benchmarks = list(sleep_benchmarks())

    for artifact in artifacts:
        for bench in benchmarks:
            identifiers += repeat_benchmark(
                    repeat_count,
                    lambda index: BenchmarkIdentifier(
                        **hq_environment(
                            zero_worker=artifact.config.zero_worker,
                            tag=artifact.config.git_ref,
                        ),
                        **bench,
                        metadata=hq_metadata(binary=str(artifact.binary_path)),
                        index=index,
                    ),
                )
    return identifiers


def create_resources_hq_benchmarks(
        artifacts: List[BuiltBinary], repeat_count=2
) -> List[BenchmarkIdentifier]:
    identifiers = []
    benchmarks = list(sleep_resource_benchmarks())

    for artifact in artifacts:
        for bench in benchmarks:
            identifiers += repeat_benchmark(
                repeat_count,
                lambda index: BenchmarkIdentifier(
                    **hq_environment(
                        zero_worker=artifact.config.zero_worker,
                        tag=artifact.config.git_ref,
                    ),
                    **bench,
                    metadata=hq_metadata(
                        binary=str(artifact.binary_path),
                        workers=[dict(
                            resources={
                                "resource1": dict(
                                    type="sum",
                                    amount=1000
                                )
                            }
                        )]
                    ),
                    index=index,
                ),
            )
    return identifiers

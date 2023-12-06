from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Type

from .benchmark.identifier import BenchmarkDescriptor
from .build.hq import BuiltBinary, iterate_binaries, BuildConfig
from .clusterutils import ClusterInfo
from .clusterutils.node_list import Local
from .clusterutils.profiler import FlamegraphProfiler
from .environment.dask import DaskClusterInfo, DaskWorkerConfig
from .environment.hq import HqClusterInfo, HqSumWorkerResource, HqWorkerConfig
from .workloads import Workload
from .workloads.sleep import Sleep, SleepHQ
from .workloads.sleep_resources import SleepWithResourcesHQ


def benchmark(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    return dict(workload=name, workload_params=args)


# Basic workloads
def sleep_workloads(cls: Type[Sleep] = SleepHQ) -> Iterable[Workload]:
    for task_count in (10, 100, 1000, 10000):
        yield cls(task_count=task_count)


def sleep_resource_benchmarks() -> Iterable[Workload]:
    for task_count in (10, 100, 1000, 10000):
        yield SleepWithResourcesHQ(task_count=task_count, resources=dict(resource1=10))


# HQ environment
def hq_env_local(
    binary: Path,
    workers: Optional[List[HqWorkerConfig]] = None,
    environment: Optional[Dict[str, Any]] = None,
    monitoring=True,
    profile=False,
    debug=False,
) -> HqClusterInfo:
    workers = workers or [HqWorkerConfig()]
    environment = environment or {}

    profilers = []
    if profile:
        profilers = [FlamegraphProfiler(99)]
        # profilers = [PerfEventsProfiler()]

    return HqClusterInfo(
        cluster=ClusterInfo(Local(), monitor_nodes=monitoring),
        binary=binary,
        workers=workers,
        server_profilers=profilers,
        worker_profilers=profilers,
        debug=debug,
        environment_params=environment,
    )


def _create_hq_benchmarks(
    artifacts: List[BuiltBinary],
    workers: List[HqWorkerConfig],
    workloads: List[Workload],
    repeat_count: int = 1,
) -> List[BenchmarkDescriptor]:
    descriptors = []

    for artifact in artifacts:
        for workload in workloads:
            descriptors.append(
                BenchmarkDescriptor(
                    env_descriptor=hq_env_local(
                        artifact.binary_path,
                        environment=dict(
                            zero_worker=artifact.config.zero_worker,
                            tag=artifact.config.git_ref,
                        ),
                        workers=workers,
                    ),
                    workload=workload,
                    repeat_count=repeat_count,
                )
            )
    return descriptors


def create_basic_hq_benchmarks(artifacts: List[BuiltBinary], repeat_count=2) -> List[BenchmarkDescriptor]:
    workloads = list(sleep_workloads())
    return _create_hq_benchmarks(artifacts, [HqWorkerConfig()], workloads, repeat_count=repeat_count)


def create_resources_hq_benchmarks(artifacts: List[BuiltBinary], repeat_count=2) -> List[BenchmarkDescriptor]:
    workloads = list(sleep_resource_benchmarks())
    return _create_hq_benchmarks(
        artifacts,
        [HqWorkerConfig(resources={"resource1": HqSumWorkerResource(amount=1000)})],
        workloads,
        repeat_count=repeat_count,
    )


def single_node_hq_cluster(hq_path: Path, worker_threads=128, **env) -> HqClusterInfo:
    return HqClusterInfo(
        cluster=ClusterInfo(monitor_nodes=True, node_list=Local()),
        environment_params=dict(worker_threads=worker_threads, **env),
        workers=[HqWorkerConfig(cpus=worker_threads)],
        binary=hq_path,
        worker_profilers=[],
    )


def single_node_dask_cluster(worker_threads=128) -> DaskClusterInfo:
    return DaskClusterInfo(
        cluster_info=ClusterInfo(monitor_nodes=True, node_list=Local()),
        environment_params=dict(worker_threads=worker_threads),
        workers=[DaskWorkerConfig(cores=worker_threads)],
    )


def get_hq_binary(**kwargs) -> Path:
    return next(iterate_binaries([BuildConfig(**kwargs)])).binary_path

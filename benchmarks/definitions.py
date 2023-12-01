from pathlib import Path

from src.build.hq import BuildConfig, iterate_binaries
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local
from src.environment.dask import DaskWorkerConfig, DaskClusterInfo
from src.environment.hq import HqClusterInfo, HqWorkerConfig


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

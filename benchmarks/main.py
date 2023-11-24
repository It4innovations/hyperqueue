import itertools
import logging
import multiprocessing
from pathlib import Path
from typing import List, Optional

import dataclasses
import typer

from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import create_basic_hq_benchmarks
from src.build.hq import BuildConfig, iterate_binaries
from src.build.repository import TAG_WORKSPACE
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local
from src.environment import EnvironmentDescriptor
from src.environment.dask import DaskClusterInfo, DaskWorkerConfig
from src.environment.hq import HqClusterInfo, HqWorkerConfig
from src.environment.snake import SnakeEnvironmentDescriptor
from src.utils.benchmark import run_benchmarks_with_postprocessing
from src.workloads import Workload
from src.workloads.sleep import SleepHQ, SleepSnake, SleepDask

app = typer.Typer()


@app.command()
def compare_hq_version(baseline: str, modified: Optional[str] = None, zero_worker: Optional[bool] = False):
    """
    Compares the performance of two HQ versions.
    If `modified` is not set, the current git workspace version will be used.
    """
    modified = modified if modified else TAG_WORKSPACE
    configs = [
        BuildConfig(git_ref=modified),
        BuildConfig(git_ref=baseline),
    ]
    if zero_worker:
        configs += [dataclasses.replace(config, zero_worker=True) for config in configs]

    artifacts = list(iterate_binaries(configs))
    identifiers = create_basic_hq_benchmarks(artifacts)
    workdir = Path(f"benchmark/cmp-{baseline}-{modified}")
    run_benchmarks_with_postprocessing(workdir, identifiers)


@app.command()
def compare_zw():
    """
    Compares the performance of HQ vs zero-worker HQ.
    """
    artifacts = list(iterate_binaries([BuildConfig(), BuildConfig(zero_worker=True)]))
    identifiers = create_basic_hq_benchmarks(artifacts)
    workdir = Path("benchmark/zw")
    run_benchmarks_with_postprocessing(workdir, identifiers)


@app.command()
def sleep():
    """
    Compare the sleep benchmarks between various tools.
    """
    hq_path = list(iterate_binaries([BuildConfig()]))[0].binary_path
    workdir = Path("benchmark/sleep")

    task_counts = (10, 100, 1000)
    descriptions = []

    def add_product(workloads: List[Workload], environments: List[EnvironmentDescriptor]):
        for env, workload in itertools.product(environments, workloads):
            descriptions.append(BenchmarkDescriptor(env_descriptor=env, workload=workload))

    add_product([SleepSnake(tc) for tc in task_counts], [SnakeEnvironmentDescriptor()])
    add_product(
        [SleepDask(tc) for tc in task_counts],
        [
            DaskClusterInfo(
                cluster_info=ClusterInfo(monitor_nodes=True, node_list=Local()),
                workers=[DaskWorkerConfig(cores=multiprocessing.cpu_count())],
            )
        ],
    )
    add_product(
        [SleepHQ(tc) for tc in task_counts],
        [
            HqClusterInfo(
                cluster=ClusterInfo(monitor_nodes=True, node_list=Local()),
                environment_params=dict(worker_count=1),
                workers=[HqWorkerConfig()],
                binary=hq_path,
            )
        ],
    )

    run_benchmarks_with_postprocessing(workdir, descriptions)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s.%(msecs)03d:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()

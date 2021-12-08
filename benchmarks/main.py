import dataclasses
import logging
from pathlib import Path
from typing import Optional

import typer

from src.benchmark import BenchmarkInstance
from src.benchmark.executor import execute_benchmark
from src.environment.snake import SnakeEnvironment, SnakeClusterInfo
from src.workloads.sleep import SleepSnake

from src.benchmark_defs import create_basic_hq_benchmarks
from src.build.hq import BuildConfig, iterate_binaries
from src.build.repository import TAG_WORKSPACE
from src.utils.benchmark import run_benchmarks_with_postprocessing

app = typer.Typer()


@app.command()
def compare_hq_version(
    baseline: str, modified: Optional[str] = None, zero_worker: Optional[bool] = False
):
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
def snake_sleep():
    """
    Simple snake sleep bench from instance
    """
    workdir = Path("benchmark/snake")
    snake = SnakeEnvironment(SnakeClusterInfo(workdir))

    # Create bench instance
    snake_bench = BenchmarkInstance(
        workload=SleepSnake(),
        environment=snake,
        workload_params={'task_count': 150},
    )
    execute_benchmark(snake_bench)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()

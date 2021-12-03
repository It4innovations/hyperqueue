import logging
from pathlib import Path
from typing import Optional

import dataclasses
import typer

from src.benchmark_set import create_basic_hq_benchmarks
from src.build.hq import BuildConfig, iterate_binaries
from src.build.repository import TAG_WORKSPACE
from src.benchutils import run_benchmarks_with_postprocessing

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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()

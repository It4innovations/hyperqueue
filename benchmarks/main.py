import dataclasses
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from src.benchmark.identifier import BenchmarkIdentifier, repeat_benchmark
from src.build.hq import BuildConfig, BuiltBinary, iterate_binaries
from src.build.repository import TAG_WORKSPACE
from src.materialization import run_benchmark_suite
from src.postprocessing.overview import generate_summary_html, generate_summary_text


def benchmark(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    return dict(workload=name, workload_params=args)


def sleep_benchmarks():
    for task_count in (10, 100, 1000, 10000):
        yield benchmark("sleep", dict(task_count=task_count))


def hq_metadata(
    binary: str, monitoring=True, profile=False, timeout=180
) -> Dict[str, Any]:
    return dict(
        monitoring=monitoring,
        timeout=timeout,
        profile=profile,
        hq=dict(binary=binary, workers=[None]),
    )


def hq_environment(worker_count=1, **kwargs) -> Dict[str, Any]:
    return dict(
        environment="hq",
        environment_params=dict(worker_count=worker_count, **kwargs),
    )


def run_benchmarks(workdir: Path, identifiers: List[BenchmarkIdentifier]):
    database = run_benchmark_suite(workdir, identifiers)

    summary_txt = workdir / "summary.txt"
    generate_summary_text(database, summary_txt)

    summary_html = workdir / "summary"
    html_index = generate_summary_html(database, summary_html)

    logging.info(f"You can find text summary at {summary_txt}")
    logging.info(f"You can find HTML summary at {html_index}")


def create_basic_hq_benchmarks(
    artifacts: List[BuiltBinary], repeat_count=2
) -> List[BenchmarkIdentifier]:
    identifiers = []
    for artifact in artifacts:
        for bench in sleep_benchmarks():
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
    run_benchmarks(workdir, identifiers)


@app.command()
def compare_zw():
    """
    Compares the performance of HQ vs zero-worker HQ.
    """
    artifacts = list(iterate_binaries([BuildConfig(), BuildConfig(zero_worker=True)]))
    identifiers = create_basic_hq_benchmarks(artifacts)
    workdir = Path("benchmark/zw")
    run_benchmarks(workdir, identifiers)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    app()

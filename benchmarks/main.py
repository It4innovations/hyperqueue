import logging
from pathlib import Path
from typing import Any, Dict

from src.benchmark.identifier import BenchmarkIdentifier, repeat_benchmark
from src.build.hq import iterate_binaries, BuildConfig
from src.materialization import run_benchmark_suite
from src.postprocessing.overview import generate_summary_text, generate_summary_html


def benchmark(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    return dict(
        workload=name,
        workload_params=args
    )


def sleep_benchmarks():
    for task_count in (10, 100, 1000, 10000):
        yield benchmark("sleep", dict(task_count=task_count))


def hq_metadata(binary: str, monitoring=True, profile=False, timeout=180) -> Dict[str, Any]:
    return dict(
        monitoring=monitoring,
        timeout=timeout,
        profile=profile,
        hq=dict(
            binary=binary,
            workers=[None]
        ),
    )


def hq_environment(worker_count=1, zero_worker=False) -> Dict[str, Any]:
    return dict(
        environment="hq",
        environment_params=dict(worker_count=worker_count, zero_worker=zero_worker),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    identifiers = []

    repeat_count = 2
    for (config, binary) in iterate_binaries([BuildConfig(), BuildConfig(zero_worker=True)]):
        for bench in sleep_benchmarks():
            identifiers += repeat_benchmark(repeat_count, lambda index: BenchmarkIdentifier(
                **hq_environment(zero_worker=config.zero_worker),
                **bench,
                metadata=hq_metadata(binary=str(binary)),
                index=index
            ))

    workdir = Path("work/zw")
    database = run_benchmark_suite(workdir, identifiers)

    summary_txt = workdir / "summary.txt"
    generate_summary_text(database, summary_txt)

    summary_html = workdir / "summary"
    html_index = generate_summary_html(database, summary_html)

    logging.info(f"You can find text summary at {summary_txt}")
    logging.info(f"You can find HTML summary at {html_index}")

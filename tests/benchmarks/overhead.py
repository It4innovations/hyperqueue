import multiprocessing

import pytest

from ..conftest import run_hq_env, HqEnv


def task_overhead(env: HqEnv, task_count: int):
    env.command([
        "submit",
        "--stdout", "none",
        "--stderr", "none",
        "--wait",
        "--array", f"1-{task_count}",
        "--",
        "sleep", "0"
    ])


@pytest.mark.parametrize("task_count", (100, 1000, 10000))
def test_benchmark_task_overhead(benchmark, tmp_path, task_count):
    with run_hq_env(tmp_path, debug=False) as hq_env:
        hq_env.start_server()
        hq_env.start_workers(1, cpus=multiprocessing.cpu_count())
        benchmark(task_overhead, hq_env, task_count)

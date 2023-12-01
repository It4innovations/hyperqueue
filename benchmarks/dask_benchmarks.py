import datetime
from pathlib import Path
from typing import Iterable

from definitions import single_node_hq_cluster, single_node_dask_cluster, get_hq_binary
from src.analysis.chart import render_chart_to_png
from src.analysis.dataframe import DataFrameExtractor
from src.benchmark.database import Database, DatabaseRecord
from src.benchmark.identifier import BenchmarkDescriptor
from src.workloads import SleepHQ
from src.workloads.empty import EmptyDask
from src.workloads.sleep import SleepDask, SleepDaskSpawn
from test_case import TestCase, register_case, create_cli

cli = create_cli()


@register_case(cli)
class DaskVsHqSleep(TestCase):
    """
    Benchmark the duration of sleeping for 250s, divided between various amounts of tasks.
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary()
        timeout = datetime.timedelta(minutes=10)

        worker_threads = 16
        total_duration_single_thread = 250

        hq_env = single_node_hq_cluster(hq_path, worker_threads=worker_threads)
        dask_env = single_node_dask_cluster(worker_threads=worker_threads)

        task_counts = [100, 1000, 5000, 10000, 25000, 50000]

        types = [
            (hq_env, SleepHQ),
            (dask_env, SleepDask),
            (dask_env, SleepDaskSpawn),
        ]
        for env, workload_cls in types:
            for task_count in task_counts:
                sleep_duration = total_duration_single_thread / task_count
                workload = workload_cls(task_count=task_count, sleep_duration=sleep_duration)
                yield BenchmarkDescriptor(env_descriptor=env, workload=workload, timeout=timeout, repeat_count=2)

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        def parse_env(record: DatabaseRecord) -> str:
            if record.environment == "hq":
                return "hq"
            return f"dask-{'sleep' if record.workload == 'sleep' else 'sleep-spawn'}"

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("environment", parse_env)
            .transform("task-count", lambda r: r.workload_params["task_count"])
            .build()
        )

        ax = sns.scatterplot(df, x="task-count", y="duration", hue="environment", marker="o")
        ax.set(ylabel="Duration [s]", xlabel="Task count")
        # ax.set(yscale="log")
        render_chart_to_png(workdir / "dask-vs-hq-sleep.png")


@register_case(cli)
class DaskVsHqEmpty(TestCase):
    """
    Benchmark the duration of an empty task between HQ and Dask.
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary(zero_worker=True)
        timeout = datetime.timedelta(minutes=10)

        worker_threads = 16

        hq_env = single_node_hq_cluster(hq_path, worker_threads=worker_threads)
        dask_env = single_node_dask_cluster(worker_threads=worker_threads)

        repeat_count = 1
        task_counts = [100, 1000, 5000, 10000, 50000]

        for task_count in task_counts:
            workload = EmptyDask(task_count=task_count)
            yield BenchmarkDescriptor(
                env_descriptor=dask_env, workload=workload, timeout=timeout, repeat_count=repeat_count
            )
        for task_count in task_counts:
            workload = SleepHQ(task_count=task_count, sleep_duration=0)
            yield BenchmarkDescriptor(
                env_descriptor=hq_env, workload=workload, timeout=timeout, repeat_count=repeat_count
            )

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration", "environment")
            .transform("task-count", lambda r: r.workload_params["task_count"])
            .build()
        )

        ax = sns.lineplot(df, x="task-count", y="duration", hue="environment", marker="o")
        ax.set(ylabel="Duration [s]", xlabel="Task count")
        # ax.set(yscale="log")
        render_chart_to_png(workdir / "dask-vs-hq-empty.png")


if __name__ == "__main__":
    cli()

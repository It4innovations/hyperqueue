import itertools
from pathlib import Path
from typing import Iterable

from src.postprocessing.common import format_large_int
from src.build.hq import Profile
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local, get_active_nodes
from src.environment.hq import HqClusterInfo, HqWorkerConfig
from src.analysis.chart import render_chart
from src.analysis.dataframe import DataFrameExtractor
from src.workloads.sleep import SleepHQ

from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary
from src.cli import register_case, TestCase, create_cli

cli = create_cli()


@register_case(cli)
class PerTaskOverhead(TestCase):
    """
    Benchmark the amount of tasks that can be executed per second
    using the zero worker mode.

    Should be run on 17 nodes (server + up to 16 workers).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary(zero_worker=True, profile=Profile.Dist)

        nodes = get_active_nodes()
        # nodes = Local()

        repeat_count = 3

        task_counts = [10000, 50000, 100000, 500000, 1000000]
        worker_counts = [1, 2, 4, 8, 16]

        def gen_items():
            for task_count, worker_count in itertools.product(
                task_counts, worker_counts
            ):
                env = HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes),
                    environment_params=dict(),
                    workers=[HqWorkerConfig() for _ in range(worker_count)],
                    binary=hq_path,
                )
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=SleepHQ(task_count=task_count, sleep_duration=0),
                    repeat_count=repeat_count,
                )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        import matplotlib
        import seaborn as sns

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration", "environment")
            .transform("task_count", lambda r: r.workload_params["task_count"])
            .transform("task_duration", lambda r: r.workload_params["duration"])
            .transform(
                "worker_count", lambda r: r.environment_params.get("worker_count", 1)
            )
            .build()
        )

        def draw(data, key: str, label: str, **kwargs):
            data = data.copy()

            ax = sns.barplot(data, x="worker_count", y=key)
            for axis in ax.containers:
                ax.bar_label(
                    axis,
                    rotation=90,
                    fmt=lambda v: format_large_int(int(v)),
                    padding=-3,
                    label_type="center",
                    color="white",
                )
            ax.set(
                ylabel=label,
                xlabel="Worker count",
                ylim=(0, data[key].max() * 1.3),
            )
            ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, pos: format_large_int(int(v))))

        df["task_per_s"] = df["task_count"] / df["duration"]

        # Seconds to microseconds
        # df["duration"] *= 1000000
        # df["overhead_per_task"] = df["duration"] / df["task_count"]
        # df["task_count_label"] = df["task_count"].map(lambda v: f"{int(v // 1000)}k")
        # grid = sns.FacetGrid(
        # df, col="worker_count", col_wrap=3, sharey=True
        # )
        # grid.map_dataframe(lambda data, **kwargs: draw(data, "overhead_per_task", "Overhead per task [μs]", **kwargs))
        # render_chart(workdir / "per-task-overhead")
        # plt.clf()

        df["task_count"] = df["task_count"].map(format_large_int)
        grid = sns.FacetGrid(df, col="task_count", col_wrap=3, sharey=True)
        grid.map_dataframe(
            lambda data, **kwargs: draw(data, "task_per_s", "Task/s", **kwargs)
        )
        grid.set_titles(col_template="{col_name} tasks")
        grid.figure.subplots_adjust(top=0.9)
        grid.figure.suptitle(f"Tasks per second with zero worker mode")

        render_chart(workdir / "task-per-s")


if __name__ == "__main__":
    cli()

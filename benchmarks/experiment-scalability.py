import datetime
import itertools
import math
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from matplotlib import pyplot as plt
from src.postprocessing.common import format_large_int
from src.build.hq import Profile
import pandas as pd
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
class Scalability(TestCase):
    """
    Benchmark the strong scalability of HyperQueue.

    Should be run on 65 nodes (server + up to 64 workers).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary(profile=Profile.Dist)

        nodes = get_active_nodes()
        # nodes = Local()

        repeat_count = 1

        single_worker_duration = 300
        cores = 128
        single_core_duration = single_worker_duration * cores

        # Fixed makespan, scale task duration + task count
        task_durations = [0.1, 0.5, 2.5]
        worker_counts = [1, 2, 4, 8, 16, 32, 64]

        parameters = list(itertools.product(task_durations, worker_counts))
        parameters = [
            (
                task_duration,
                math.ceil(single_core_duration / task_duration),
                worker_count,
            )
            for (task_duration, worker_count) in parameters
        ]

        # Fixed task duration, scale task count
        task_counts = [10000, 50000, 100000]
        parameters += list(itertools.product([1], task_counts, worker_counts))

        def gen_items():
            for task_duration, task_count, worker_count in parameters:
                env = HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes),
                    environment_params=dict(),
                    workers=[HqWorkerConfig() for _ in range(worker_count)],
                    binary=hq_path,
                    fast_spawn=True,
                )
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=SleepHQ(
                        task_count=task_count, sleep_duration=task_duration
                    ),
                    repeat_count=repeat_count,
                    timeout=datetime.timedelta(seconds=3600),
                )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("task_count", lambda r: r.workload_params["task_count"])
            .transform("task_duration", lambda r: r.workload_params["duration"])
            .transform("worker_count", lambda r: r.environment_params["worker_count"])
            .build()
        )

        fixed_makespan = df[df["task_duration"] != 1].copy()
        fixed_makespan["task_count"] = fixed_makespan.apply(
            lambda row: f"{format_large_int(int(row['task_count']))} tasks, task duration {row['task_duration']}s",
            axis=1,
        )
        draw_scalability_chart(
            fixed_makespan,
            title="Strong scalability (target makespan 300s)",
        )
        render_chart(workdir / "scalability-fixed-makespan")

        plt.clf()

        fixed_task_duration = df[df["task_duration"] == 1].copy()
        fixed_task_duration["task_count"] = fixed_task_duration.apply(
            lambda row: f"{format_large_int(int(row['task_count']))} tasks",
            axis=1,
        )
        draw_scalability_chart(
            fixed_task_duration, title="Strong scalability (task duration 1s)"
        )
        render_chart(workdir / "scalability-fixed-task-duration")


def draw_scalability_chart(df: pd.DataFrame, title: str):
    import seaborn as sns

    speedups = []
    ideal_durations = []
    for _, row in df.iterrows():
        ref_row = df[df["task_count"] == row["task_count"]]
        ref_row = ref_row[ref_row["worker_count"] == 1]
        ref_row = ref_row[ref_row["task_duration"] == row["task_duration"]]
        # ref_duration = (
        # row["task_duration"] * row["task_count"]
        # ) / 128
        ref_duration = ref_row["duration"].mean()
        speedup = 1 if row["worker_count"] == 1 else ref_duration / row["duration"]
        speedups.append(speedup)
        ideal_duration = ref_duration / row["worker_count"]
        ideal_durations.append(ideal_duration)
    df["speedup"] = speedups
    df["ideal-duration"] = ideal_durations

    ylabel_top_set = None

    def draw(data, **kwargs):
        nonlocal ylabel_top_set

        if data["kind"].iloc[0] == "duration":
            # data = data.copy()
            # data["mode"] = "Achieved"
            # data2 = data.copy()
            # data2["mode"] = "Ideal"
            # data2["duration"] = data2["ideal-duration"]

            # data = pd.concat((data, data2))

            ax = sns.lineplot(data, x="worker_count", y="duration", marker="o")
            ylabel = "" if ylabel_top_set else "Duration [s]"
            ylabel_top_set = True
            ax.set(
                ylabel=ylabel,
                xlabel="Worker count",
                ylim=(0, data["duration"].max() * 1.1),
            )
            for x in data["worker_count"].unique():
                max_duration = data[data["worker_count"] == x]["duration"].max()
                x_offset = 2
                y_offset = 3
                ax.text(
                    x + x_offset,
                    max_duration + y_offset,
                    f"{max_duration:.1f}",
                    fontsize="small",
                )
        else:
            data = data.copy()
            data["mode"] = "Achieved"
            data2 = data.copy()
            data2["mode"] = "Ideal"
            data2["speedup"] = data2["worker_count"]

            data = pd.concat((data, data2))

            ax = sns.lineplot(
                data,
                x="worker_count",
                y="speedup",
                hue="mode",
            )
            sns.scatterplot(
                data[data["mode"] == "Achieved"],
                x="worker_count",
                y="speedup",
                marker="o",
                zorder=999,
            )
            ax.set(
                ylabel="Speedup",
                xlabel="Worker count",
                ylim=(0, data["speedup"].max()),
            )
            data = data[data["mode"] == "Achieved"]
            positions = {2: (2, -1.75)}
            for x in data["worker_count"].unique():
                if x > 1:
                    max_speedup = data[data["worker_count"] == x]["speedup"].max()
                    offset = positions.get(x, (3, -1))
                    ax.text(
                        x + offset[0],
                        max_speedup + offset[1],
                        (
                            f"{int(round(max_speedup, 1))}x"
                            if abs(int(round(max_speedup, 1)) - round(max_speedup, 1))
                            < 0.01
                            else f"{max_speedup:.1f}x"
                        ),
                        fontsize="small",
                    )

    df["kind"] = "duration"
    df2 = df.copy()
    df2["kind"] = "speedup"
    df = pd.concat((df, df2))

    grid = sns.FacetGrid(
        df, col="task_count", row="kind", sharey=False, margin_titles=True
    )
    grid.map_dataframe(draw)
    grid.add_legend(title="Speedup")
    grid.set_titles(row_template="", col_template="{col_name}")
    grid.figure.subplots_adjust(top=0.85)
    grid.figure.suptitle(title)


if __name__ == "__main__":
    cli()

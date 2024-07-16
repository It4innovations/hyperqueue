import datetime
import itertools
from pathlib import Path
import subprocess
import time
from typing import Any, Dict, Iterable

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from src.workloads.workload import WorkloadExecutionResult
from src.environment import Environment, EnvironmentDescriptor
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local, get_active_nodes
from src.environment.hq import HqClusterInfo, HqWorkerConfig
from src.analysis.chart import render_chart
from src.analysis.dataframe import DataFrameExtractor
from src.workloads.sleep import Sleep, SleepHQ

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
        hq_path = get_hq_binary()

        nodes = get_active_nodes()
        # nodes = Local()

        repeat_count = 1

        task_counts = [10000, 50000, 100000, 250000]
        task_durations = [0.5]
        worker_counts = [1, 2, 4, 8, 16, 32, 64]

        parameters = list(itertools.product(task_counts, task_durations, worker_counts))

        def gen_items():
            for task_count, task_duration, worker_count in parameters:
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
                    timeout=datetime.timedelta(seconds=20000),
                )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("task_count", lambda r: r.workload_params["task_count"])
            .transform("task_duration", lambda r: r.workload_params["duration"])
            .transform("worker_count", lambda r: r.environment_params["worker_count"])
            .build()
        )

        task_duration = 0.5
        df = df[df["task_duration"] == task_duration]

        speedups = []
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
        df["speedup"] = speedups

        def draw(data, **kwargs):
            if data["kind"].iloc[0] == "duration":
                ax = sns.lineplot(data, x="worker_count", y="duration", marker="o")
                ax.set(
                    ylabel="Duration [s]",
                    xlabel="Worker count",
                    ylim=(0, data["duration"].max() * 1.3),
                )
                for x in data["worker_count"].unique():
                    max_duration = data[data["worker_count"] == x]["duration"].max()
                    x_offset = 0.4
                    y_offset = 2
                    ax.text(
                        x + x_offset,
                        max_duration + y_offset,
                        f"{max_duration:.1f}",
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
                    ylim=(0, data["speedup"].max() * 1.3),
                )
                data = data[data["mode"] == "Achieved"]
                for x in data["worker_count"].unique():
                    max_speedup = data[data["worker_count"] == x]["speedup"].max()
                    x_offset = 0.5
                    y_offset = -1
                    if x == 1:
                        y_offset = -0.9
                    elif x == 2:
                        y_offset = -0.75
                    ax.text(
                        x + x_offset,
                        max_speedup + y_offset,
                        f"{max_speedup:.1f}x",
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
        grid.set_titles(row_template="")
        grid.figure.subplots_adjust(top=0.85)
        grid.figure.suptitle(f"Strong scalability (task duration={task_duration}s)")

        render_chart(workdir / "scalability")


if __name__ == "__main__":
    cli()

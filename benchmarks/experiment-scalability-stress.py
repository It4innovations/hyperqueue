import datetime
import itertools
from pathlib import Path
import subprocess
from typing import Any, Dict, Iterable

from matplotlib import pyplot as plt

from src.environment.dask import DaskClusterInfo, DaskWorkerConfig
from src.workloads.utils import measure_dask_tasks, measure_hq_tasks
from src.environment import Environment
from src.workloads.workload import WorkloadExecutionResult
from src.workloads.stress import Stress, StressHQ
from src.postprocessing.common import (
    analyze_per_worker_utilization,
    analyze_results_utilization,
    format_large_int,
)
from src.build.hq import Profile
import pandas as pd
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local, get_active_nodes
from src.environment.hq import HqClusterInfo, HqWorkerConfig
from src.analysis.chart import render_chart
from src.analysis.dataframe import DataFrameExtractor

from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary
from src.cli import register_case, TestCase, create_cli

cli = create_cli()


class StressWorkload(Stress):
    def __init__(
        self, task_count: int, cpu_count: int, stress_duration: int, mode: str
    ):
        super().__init__(task_count, cpu_count, stress_duration)
        self.mode = mode

    def parameters(self) -> Dict[str, Any]:
        params = super().parameters()
        params["duration-mode"] = self.mode
        return params

    def compute(
        self, env: Environment, task_count: int, cpu_count: int, stress_duration: int
    ) -> WorkloadExecutionResult:
        assert isinstance(stress_duration, int)
        assert stress_duration > 0

        args = [
            "bash",
            "--noprofile",
            "--norc",
            "/mnt/proj1/dd-23-154/beranekj/hyperqueue/benchmarks/stress/run.sh",
        ]

        if self.mode == "direct":
            return measure_hq_tasks(
                env,
                [
                    "/mnt/proj1/dd-23-154/beranekj/hyperqueue/benchmarks/stress/build/bin/stress",
                    "--cpu",
                    str(cpu_count),
                    "--timeout",
                    str(stress_duration),
                ],
                task_count=task_count,
                cpus_per_task=cpu_count,
            )
        elif self.mode == "uniform":
            args.append(str(self.stress_duration))
        elif self.mode == "5-e4-30":
            pass
        else:
            raise Exception(f"Unknown mode {self.mode}")

        return measure_hq_tasks(
            env,
            args,
            task_count=task_count,
            cpus_per_task=cpu_count,
        )


def stress_task(stress_duration: int):
    subprocess.run(
        [
            "/mnt/proj1/dd-23-154/beranekj/hyperqueue/benchmarks/stress/build/bin/stress",
            "--cpu",
            "1",
            "--timeout",
            str(stress_duration),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
    )


class StressWorkloadDask(StressWorkload):
    def compute(
        self, env: Environment, task_count: int, cpu_count: int, stress_duration: int
    ) -> WorkloadExecutionResult:
        assert isinstance(stress_duration, int)
        assert stress_duration > 0

        assert self.mode == "direct"

        from distributed import Client

        def run(client: Client):
            tasks = client.map(stress_task, [stress_duration] * task_count, pure=False)
            client.gather(tasks)

        return measure_dask_tasks(env, run)


def create_dask_worker(cores: int, n_processes: int) -> DaskWorkerConfig:
    return DaskWorkerConfig.create(
        count=cores,
        n_processes=n_processes,
        init_cmd=["source /mnt/proj1/dd-23-154/beranekj/hyperqueue/modules.sh"],
    )


@register_case(cli)
class ScalabilityStress(TestCase):
    """
    Benchmark the strong scalability of HyperQueue with a CPU stress test.

    Should be run on 5 nodes (server + up to 4 workers).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary(profile=Profile.Dist)

        nodes = get_active_nodes()
        # nodes = Local()

        repeat_count = 1

        worker_counts = [1, 2, 4]  # , 8, 16, 32, 64]
        stress_duration = 5

        # Fixed task duration, scale task count
        task_counts = [10000, 20000]
        parameters = list(
            itertools.product([stress_duration], task_counts, worker_counts)
        )

        def gen_items():
            for task_duration, task_count, worker_count in parameters:
                env = HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes, monitor_nodes=True),
                    environment_params=dict(),
                    workers=[HqWorkerConfig() for _ in range(worker_count)],
                    binary=hq_path,
                )

                items = [
                    (
                        env,
                        StressWorkload(
                            task_count=task_count,
                            cpu_count=1,
                            stress_duration=task_duration,
                            mode=mode,
                        ),
                    )
                    for mode in ("direct", "uniform", "5-e4-30")
                ]
                items += [
                    (
                        DaskClusterInfo(
                            cluster_info=ClusterInfo(
                                node_list=nodes, monitor_nodes=True
                            ),
                            environment_params=dict(worker_threads=128, processes=1),
                            workers=[
                                create_dask_worker(cores=128, n_processes=1)
                                for _ in range(worker_count)
                            ],
                        ),
                        StressWorkloadDask(
                            task_count=task_count,
                            cpu_count=1,
                            stress_duration=task_duration,
                            mode="direct",
                        ),
                    )
                ]

                for env, workload in items:
                    yield BenchmarkDescriptor(
                        env_descriptor=env,
                        workload=workload,
                        repeat_count=repeat_count,
                        timeout=datetime.timedelta(seconds=3600),
                    )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = analyze_results_utilization(database)
        print(
            f"""UTILIZATION
{df}
"""
        )

        # Worker CPU utilization
        util_df = analyze_per_worker_utilization(database)

        task_count = 10000
        worker_count = 4
        util_df = util_df[util_df["worker-count"] == worker_count]
        util_df = util_df[util_df["workload-task_count"] == task_count]
        util_df = util_df[util_df["workload-duration-mode"] != "uniform"]
        util_df = util_df[util_df["environment"] != "dask"]

        def rename_mode(mode: str) -> str:
            return {
                "direct": "Task duration 5s",
                "uniform": "Task duration 5s",
                "5-e4-30": "Task duration 5s/30s",
            }[mode]

        util_df["workload-duration-mode"] = util_df["workload-duration-mode"].map(
            rename_mode
        )
        selected_modes = tuple(rename_mode(m) for m in ("direct", "5-e4-30"))

        util_df = util_df[util_df["workload-duration-mode"].isin(selected_modes)]

        def name_workers(rows):
            workers = sorted(rows.unique())
            names = {w: f"Worker {i + 1}" for (i, w) in enumerate(workers)}
            return rows.map(lambda v: names[v])

        # Normalize timestamps and worker names per benchmark
        for uuid in util_df["uuid"].unique():
            data = util_df[util_df["uuid"] == uuid].copy()
            data["timestamp"] -= data["timestamp"].min()
            data["worker"] = name_workers(data["worker"])
            util_df.loc[data.index] = data

        def draw(data, **kwargs):
            avg_util = data["utilization"].mean()
            ax = sns.lineplot(data, x="timestamp", y="utilization")

            first_row = data["workload-duration-mode"].iloc[0] == selected_modes[0]

            width = data["timestamp"].max() * 1.2
            start_rel = 0.05
            x_start = -(width * start_rel)
            ax.set(
                ylabel="Node utilization (%)",
                xlabel="" if first_row else "Time from start (s)",
                xlim=(x_start, width),
            )

            plt.axhline(y=avg_util, color="red", linestyle="--")
            ax.text(
                x=-(width * (start_rel + 0.01)),
                y=avg_util,
                s=f"{avg_util:.0f}%",
                color="red",
                size="medium",
                verticalalignment="center",
                horizontalalignment="right",
            )

        grid = sns.FacetGrid(
            util_df,
            col="worker",
            row="workload-duration-mode",
            # row="workload-task_count",
            row_order=selected_modes,
            sharey=True,
            sharex="row",
            margin_titles=True,
            height=4,
        )
        grid.map_dataframe(draw)
        grid.set_titles(col_template="{col_name}", row_template="{row_name}")
        grid.figure.subplots_adjust(top=0.9)
        grid.figure.suptitle(
            f"Node CPU utilization ({format_large_int(task_count)} tasks, {worker_count} workers)"
        )

        render_chart(workdir / "scalability-stress-utilization")

        plt.clf()

        # Scalability
        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("task_count", lambda r: r.workload_params["task_count"])
            .transform("task_duration", lambda r: r.workload_params["duration"])
            .transform("worker_count", lambda r: r.environment_params["worker_count"])
            .build()
        )

        df["task_count"] = df.apply(
            lambda row: f"{format_large_int(int(row['task_count']))} tasks",
            axis=1,
        )
        draw_scalability_chart(df, title="Strong scalability (task duration 5s)")
        render_chart(workdir / "scalability-stress-speedup")


def draw_scalability_chart(df: pd.DataFrame, title: str):
    """
    Draws a strong scalability chart.
    Expects key "task_count", "worker_count", "task_duration" and "duration"
    in the passed dataframe `df`.
    """
    import seaborn as sns

    speedups = []
    ideal_durations = []
    for _, row in df.iterrows():
        ref_row = df[df["task_count"] == row["task_count"]]
        ref_row = ref_row[ref_row["worker_count"] == 1]
        ref_row = ref_row[ref_row["task_duration"] == row["task_duration"]]
        ref_duration = ref_row["duration"].mean()
        speedup = 1 if row["worker_count"] == 1 else ref_duration / row["duration"]
        speedups.append(speedup)
        ideal_duration = ref_duration / row["worker_count"]
        ideal_durations.append(ideal_duration)
    df["speedup"] = speedups
    df["ideal-duration"] = ideal_durations

    ylabel_top_set = False

    def draw(data, **kwargs):
        nonlocal ylabel_top_set

        if data["kind"].iloc[0] == "duration":
            ax = sns.lineplot(data, x="worker_count", y="duration", marker="o")
            ax.set(
                ylabel="" if ylabel_top_set else "Duration [s]",
                xlabel="Worker count",
                ylim=(0, data["duration"].max() * 1.1),
            )
            ylabel_top_set = True

            for x in data["worker_count"].unique():
                max_duration = data[data["worker_count"] == x]["duration"].max()
                x_offset = 0.1
                y_offset = 0
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
            for x in data["worker_count"].unique():
                if x > 1:
                    max_speedup = data[data["worker_count"] == x]["speedup"].max()
                    offset = (0.2, -0.2)
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

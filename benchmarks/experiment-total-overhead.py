import itertools
from pathlib import Path
import subprocess
import time
from typing import Any, Dict, Iterable

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from src.postprocessing.common import format_large_int
from src.workloads.workload import WorkloadExecutionResult
from src.environment import Environment, EnvironmentDescriptor
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import get_active_nodes
from src.environment.hq import HqClusterInfo, HqWorkerConfig
from src.analysis.chart import render_chart
from src.analysis.dataframe import DataFrameExtractor
from src.workloads.sleep import Sleep, SleepHQ

from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary
from src.cli import register_case, TestCase, create_cli

cli = create_cli()


class DummyEnvironment(Environment):
    def start(self):
        pass

    def stop(self):
        pass


class DummyEnvDescriptor(EnvironmentDescriptor):
    def create_environment(self, workdir: Path) -> Environment:
        return DummyEnvironment()

    def name(self) -> str:
        return "manual"

    def parameters(self) -> Dict[str, Any]:
        return dict(worker_count=1)


CURRENT_DIR = Path(__file__).absolute().parent


class LocalSleepSpawner(Sleep):
    def execute(self, env: Environment) -> WorkloadExecutionResult:
        args = [
            str(CURRENT_DIR / "spawner/target/release/spawner"),
            str(self.task_count),
            str(self.sleep_duration),
        ]
        print(f"Running {args}")

        start = time.time()
        subprocess.run(args)
        duration = time.time() - start

        return WorkloadExecutionResult(duration=duration)


@register_case(cli)
class TotalOverhead(TestCase):
    """
    Benchmark the total overhead of HyperQueue.
    Measures how long it should take to execute a given task graph (by just adding the
    durations of all tasks) vs what was the makespan when executed in HQ.

    Should be run on 5 nodes (server + up to 4 workers).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary()

        fast_spawn = True

        nodes = get_active_nodes()
        # nodes = Local()

        repeat_count = 3

        task_counts = [10000, 50000]
        durations = [0.001, 0.01, 0.1, 0.25]
        worker_counts = [1, 2, 4]

        def gen_items():
            for task_count, duration, worker_count in itertools.product(task_counts, durations, worker_counts):
                # HQ
                env = HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes),
                    environment_params=dict(fast_spawn=fast_spawn),
                    workers=[HqWorkerConfig() for _ in range(worker_count)],
                    binary=hq_path,
                    fast_spawn=fast_spawn,
                )
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=SleepHQ(task_count=task_count, sleep_duration=duration),
                    repeat_count=repeat_count,
                )

                # Manual spawn
                if worker_count == 1:
                    env = DummyEnvDescriptor()
                    yield BenchmarkDescriptor(
                        env_descriptor=env,
                        workload=LocalSleepSpawner(task_count=task_count, sleep_duration=duration),
                        repeat_count=repeat_count,
                    )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        df = (
            DataFrameExtractor(database)
            .extract("index", "duration", "environment")
            .transform("task_count", lambda r: r.workload_params["task_count"])
            .transform("task_duration", lambda r: r.workload_params["duration"])
            .transform("worker_count", lambda r: r.environment_params.get("worker_count", 1))
            .build()
        )

        render_hq_overhead_ratio(df, workdir / "total-overhead-vs-ideal")
        render_hq_overhead_ratio_vs_manual(df, workdir / "total-overhead-vs-manual")


def render_hq_overhead_ratio(df: pd.DataFrame, path: Path):
    df = df[df["environment"] == "hq"].copy()
    # Calculate theoretical ideal makespan
    df["expected_duration"] = (df["task_count"] * np.maximum(df["task_duration"], 0.001)) / (128 * df["worker_count"])

    draw_bar_chart_ratio(df, "HyperQueue makespan vs ideal makespan", sharey="col")
    render_chart(path)


def render_hq_overhead_ratio_vs_manual(df: pd.DataFrame, path: Path):
    data = df.copy()
    df = df[df["environment"] == "hq"].copy()
    df = df[df["worker_count"] == 1]

    ref = data[data["environment"] != "hq"]

    expected_durations = []
    for _, row in df.iterrows():
        ref_row = ref[ref["task_count"] == row["task_count"]]
        ref_row = ref_row[ref_row["task_duration"] == row["task_duration"]]
        ref_duration = ref_row["duration"].min()
        expected_durations.append(ref_duration)

    df["expected_duration"] = expected_durations

    draw_bar_chart_ratio(df, "HyperQueue makespan vs manual process execution", sharey=True)
    render_chart(path)


def draw_bar_chart_ratio(df: pd.DataFrame, title: str, sharey):
    import seaborn as sns

    seen_rows = set()

    def draw(data, **kwargs):
        task_count = data["task_count"].iloc[0]
        if task_count not in seen_rows:
            ylabel = "Ratio (vs ideal duration)"
            seen_rows.add(task_count)
        else:
            ylabel = None

        data = data.copy()
        data["ratio"] = data["duration"] / data["expected_duration"]
        data = data.rename(
            columns={
                "duration": "HyperQueue duration",
                "expected_duration": "Expected duration",
            }
        )

        ax = sns.barplot(data, x="worker_count", y="ratio")
        for axis in ax.containers:
            ax.bar_label(axis, rotation=90, fmt="%.2f", padding=5)
        ax.set(
            ylabel=ylabel,
            xlabel="Worker count",
            ylim=(0, data["ratio"].max() * 1.4),
        )
        plt.axhline(y=1, color="red", linestyle="--")

    df["task_duration"] = df["task_duration"].map(lambda v: f"Task duration {v}s")
    df["task_count"] = df["task_count"].map(lambda v: f"{format_large_int(v)} tasks")

    grid = sns.FacetGrid(df, col="task_duration", row="task_count", sharey=sharey, margin_titles=True)
    grid.map_dataframe(draw)
    grid.add_legend(loc="upper center")
    grid.set_titles(col_template="{col_name}", row_template="{row_name}")
    grid.figure.subplots_adjust(top=0.9, right=0.9)
    grid.figure.suptitle(title)


def render_duration_vs_expected_duration(df: pd.DataFrame, path: Path):
    import seaborn as sns

    def draw(data, **kwargs):
        data = data.rename(
            columns={
                "duration": "HyperQueue duration",
                "expected_duration": "Ideal duration",
            }
        )
        data = pd.melt(
            data,
            value_vars=["HyperQueue duration", "Ideal duration"],
            value_name="makespan",
            var_name="mode",
            id_vars=["task_duration"],
        )

        ax = sns.barplot(data, x="task_duration", y="makespan", hue="mode")
        for axis in ax.containers:
            ax.bar_label(axis, rotation=90, fmt="%.2f", padding=5)
        ax.set(
            ylabel="Duration [s]",
            xlabel="Task duration [s]",
            ylim=(0, data["makespan"].max() * 1.3),
        )

    grid = sns.FacetGrid(df, row="worker_count", col="task_count", sharey=False, sharex=False)
    grid.map_dataframe(draw)
    grid.add_legend(loc="upper center")
    grid.figure.subplots_adjust(top=0.9, right=0.9)

    render_chart(path)


if __name__ == "__main__":
    cli()

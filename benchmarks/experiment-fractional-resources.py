from collections import defaultdict
import datetime
import itertools
import json
from pathlib import Path
from typing import Any, Dict, Iterable

from src.workloads.utils import measure_hq_tasks
from src.environment import Environment
from src.workloads.workload import Workload, WorkloadExecutionResult
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local
from src.environment.hq import HqClusterInfo, HqEnvironment, HqWorkerConfig
from src.analysis.chart import render_chart
from src.analysis.dataframe import DataFrameExtractor
from src.trace.export import parse_hq_time

from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary
from src.cli import register_case, TestCase, create_cli

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

cli = create_cli()


class ModelTrainWorkload(Workload):
    def __init__(
        self,
        task_count: int,
        cpu_count: int,
        gpu_count: str
    ):
        self.task_count = task_count
        self.cpu_count = cpu_count
        self.gpu_count = gpu_count

    def name(self) -> str:
        return "model-train"

    def parameters(self) -> Dict[str, Any]:
        return {
            "task-count": self.task_count,
            "cpu-count": self.cpu_count,
            "gpu-count": self.gpu_count,
        }

    def execute(self, env: Environment) -> WorkloadExecutionResult:
        assert isinstance(env, HqEnvironment)

        return measure_hq_tasks(
            env,
            [
                "/mnt/proj1/dd-23-154/beranekj/hyperqueue/benchmarks/model-train/run.sh",
            ],
            task_count=self.task_count,
            cpus_per_task=self.cpu_count,
            additional_args=["--resource", f"gpus/nvidia={self.gpu_count}"],
            # stdout=None,
        )


@register_case(cli)
class FractionalResources(TestCase):
    """
    Benchmark fractional resources.

    Should be run on 2 GPU nodes (server + 1 worker).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary()

        # nodes = get_active_nodes()
        nodes = Local()

        # repeat_count = 3
        repeat_count = 1

        worker_counts = [1]
        task_counts = [20]
        gpu_counts = ["0.1", "0.25", "0.5", "1"]

        parameters = list(
            itertools.product(
                task_counts,
                gpu_counts,
                worker_counts,
            )
        )

        def gen_items():
            for (
                task_count,
                gpu_count,
                worker_count,
            ) in parameters:
                env = HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes),
                    environment_params=dict(),
                    workers=[
                        HqWorkerConfig(
                            overview_interval=datetime.timedelta(seconds=1)
                        ) for _ in range(worker_count)
                    ],
                    binary=hq_path,
                    generate_event_log=True
                )
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=ModelTrainWorkload(
                        task_count=task_count,
                        cpu_count=1,
                        gpu_count=gpu_count,
                    ),
                    repeat_count=repeat_count,
                    timeout=datetime.timedelta(seconds=3600),
                )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("task-count", lambda r: r.workload_params["task-count"])
            .transform("gpu-count", lambda r: r.workload_params["gpu-count"])
            .transform("worker-count", lambda r: r.environment_params["worker_count"])
            .transform("workdir", lambda r: r.benchmark_metadata["workdir"])
            .build()
        )

        task_count = df["task-count"].iloc[0]

        data = None
        for (_, row) in df.iterrows():
            bench_workdir = Path(row["workdir"])
            timeline = parse_timeline(bench_workdir / "server" / "events.json")

            # timeline = timeline.set_index("time")
            # timeline = timeline.resample("1s").fillna("backfill")
            # timeline["time"] = timeline.index

            start = timeline["time"].min()
            timeline["time"] = timeline["time"] - start
            timeline["time"] = timeline["time"].map(lambda v: v.total_seconds())
            timeline["gpu-count"] = row["gpu-count"]
            timeline["makespan"] = row["duration"]

            if data is None:
                data = timeline
            else:
                data = pd.concat((data, timeline))

        gpu_counts = sorted(data["gpu-count"].unique(), reverse=True)
        palette = sns.color_palette()

        def draw(data, **kwargs):
            # data = pd.melt(
            #     data,
            #     value_vars=["gpu-util-0", "gpu-util-1"],
            #     id_vars=["time"],
            #     var_name="gpu",
            #     value_name="gpu-util"
            # )
            # ax = sns.lineplot(data, x="time", y="gpu-util", hue="gpu")

            index = gpu_counts.index(data["gpu-count"].iloc[0])
            # Skip red color
            if index == len(gpu_counts) - 1:
                index += 1
            ax = sns.lineplot(data, x="time", y="gpu-util", color=palette[index])

            ax.set(
                ylabel="Average GPU utilization (%)",
                xlabel="Time from start [s]",
                ylim=(0, 110)
            )

            end = data["makespan"].iloc[0]

            # Average utilization
            avg_util = data["gpu-util"].mean()
            plt.axhline(y=avg_util, color="red", linestyle="--")
            # plt.hlines(
            #     y=avg_util, xmin=0, xmax=end, color="red", linestyle="--", linewidth=2
            # )
            ax.text(
                x=-25,
                y=avg_util + 1,
                s=f"{avg_util:.0f}%",
                color="red",
                size="medium",
                verticalalignment="center",
                horizontalalignment="right",
            )

            # Makespan
            plt.vlines(
                x=end, ymin=-4, ymax=5, color="black", linewidth=3, clip_on=False
            )
            ax.text(
                x=end + 5,
                y=4,
                s=f"{end:.0f}s",
                size="large",
                color="black",
                weight="bold",
                verticalalignment="bottom",
                horizontalalignment="left",
            )

        grid = sns.FacetGrid(
            data,
            col="gpu-count",
            col_order=gpu_counts,
            sharex=True,
            sharey=True,
            height=4,
        )
        grid.map_dataframe(draw)
        grid.set_titles(
            col_template="GPU fraction per task: {col_name}",
            size=12
        )
        grid.figure.subplots_adjust(top=0.8)
        grid.figure.suptitle(
            f"GPU hardware utilization effect of fractional resources ({task_count} tasks, 2 GPUs)"
        )

        render_chart(workdir / "fractional-resources")


def parse_timeline(events_path: Path) -> pd.DataFrame:
    data = defaultdict(list)
    with open(events_path) as f:
        for line in f:
            event = json.loads(line)
            time = parse_hq_time(event["time"])
            event = event["event"]
            type = event["type"]
            if type == "worker-overview":
                state = event["hw-state"]["state"]["nvidia_gpus"]["gpus"]
                usages = [gpu["processor_usage"] for gpu in state]
                average = np.mean(np.array(usages))
                data["time"].append(time)
                data["gpu-util"].append(average)
                for (i, utilization) in enumerate(usages):
                    data[f"gpu-util-{i}"].append(utilization)
    return pd.DataFrame(data)


if __name__ == "__main__":
    cli()

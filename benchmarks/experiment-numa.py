import datetime
import enum
import itertools
from pathlib import Path
import random
from typing import Any, Dict, Iterable

from src.workloads.utils import measure_hq_tasks
from src.environment import Environment
from src.workloads.workload import Workload, WorkloadExecutionResult
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local
from src.environment.hq import HqClusterInfo, HqEnvironment, HqWorkerConfig
from src.analysis.chart import render_chart
from src.analysis.dataframe import DataFrameExtractor

from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary
from src.cli import register_case, TestCase, create_cli

cli = create_cli()


class Strategy(enum.Enum):
    Compact = enum.auto()
    ForceCompact = enum.auto()
    Scatter = enum.auto()
    NoNuma = enum.auto()


class NumaWorkload(Workload):
    def __init__(
        self,
        task_count: int,
        cpu_count: int,
        strategy: Strategy,
        parallel: bool,
        binary: Path,
    ):
        self.task_count = task_count
        self.cpu_count = cpu_count
        self.strategy = strategy
        self.parallel = parallel
        self.binary = binary

    def name(self) -> str:
        return "numa"

    def parameters(self) -> Dict[str, Any]:
        return {
            "task-count": self.task_count,
            "cpu-count": self.cpu_count,
            "strategy": self.strategy.name.lower(),
            # "parallel": self.parallel,
            # "binary": self.binary.name,
        }

    def execute(self, env: Environment) -> WorkloadExecutionResult:
        assert isinstance(env, HqEnvironment)

        cpus = str(self.cpu_count)
        if self.strategy == Strategy.Compact:
            cpus = f"{self.cpu_count} compact"
        elif self.strategy == Strategy.ForceCompact:
            cpus = f"{self.cpu_count} compact!"
        elif self.strategy == Strategy.Scatter:
            cpus = f"{self.cpu_count} scatter"
        elif self.strategy == Strategy.NoNuma:
            cpus = str(self.cpu_count)
        else:
            assert False

        args = ["--pin", "taskset"]
        if not self.parallel:
            args += ["--env", "NUMA_SEQUENTIAL=1"]

        return measure_hq_tasks(
            env,
            [
                # "printenv"
                # "/mnt/proj1/dd-23-154/beranekj/hyperqueue/benchmarks/stream/stream",
                str(self.binary),
            ],
            task_count=self.task_count,
            cpus_per_task=cpus,
            additional_args=args,
            # stdout=None,
        )


@register_case(cli)
class Numa(TestCase):
    """
    Benchmark resource placement modes.

    Should be run on 2 nodes (server + 1 worker).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary()

        # nodes = get_active_nodes()
        nodes = Local()

        # repeat_count = 3
        repeat_count = 1

        worker_counts = [1]
        task_counts = [100]
        cpu_counts = [4, 8]  # , 16]
        parallel_values = [True]
        strategies = [
            Strategy.Compact,
            Strategy.ForceCompact,
            Strategy.Scatter,
            Strategy.NoNuma,
        ]
        binaries = [
            Path(
                "/mnt/proj1/dd-23-154/beranekj/hyperqueue/benchmarks/stream/numa-stress/target/release/numa-stress"
            ),
            # Path(
            #     "/mnt/proj1/dd-23-154/beranekj/hyperqueue/benchmarks/stream/numa-stress/target/release/numa-stress2"
            # ),
        ]

        random.seed(42)
        ungrouped_cpus = list(range(128))
        random.shuffle(ungrouped_cpus)
        ungrouped_cpus = f"[{','.join(str(c) for c in ungrouped_cpus)}]"

        parameters = list(
            itertools.product(
                task_counts,
                cpu_counts,
                strategies,
                parallel_values,
                binaries,
                worker_counts,
            )
        )

        def gen_items():
            for (
                task_count,
                cpu_count,
                strategy,
                parallel,
                binary,
                worker_count,
            ) in parameters:
                worker_args = {}

                # Turn off CPU group detection
                if strategy == Strategy.NoNuma:
                    worker_args["cpus"] = ungrouped_cpus
                env = HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes),
                    environment_params=dict(),
                    workers=[
                        HqWorkerConfig(**worker_args) for _ in range(worker_count)
                    ],
                    binary=hq_path,
                )
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=NumaWorkload(
                        task_count=task_count,
                        cpu_count=cpu_count,
                        strategy=strategy,
                        parallel=parallel,
                        binary=binary,
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
            .transform("cpu-count", lambda r: r.workload_params["cpu-count"])
            .transform("strategy", lambda r: r.workload_params["strategy"])
            # .transform("parallel", lambda r: r.workload_params["parallel"])
            # .transform("binary", lambda r: r.workload_params["binary"])
            .transform("worker-count", lambda r: r.environment_params["worker_count"])
            .build()
        )

        def rename_strategy(strategy: str) -> str:
            if strategy == "compact":
                return "Compact"
            if strategy == "forcecompact":
                return "Strict compact"
            if strategy == "scatter":
                return "Scatter"
            if strategy == "nonuma":
                return "No strategy"
            else:
                assert False

        df["strategy"] = df["strategy"].map(rename_strategy)
        task_count = df["task-count"].iloc[0]

        label_set = False

        def draw(data, **kwargs):
            nonlocal label_set

            ax = sns.barplot(data, hue="strategy", y="duration", gap=0.1)

            ylabel = None
            if not label_set:
                ylabel = "Duration [s]"
                label_set = True

            ax.set(ylabel=ylabel, ylim=(0, data["duration"].max() * 1.2))
            for axis in ax.containers:
                ax.bar_label(axis, fmt="%.1f", padding=5)

        grid = sns.FacetGrid(df, col="cpu-count", sharey=False)
        grid.map_dataframe(draw)
        grid.add_legend(title="Strategy")
        grid.set_titles(
            col_template="{col_name} threads per task",
            # row_template="Binary: {row_name}",
        )
        grid.figure.subplots_adjust(top=0.8)
        grid.figure.suptitle(
            f"Effect of resource group allocation strategies ({task_count} tasks)"
        )

        render_chart(workdir / "numa")


if __name__ == "__main__":
    cli()

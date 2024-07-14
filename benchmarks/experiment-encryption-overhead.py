from pathlib import Path
from typing import Iterable

from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import get_active_nodes
from src.analysis.chart import render_chart
from src.analysis.dataframe import DataFrameExtractor
from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary
from src.cli import register_case, TestCase, create_cli
from src.environment.hq import HqWorkerConfig, HqClusterInfo
from src.workloads.sleep import SleepHQ

import numpy as np

cli = create_cli()


@register_case(cli)
class EncryptionOverhead(TestCase):
    """
    Benchmarks the encryption overhead of HyperQueue, both with and without the "zero-worker" mode.

    Should be run on 5 nodes (server + 4 workers).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        def gen_items(encrypt: bool, zero_worker: bool = True):
            hq_path = get_hq_binary(zero_worker=zero_worker)
            env = HqClusterInfo(
                cluster=ClusterInfo(node_list=get_active_nodes()),
                environment_params=dict(encrypted=encrypt, zw=zero_worker),
                workers=[HqWorkerConfig() for _ in range(4)],
                binary=hq_path,
                encryption=encrypt,
            )
            task_counts = [10000, 50000, 100000]
            for task_count in task_counts:
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=SleepHQ(task_count=task_count),
                    repeat_count=3,
                )

        benchmarks = []
        for encrypt in (True, False):
            for zw in (True, False):
                benchmarks.extend(gen_items(encrypt=encrypt, zero_worker=zw))
        return benchmarks

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("task_count", lambda r: r.workload_params["task_count"])
            .transform("encrypted", lambda r: r.environment_params["encrypted"])
            .transform("zero-worker", lambda r: r.environment_params["zw"])
            .build()
        )
        df["duration"] *= 1000
        df["encrypted"] = np.where(df["encrypted"], "encrypted", "not encrypted")

        def draw(data, **kwargs):
            ax = sns.barplot(data, x="task_count", y="duration", hue="encrypted")
            for axis in ax.containers:
                ax.bar_label(axis, rotation=90, fmt="%.0f", padding=5)
            ax.set(
                ylabel="Duration [ms]",
                xlabel="Task count",
                ylim=(0, data["duration"].max() * 1.3),
            )

        grid = sns.FacetGrid(df, col="zero-worker", col_order=[True, False], sharey=False)
        grid.map_dataframe(draw)
        grid.add_legend()
        grid.figure.subplots_adjust(top=0.8)
        grid.figure.suptitle("Encryption overhead (4 workers)")

        render_chart(workdir / "encryption-overhead")


if __name__ == "__main__":
    cli()

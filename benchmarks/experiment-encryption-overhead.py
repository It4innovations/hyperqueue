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

cli = create_cli()


@register_case(cli)
class EncryptionOverhead(TestCase):
    """
    Benchmarks the encryption overhead of HyperQueue, using the "zero-worker" mode.
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        def gen_items(encrypt: bool, zero_worker: bool = False):
            hq_path = get_hq_binary(zero_worker=zero_worker)
            env = HqClusterInfo(
                cluster=ClusterInfo(node_list=get_active_nodes()),
                environment_params=dict(encrypted=encrypt, zw=zero_worker),
                workers=[HqWorkerConfig()],
                binary=hq_path,
            )
            task_counts = [100, 1000, 10000, 50000]
            for task_count in task_counts:
                yield BenchmarkDescriptor(env_descriptor=env, workload=SleepHQ(task_count=task_count), repeat_count=2)

        benchmarks = list(gen_items(True)) + list(gen_items(False))
        return benchmarks

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("task_count", lambda r: r.workload_params["task_count"])
            .transform("encrypted", lambda r: r.environment_params["encrypted"])
            .build()
        )
        df["duration"] *= 1000

        ax = sns.barplot(df, x="task_count", y="duration", hue="encrypted")
        ax.set(ylabel="Duration [ms]", xlabel="Task count")
        render_chart(workdir / "hq-encryption-overhead")


if __name__ == "__main__":
    cli()

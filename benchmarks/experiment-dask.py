import datetime
import itertools
from pathlib import Path
from typing import Iterable

from src.build.hq import Profile
from src.postprocessing.common import format_large_int
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import get_active_nodes
from src.environment.dask import DaskClusterInfo, DaskWorkerConfig
from src.environment.hq import HqClusterInfo, HqWorkerConfig
from src.analysis.chart import render_chart
from src.analysis.dataframe import DataFrameExtractor
from src.benchmark.database import Database, DatabaseRecord
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import (
    get_hq_binary,
)
from src.cli import TestCase, register_case, create_cli
from src.workloads import SleepHQ
from src.workloads.empty import EmptyDask
from src.workloads.sleep import SleepDask

cli = create_cli()


def create_dask_worker(cores: int, n_processes: int) -> DaskWorkerConfig:
    return DaskWorkerConfig.create(
        count=cores,
        n_processes=n_processes,
        init_cmd=["source /mnt/proj1/dd-23-154/beranekj/hyperqueue/modules.sh"],
    )


@register_case(cli)
class DaskVsHqSleep(TestCase):
    """
    Benchmark the duration of sleeping for 30s, divided between various amounts of tasks.

    Should be run on 9 nodes (server + up to 8 workers).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary()
        timeout = datetime.timedelta(minutes=10)

        nodes = get_active_nodes()
        # nodes = Local()

        repeat_count = 1

        cores = 128
        total_duration_single_core = cores * 30

        task_counts = [1000, 5000, 25000, 50000]
        worker_counts = [1, 2, 4, 8]
        dask_processes = [1, 8, 128]

        dask_envs = []
        hq_envs = []
        for worker_count in worker_counts:
            for n_processes in dask_processes:
                dask_envs.append(
                    DaskClusterInfo(
                        cluster_info=ClusterInfo(node_list=nodes),
                        environment_params=dict(
                            worker_threads=cores, processes=n_processes
                        ),
                        workers=[
                            create_dask_worker(cores=cores, n_processes=n_processes)
                            for _ in range(worker_count)
                        ],
                    )
                )
            hq_envs.append(
                HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes),
                    environment_params=dict(worker_threads=cores),
                    workers=[HqWorkerConfig(cpus=cores) for _ in range(worker_count)],
                    binary=hq_path,
                    fast_spawn=True,
                )
            )

        envs = [(env, SleepDask) for env in dask_envs]
        # envs += [(env, SleepDaskSpawn) for env in dask_envs]
        envs += [(env, SleepHQ) for env in hq_envs]

        parameters = list(itertools.product(envs, task_counts))

        for (env, workload_cls), task_count in parameters:
            # Avoid rounding errors
            sleep_duration = str(round(total_duration_single_core / task_count, 4))
            print(
                f"Sleep duration {sleep_duration}, task_count {task_count}, env {env.__class__.__name__} ({env.parameters()})"
            )
            workload = workload_cls(
                task_count=task_count, sleep_duration=sleep_duration
            )
            yield BenchmarkDescriptor(
                env_descriptor=env,
                workload=workload,
                timeout=timeout,
                repeat_count=repeat_count,
            )

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        def parse_env(record: DatabaseRecord) -> str:
            if record.environment == "hq":
                return "HyperQueue"
            total_cores = record.environment_params["worker_threads"]
            processes = record.environment_params["processes"]
            threads = int(total_cores // processes)
            env = f"Dask ({processes}p/{threads}t)"

            if record.workload == "sleep-spawn":
                env += "-spawn"
            return env

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("environment", parse_env)
            .transform("task-count", lambda r: r.workload_params["task_count"])
            .transform("task-duration", lambda r: r.workload_params["duration"])
            .transform("worker-count", lambda r: r.environment_params["worker_count"])
            .build()
        )

        df = df[~df["environment"].str.contains("spawn")]

        def draw(data, **kwargs):
            ax = sns.barplot(data, x="worker-count", y="duration", hue="environment")
            for axis in ax.containers:
                ax.bar_label(axis, rotation=90, fmt="%.1f", padding=5, fontsize="small")
            ax.set(
                ylabel="Duration [s]",
                xlabel="Node count",
                ylim=(0, 95),  # data["duration"].max() * 1.5),
            )

        df["task-count"] = df.apply(
            lambda row: f"{format_large_int(int(row['task-count']))} tasks, task duration {row['task-duration']}s",
            axis=1,
        )
        grid = sns.FacetGrid(
            df,
            col="task-count",
            col_wrap=2,
            sharey=True,
        )
        grid.map_dataframe(draw)
        grid.add_legend(title="Task runtime")
        grid.set_titles(col_template="{col_name}")
        grid.figure.subplots_adjust(top=0.9)
        grid.figure.suptitle("Scalability of Dask vs HyperQueue (target makespan 30s)")

        render_chart(workdir / "dask-vs-hq-sleep")


@register_case(cli)
class DaskVsHqEmpty(TestCase):
    """
    Benchmark the duration of an empty task between HQ and Dask.

    Should be run on 5 nodes (server + up to 4 workers).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path_zw = get_hq_binary(zero_worker=True, profile=Profile.Dist)
        hq_path = get_hq_binary(zero_worker=False)
        timeout = datetime.timedelta(minutes=10)

        nodes = get_active_nodes()
        # nodes = Local()

        cores = 128
        repeat_count = 1
        task_counts = [5000, 10000, 50000, 100000]
        worker_counts = [1, 2, 4]
        dask_processes = [1, 8, 128]

        def gen_items():
            benchmarks = []
            for worker_count in worker_counts:
                for task_count in task_counts:
                    for n_processes in dask_processes:
                        workload = EmptyDask(task_count=task_count)
                        benchmarks.append(
                            (
                                workload,
                                DaskClusterInfo(
                                    cluster_info=ClusterInfo(node_list=nodes),
                                    environment_params=dict(
                                        worker_threads=cores, processes=n_processes
                                    ),
                                    workers=[
                                        create_dask_worker(
                                            cores=cores, n_processes=n_processes
                                        )
                                        for _ in range(worker_count)
                                    ],
                                ),
                            )
                        )
                    workload = SleepHQ(task_count=task_count, sleep_duration=0)
                    benchmarks.append(
                        (
                            workload,
                            HqClusterInfo(
                                cluster=ClusterInfo(node_list=nodes),
                                environment_params=dict(worker_threads=cores, zw=False),
                                workers=[
                                    HqWorkerConfig(cpus=cores)
                                    for _ in range(worker_count)
                                ],
                                binary=hq_path,
                                fast_spawn=True,
                            ),
                        )
                    )
                    benchmarks.append(
                        (
                            workload,
                            HqClusterInfo(
                                cluster=ClusterInfo(node_list=nodes),
                                environment_params=dict(worker_threads=cores, zw=True),
                                workers=[
                                    HqWorkerConfig(cpus=cores)
                                    for _ in range(worker_count)
                                ],
                                binary=hq_path_zw,
                            ),
                        )
                    )

            for workload, env in benchmarks:
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=workload,
                    timeout=timeout,
                    repeat_count=repeat_count,
                )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        def parse_env(record: DatabaseRecord) -> str:
            if record.environment == "hq":
                if record.environment_params["zw"]:
                    return "HQ (zero worker)"
                return "HQ"
            total_cores = record.environment_params["worker_threads"]
            processes = record.environment_params["processes"]
            threads = int(total_cores // processes)
            return f"Dask ({processes}p/{threads}t)"

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("environment", parse_env)
            .transform("task-count", lambda r: r.workload_params["task_count"])
            .transform("worker-count", lambda r: r.environment_params["worker_count"])
            .build()
        )

        def draw(data, **kwargs):
            show_yaxis_label = (
                data["task-count"].isin(("5000 tasks", "50 000 tasks")).any()
            )
            ax = sns.barplot(data, x="worker-count", y="duration", hue="environment")
            for axis in ax.containers:
                ax.bar_label(axis, rotation=90, fmt="%.1f", padding=5)
            ax.set(
                ylabel="Duration [s]" if show_yaxis_label else "",
                xlabel="Node count",
                ylim=(0, data["duration"].max() * 1.3),
            )

        def sort(values):
            def extract(v: str) -> str:
                if v.startswith("Dask"):
                    v = v[6:]
                    return v[: v.index("p")].zfill(3)
                return v

            return values.map(extract)

        df = df.sort_values(by="task-count")
        df = df.sort_values(by="environment", key=sort, kind="stable")
        df["task-count"] = df.apply(
            lambda row: f"{format_large_int(int(row['task-count']))} tasks",
            axis=1,
        )
        grid = sns.FacetGrid(
            df,
            col="task-count",
            col_wrap=2,
            sharey=False,
        )
        grid.map_dataframe(draw)
        grid.add_legend(title="Task runtime")
        grid.set_titles(col_template="{col_name}")
        grid.figure.subplots_adjust(top=0.9)
        grid.figure.suptitle("Scalability of Dask vs HyperQueue (empty task)")

        render_chart(workdir / "dask-vs-hq-empty")


if __name__ == "__main__":
    cli()

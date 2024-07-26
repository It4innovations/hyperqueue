import datetime
import glob
import itertools
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple

import dataclasses
import distributed
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import tqdm

from src.postprocessing.common import (
    analyze_per_worker_utilization,
    analyze_results_utilization,
    format_large_int,
)
from src.environment.dask import DaskEnvironment
from src.analysis.chart import render_chart
from src.clusterutils import ClusterInfo
from hyperqueue import Client
from hyperqueue.job import SubmittedJob, Job
from hyperqueue.visualization import visualize_job
from src.analysis.dataframe import DataFrameExtractor
from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary
from src.cli import register_case, TestCase, create_cli
from src.clusterutils.node_list import Local, get_active_nodes
from src.environment.hq import HqEnvironment, HqClusterInfo, HqWorkerConfig
from src.utils import activate_cwd, ensure_directory
from src.workloads import Workload
from src.workloads.utils import create_result, get_last_hq_job_duration
from src.workloads.workload import WorkloadExecutionResult

CURRENT_DIR = Path(__file__).absolute().parent

# CONTAINER_PATH = CURRENT_DIR / "ligen.sif"
CONTAINER_PATH = Path("/scratch/project/dd-23-154/hq-experiments/ligen.sif")
assert CONTAINER_PATH.is_file()

cli = create_cli()


@dataclasses.dataclass
class LigenConfig:
    container_path: Path
    smi_path: Path
    probe_mol2_path: Path
    protein_path: Path
    max_molecules: int
    screening_threads: int

    def __post_init__(self):
        assert self.container_path.is_file()
        assert self.smi_path.is_file()
        assert self.probe_mol2_path.is_file()
        assert self.protein_path.is_file()


class LigenHQWorkload(Workload):
    def __init__(self, config: LigenConfig):
        self.config = config
        self.screening_threads = self.config.screening_threads
        # min(
        #     self.config.max_molecules, self.config.screening_threads
        # )
        # if self.screening_threads != self.config.screening_threads:
        #     logging.warning(
        #         f"Setting screening threads to {self.config.max_molecules}, because there won't be more work"
        #     )

    def name(self) -> str:
        return "ligen-vscreen"

    def parameters(self) -> Dict[str, Any]:
        return {
            "smi": self.config.smi_path.name,
            "molecules-per-task": self.config.max_molecules,
            "screening-threads": self.screening_threads,
        }

    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        from hyperqueue.task.function import PythonEnv

        with activate_cwd(env.workdir):
            python_env = PythonEnv(
                prologue=f"""export PYTHONPATH=$PYTHONPATH:{os.getcwd()}""",
                python_bin=sys.executable,
            )
            client = env.create_client(python_env=python_env)
            submitted = self.submit_ligen_benchmark(env, client)
            client.wait_for_jobs([submitted])
        return create_result(get_last_hq_job_duration(env))

    def submit_ligen_benchmark(
        self, env: HqEnvironment, client: Client
    ) -> SubmittedJob:
        from ligate.awh.pipeline.virtual_screening import VirtualScreeningPipelineConfig
        from ligate.awh.pipeline.virtual_screening import (
            hq_submit_ligen_virtual_screening_workflow,
        )
        from ligate.awh.ligen.common import LigenTaskContext
        from ligate.awh.ligen.virtual_screening import CpuResourceConfig

        workdir = ensure_directory(env.workdir / "ligen-work")
        tasks_workdir = ensure_directory(env.workdir / "tasks")

        ctx = LigenTaskContext(
            workdir=workdir, container_path=Path(CONTAINER_PATH).absolute()
        )

        job = Job(default_workdir=tasks_workdir)
        screening_config = VirtualScreeningPipelineConfig(
            input_smi=self.config.smi_path,
            input_probe_mol2=self.config.probe_mol2_path,
            input_protein=self.config.protein_path,
            max_molecules_per_smi=self.config.max_molecules,
        )

        cores = self.screening_threads
        hq_submit_ligen_virtual_screening_workflow(
            ctx,
            ctx.workdir / "vscreening",
            config=screening_config,
            job=job,
            resources=CpuResourceConfig(
                num_parser=cores, num_workers_unfold=cores, docknscore_cpu_cores=cores
            ),
            deps=[],
        )

        # Visualize task graph
        dot_file = env.workdir / "task-graph.dot"
        visualize_job(job, dot_file)

        return client.submit(job)


class LigenDaskWorkload(LigenHQWorkload):
    def parameters(self) -> Dict[str, Any]:
        params = super().parameters()
        params["env"] = "dask"
        return params

    def execute(self, env: DaskEnvironment) -> WorkloadExecutionResult:
        timer = Timings()
        with activate_cwd(env.workdir):
            client = env.get_client()
            with timer.time():
                self.submit_ligen_benchmark(env, client)
        return create_result(timer.duration())

    def submit_ligen_benchmark(self, env: DaskEnvironment, client: distributed.Client):
        from ligate.ligen.common import LigenTaskContext
        from ligate.ligen.expansion import create_configs_from_smi, expand_task
        from ligate.ligen.virtual_screening import ScreeningConfig, screening_task

        workdir = ensure_directory(env.workdir / "ligen-work")
        inputs = ensure_directory(workdir / "expansion" / "inputs")
        outputs = ensure_directory(workdir / "expansion" / "outputs")

        ctx = LigenTaskContext(
            workdir=workdir, container_path=Path(CONTAINER_PATH).absolute()
        )

        expansion_configs = create_configs_from_smi(
            input_smi=self.config.smi_path,
            workdir_inputs=inputs,
            workdir_outputs=outputs,
            max_molecules=self.config.max_molecules,
        )

        # TODO: use client.map
        expand_tasks = []
        for config in expansion_configs:
            with dask.annotate(resources=dict(cores=1)):
                expand_tasks.append((config, client.submit(expand_task, ctx, config)))

        screen_tasks = []
        for expand_config, expand_future in expand_tasks:
            config = ScreeningConfig(
                input_mol2=CURRENT_DIR / "datasets" / "ligen" / "crystal.mol2",
                input_pdb=CURRENT_DIR / "datasets" / "ligen" / "protein.pdb",
                output_path=workdir / f"screening-{expand_config.id}.csv",
                input_protein_name="1CVU",
                ligand_expansion=SubmittedExpansion(config=expand_config, task=None),
                cores=self.screening_threads,
                num_parser=min(20, self.screening_threads),
                num_workers_unfold=min(20, self.screening_threads),
                num_workers_docknscore=self.screening_threads,
            )

            with dask.annotate(resources=dict(cores=self.screening_threads)):
                future = client.submit(screening_task, ctx, config, expand_future)
            screen_tasks.append(future)

        # dask.visualize(*screen_tasks, filename="out.svg")
        client.gather(screen_tasks)


def get_dataset_path(path: Path) -> Path:
    return CURRENT_DIR / "datasets" / path


@register_case(cli)
class DaskVsHqLigen(TestCase):
    """
    Benchmark the duration of the LiGen workflow between HQ and Dask.
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary(debug_symbols=True)

        worker_threads = min(multiprocessing.cpu_count(), 64)
        hq_env = dataclasses.replace(
            single_node_hq_cluster(hq_path, worker_threads=worker_threads),
            generate_event_log=False,
        )
        dask_env = single_node_dask_cluster(worker_threads=worker_threads)
        timeout = datetime.timedelta(minutes=10)

        input_smi = CURRENT_DIR / "datasets/ligen/artif-32.smi"
        variants = [
            (1, 1),
            (4, 4),
        ]  # , (4, 4), (8, 8), (32, 4)]  # One molecule per task, one thread per task

        def gen_descriptions(
            env: EnvironmentDescriptor, workload_cls
        ) -> List[BenchmarkDescriptor]:
            for max_molecules, threads in variants:
                if max_molecules == 1 and threads > 1:
                    continue
                config = LigenConfig(
                    container_path=CONTAINER_PATH,
                    smi_path=input_smi,
                    max_molecules=max_molecules,
                    screening_threads=threads,
                )
                workload = workload_cls(config=config)
                yield BenchmarkDescriptor(
                    env_descriptor=env, workload=workload, timeout=timeout
                )

        yield from gen_descriptions(hq_env, LigenHQWorkload)
        yield from gen_descriptions(dask_env, LigenDaskWorkload)

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = analyze_results_utilization(database)
        print(
            f"""UTILIZATION
{df}
"""
        )

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration", "environment")
            .transform("threads", lambda r: r.workload_params["screening-threads"])
            .build()
        )

        ax = sns.lineplot(df, x="threads", y="duration", hue="environment", marker="o")
        ax.set(ylabel="Duration [s]", xlabel="Threads")
        render_chart(workdir / "dask-vs-hq-ligen.png")


@register_case(cli)
class LigenAggregation(TestCase):
    """
    Benchmark the duration of the LiGen workflow between HQ and Dask.
    """

    # def get_workdir(self) -> Path:
    #     return Path("/scratch/project/dd-23-154/hq-experiments") / self.name()

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        """
        This benchmark tests the performance of Ligen + HQ when we use a single task
        per input ligand, vs. when we use 4/8/16 ligands for each task.
        """
        hq_path = get_hq_binary()

        nodes = get_active_nodes()
        # nodes = Local()

        repeat_count = 1
        variants = [
            (100, 8),
            (200, 8),
            (400, 8),
            (400, 16),
            (1000, 8),
            (5000, 8),
        ]  # (2, 1), (4, 2), (8, 4), (8, 8), (16, 8), (16, 16), (100, 8)]
        worker_counts = [1, 2, 4]

        input_files = [
            # get_dataset_path(Path("ligen-karolina/ligands-50k-eq.smi")),
            # get_dataset_path(Path("ligen-karolina/ligands-100k-eq.smi")),
            get_dataset_path(Path("ligen-karolina/ligands-200k.smi")),
            get_dataset_path(Path("ligen-karolina/ligands-200k-eq.smi")),
            # get_dataset_path(Path("ligen-karolina/ligands-24k.smi")),
            # get_dataset_path(Path("ligen-karolina/ligands-100k.smi")),
        ]
        probe_mol2 = get_dataset_path(Path("ligen-karolina/crystal_AGH62581.mol2"))
        protein_pdb = get_dataset_path(Path("ligen-karolina/protein_AGH62581.pdb"))

        parameters = list(itertools.product(input_files, variants, worker_counts))

        def gen_items():
            for (
                input_smi_file,
                (
                    max_molecules,
                    num_threads,
                ),
                worker_count,
            ) in parameters:
                env = HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes, monitor_nodes=True),
                    environment_params=dict(),
                    workers=[HqWorkerConfig() for _ in range(worker_count)],
                    binary=hq_path,
                    encryption=True,
                )

                config = LigenConfig(
                    container_path=CONTAINER_PATH,
                    smi_path=input_smi_file,
                    probe_mol2_path=probe_mol2,
                    protein_path=protein_pdb,
                    max_molecules=max_molecules,
                    screening_threads=num_threads,
                )
                workload = LigenHQWorkload(config)
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=workload,
                    repeat_count=repeat_count,
                    timeout=datetime.timedelta(minutes=60),
                )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        #         df = analyze_results_utilization(database)
        #         print(
        #             f"""UTILIZATION
        # {df}
        # """
        #         )

        util_df = analyze_per_worker_utilization(database)
        util_df["kind"] = util_df.apply(
            lambda r: f"{r['workload-molecules-per-task']}s/{r['workload-screening-threads']}t",
            axis=1,
        )
        draw_worker_utilization(util_df)
        render_chart(workdir / "ligen-aggregation-utilization")

        plt.clf()

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration", "environment")
            .transform(
                "molecules-per-task", lambda r: r.workload_params["molecules-per-task"]
            )
            .transform(
                "ligen-threads", lambda r: r.workload_params["screening-threads"]
            )
            .transform("worker-count", lambda r: r.environment_params["worker_count"])
            .transform("input-file", lambda r: r.workload_params["smi"])
            .transform("workdir", lambda r: r.benchmark_metadata["workdir"])
            .build()
        )

        render_graph_images(df)

        df["kind"] = df.apply(
            lambda row: f"{row['molecules-per-task']}s/{row['ligen-threads']}t", axis=1
        )
        df = df[df["kind"].isin(("100s/8t", "400s/8t", "1000s/8t", "5000s/8t"))]

        def draw(data, **kwargs):
            ax = sns.lineplot(
                data, x="worker-count", y="duration", hue="kind", marker="o"
            )
            for axis in ax.containers:
                ax.bar_label(axis, rotation=90, fmt="%.1f", padding=5)
            ax.set(
                ylabel="Duration [s]",
                xlabel="Worker count",
                ylim=(0, data["duration"].max() * 1.3),
            )

        def format_kind(kind: str) -> str:
            ligands, threads = kind.split("/")
            ligands = f"{ligands[:-1]} lig."
            threads = f"{threads[:-1]} thr."
            return f"{ligands}/{threads}"

        df["kind"] = df["kind"].map(format_kind)
        df["input-file"] = df["input-file"].map(rename_input_file)
        grid = sns.FacetGrid(df, col="input-file", sharey=False)
        grid.map_dataframe(draw)
        grid.add_legend()
        grid.set_titles(col_template="{col_name}")
        grid.figure.subplots_adjust(top=0.8)
        grid.figure.suptitle(
            f"Strong scalability of the LiGen virtual screening workflow"
        )

        render_chart(workdir / "ligen-aggregation-scalability")


def render_graph_images(df: pd.DataFrame):
    # Render task graphs to PNG
    if shutil.which("dot") is not None:
        for bench_dir in df["workdir"].unique():
            bench_dir = Path(bench_dir)
            task_graph_target = bench_dir / "task-graph.png"
            if not task_graph_target.is_file():
                print(f"Rendering task graph into {task_graph_target}")
                subprocess.run(
                    [
                        "dot",
                        "-Tpng",
                        str(bench_dir / "task-graph.dot"),
                        "-o",
                        str(task_graph_target),
                    ],
                    check=True,
                )


def draw_worker_utilization(df: pd.DataFrame):
    import seaborn as sns

    df["end-of-expansion"] = 0

    input_files = None
    kinds = None
    draw_parts = False

    # mode = "util-per-worker"
    # mode = "util-per-smi"
    mode = "util-per-kind"
    if mode == "util-per-worker":
        col = "worker"
        # row = "kind"
        # sharex = "col"
        row = "workload-smi"
        sharex = "row"
        worker_count = 4
        input_files = ("ligands-200k-eq.smi", "ligands-200k.smi")
        kinds = [(400, 8)]
        title = f"Node CPU utilization ({kinds[0][0]} ligands/{kinds[0][1]} threads per task, {worker_count} workers)"
        draw_parts = True
    elif mode == "util-per-kind":
        col = "kind"
        row = "workload-smi"
        sharex = "row"
        worker_count = 1
        kinds = [(100, 8), (400, 8), (1000, 8), (5000, 8)]
        input_files = ("ligands-200k-eq.smi", "ligands-200k.smi")
        title = f"Node CPU utilization (LiGen virtual screening, 1 worker, 128 cores)"
        draw_parts = True
    elif mode == "util-per-smi":
        col = "workload-smi"
        row = "kind"
        worker_count = 1
        kinds = [(400, 8)]
        title = f"Node CPU utilization ({kinds[0][0]} ligands/{kinds[0][1]} threads per task, {worker_count} workers)"
    else:
        assert False

    def kind_to_label(kind: Tuple[int, int]) -> str:
        return f"{kind[0]}s/{kind[1]}t"

    df = df[df["worker-count"] == worker_count]
    if input_files is not None:
        df = df[df["workload-smi"].isin(input_files)]
    if kinds is not None:
        kinds_str = tuple(kind_to_label(kind) for kind in kinds)
        df = df[df["kind"].isin(kinds_str)]

    def name_workers(rows):
        workers = sorted(rows.unique())
        names = {w: f"Worker {i + 1}" for (i, w) in enumerate(workers)}
        return rows.map(lambda v: names[v])

    # Normalize timestamps and worker names per benchmark
    for uuid in df["uuid"].unique():
        data = df[df["uuid"] == uuid].copy()

        end_of_expansion = find_end_of_expansion(data["workdir"].iloc[0])

        timestamp_start = data["timestamp"].min()
        data["timestamp"] -= timestamp_start
        data["end-of-expansion"] = end_of_expansion
        data["end-of-expansion"] -= timestamp_start
        assert (data["end-of-expansion"] > 0).all()
        data["worker"] = name_workers(data["worker"])
        df.loc[data.index] = data

    def draw(data, **kwargs):
        avg_util = data["utilization"].mean()
        ax = sns.lineplot(data, x="timestamp", y="utilization")

        first_row = False
        if input_files is not None:
            first_row = data["workload-smi"].iloc[0] == rename_input_file(
                input_files[0]
            )

        ymax = 119
        width = (df[df[row] == data[row].iloc[0]]["timestamp"].max()) * 1.2
        start_rel = 0.05
        x_start = -(width * start_rel)
        ax.set(
            ylabel="Node utilization (%)",
            xlabel="" if first_row else "Time from start (s)",
            xlim=(x_start, width),
            ylim=(0, ymax),
        )

        # Avg utilization
        plt.axhline(
            y=avg_util,
            color="red",
            linestyle="--",
        )
        ax.text(
            x=-(width * (start_rel + 0.01)),
            y=avg_util,
            s=f"{avg_util:.0f}%",
            color="red",
            size="medium",
            verticalalignment="center",
            horizontalalignment="right",
        )

        if draw_parts:
            part_color = "black"
            label_y = 115
            ymax_rel = label_y / ymax

            expansion_end = data["end-of-expansion"].max()
            avg_expansion = data[data["timestamp"] <= expansion_end][
                "utilization"
            ].mean()
            avg_vscreen = data[data["timestamp"] > expansion_end]["utilization"].mean()

            expansion_text = "Expansion" if first_row else "Exp."
            expansion_text += f"\n{avg_expansion:.0f}%"
            ax.text(
                x=(expansion_end // 2),
                y=label_y,
                s=expansion_text,
                size="large",
                color=part_color,
                verticalalignment="top",
                horizontalalignment="center",
            )
            plt.axvline(
                x=expansion_end, ymax=ymax_rel, color=part_color, linestyle="--"
            )

            ax.text(
                x=(expansion_end + ((width - expansion_end) // 2)),
                y=label_y,
                s=f"Scoring\n{avg_vscreen:.0f}%",
                size="large",
                color=part_color,
                verticalalignment="top",
                horizontalalignment="center",
            )

            # Makespan
            end = data["timestamp"].max()
            plt.vlines(
                x=end, ymin=-2, ymax=3, color=part_color, linewidth=2, clip_on=False
            )
            ax.text(
                x=end + (width * 0.02),
                y=2,
                s=f"{end:.0f}s",
                size="large",
                color=part_color,
                weight="bold",
                verticalalignment="bottom",
                horizontalalignment="left",
            )

    def format_kind(kind: str) -> str:
        ligands, threads = kind.split("/")
        ligands = int(ligands[:-1])
        threads = int(threads[:-1])
        subfiles = 200000 // ligands
        return f"{format_large_int(ligands)} ligands/{threads} threads per file ({subfiles} files total)"

    col_order = None
    if kinds is not None:
        col_order = [format_kind(kind_to_label(k)) for k in kinds]
    df["kind"] = df["kind"].map(format_kind)
    df["workload-smi"] = df["workload-smi"].map(rename_input_file)
    grid = sns.FacetGrid(
        df,
        col=col,
        col_order=col_order,
        row=row,
        sharey=True,
        sharex=sharex,
        margin_titles=True,
        height=5,
    )
    grid.map_dataframe(draw)
    grid.set_titles(col_template="{col_name}", row_template="{row_name}", size="large")
    grid.figure.subplots_adjust(top=0.9)
    grid.figure.suptitle(title)


def find_end_of_expansion(workdir: str) -> int:
    workdir = Path(workdir)
    cached_path = workdir / "end-of-expansion.cache"
    if cached_path.is_file():
        with open(cached_path) as f:
            return int(f.read().strip())

    end_timestamp = 0

    for file in tqdm.tqdm(list(glob.glob(str(workdir / "tasks/*.stderr")))):
        with open(file) as f:
            for line in f:
                if "INFO Finished expansion of" in line:
                    line = line.strip().split()[:2]
                    time = datetime.datetime.strptime(
                        " ".join(line), "%d-%m-%Y %H:%M:%S"
                    )
                    time = int(time.timestamp())
                    end_timestamp = max(time, end_timestamp)
    with open(cached_path, "w") as f:
        print(end_timestamp, file=f)
    return end_timestamp


def rename_input_file(file: str) -> str:
    return {
        "ligands-200k.smi": "200k ligands (skewed)",
        "ligands-200k-eq.smi": "200k ligands (uniform)",
    }[file]


if __name__ == "__main__":
    cli()

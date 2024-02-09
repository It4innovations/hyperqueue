import datetime
import logging
import multiprocessing
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, List, Iterable

import dask
import dataclasses
import distributed
import numpy as np
import pandas as pd
from ligate.ligen.expansion import SubmittedExpansion

from hyperqueue import Client, Job
from hyperqueue.job import SubmittedJob
from src.analysis.chart import render_chart_to_png
from src.analysis.dataframe import DataFrameExtractor
from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary, single_node_hq_cluster, single_node_dask_cluster
from src.cli import register_case, TestCase, create_cli
from src.environment import EnvironmentDescriptor
from src.environment.dask import DaskEnvironment
from src.environment.hq import HqEnvironment
from src.postprocessing.report import ClusterReport
from src.utils import activate_cwd, ensure_directory
from src.utils.benchmark import run_benchmarks_with_postprocessing, DEFAULT_DATA_JSON
from src.utils.timing import Timings
from src.workloads import Workload
from src.workloads.utils import create_result
from src.workloads.workload import WorkloadExecutionResult

CURRENT_DIR = Path(__file__).absolute().parent
BENCH_WORKDIR = CURRENT_DIR / Path("benchmark-run/ligen")

# LIGEN_ROOT = Path("/mnt/proj2/dd-21-9/beranekj/awh-hq")
LIGEN_ROOT = Path("/projects/it4i/ligate/cadd")
CONTAINER_PATH = LIGEN_ROOT / "ligen.sif"
assert CONTAINER_PATH.is_file()

cli = create_cli()


class LigenHQWorkload(Workload):
    def __init__(self, smi_path: Path, max_molecules: int, screening_threads: int):
        self.smi_path = smi_path
        self.max_molecules = max_molecules
        self.screening_threads = min(max_molecules, screening_threads)
        if self.screening_threads != screening_threads:
            logging.warning(f"Setting screening threads to {self.max_molecules}, because there won't be more work")

    def name(self) -> str:
        return "ligen-vscreen"

    def parameters(self) -> Dict[str, Any]:
        return {
            "env": "hq",
            "smi": self.smi_path.name,
            "molecules-per-task": self.max_molecules,
            "screening-threads": self.screening_threads,
        }

    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        timer = Timings()
        with activate_cwd(env.workdir):
            client = env.create_client()
            submitted = self.submit_ligen_benchmark(env, client)
            with timer.time():
                client.wait_for_jobs([submitted])
        return create_result(timer.duration())

    def submit_ligen_benchmark(self, env: HqEnvironment, client: Client) -> SubmittedJob:
        from ligate.ligen.common import LigenTaskContext
        from ligate.ligen.expansion import (
            create_configs_from_smi,
            submit_expansion,
        )
        from ligate.ligen.virtual_screening import ScreeningConfig, submit_screening

        workdir = ensure_directory(env.workdir / "ligen-work")
        inputs = ensure_directory(workdir / "expansion" / "inputs")
        outputs = ensure_directory(workdir / "expansion" / "outputs")

        ctx = LigenTaskContext(workdir=workdir, container_path=Path(CONTAINER_PATH).absolute())

        expansion_configs = create_configs_from_smi(
            input_smi=self.smi_path,
            workdir_inputs=inputs,
            workdir_outputs=outputs,
            max_molecules=self.max_molecules,
        )

        job = Job(workdir, default_env=dict(HQ_PYLOG="DEBUG"))
        expand_tasks = []
        for config in expansion_configs:
            expand_tasks.append(submit_expansion(ctx, config, job))

        for task in expand_tasks:
            submit_screening(
                ctx,
                ScreeningConfig(
                    input_mol2=CURRENT_DIR / "datasets" / "ligen" / "crystal.mol2",
                    input_pdb=CURRENT_DIR / "datasets" / "ligen" / "protein.pdb",
                    output_path=workdir / f"screening-{task.config.id}.csv",
                    input_protein_name="1CVU",
                    ligand_expansion=task,
                    cores=self.screening_threads,
                    num_parser=min(20, self.screening_threads),
                    num_workers_unfold=min(20, self.screening_threads),
                    num_workers_docknscore=self.screening_threads,
                ),
                job,
            )

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

        ctx = LigenTaskContext(workdir=workdir, container_path=Path(CONTAINER_PATH).absolute())

        expansion_configs = create_configs_from_smi(
            input_smi=self.smi_path,
            workdir_inputs=inputs,
            workdir_outputs=outputs,
            max_molecules=self.max_molecules,
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


def get_output_path(path: Path) -> Path:
    return ensure_directory(CURRENT_DIR / "outputs" / path)


@register_case(cli)
class DaskVsHqLigen(TestCase):
    """
    Benchmark the duration of the LiGen workflow between HQ and Dask.
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary(debug_symbols=True)

        worker_threads = min(multiprocessing.cpu_count(), 64)
        hq_env = dataclasses.replace(
            single_node_hq_cluster(hq_path, worker_threads=worker_threads), generate_event_log=False
        )
        dask_env = single_node_dask_cluster(worker_threads=worker_threads)
        timeout = datetime.timedelta(minutes=10)

        input_smi = CURRENT_DIR / "datasets/ligen/artif-32.smi"
        variants = [(1, 1), (4, 4)]  # , (4, 4), (8, 8), (32, 4)]  # One molecule per task, one thread per task

        def gen_descriptions(env: EnvironmentDescriptor, workload_cls) -> List[BenchmarkDescriptor]:
            for max_molecules, threads in variants:
                if max_molecules == 1 and threads > 1:
                    continue
                workload = workload_cls(smi_path=input_smi, max_molecules=max_molecules, screening_threads=threads)
                yield BenchmarkDescriptor(env_descriptor=env, workload=workload, timeout=timeout)

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
        render_chart_to_png(workdir / "dask-vs-hq-ligen.png")


def analyze_results_utilization(db: Database) -> pd.DataFrame:
    results = defaultdict(list)

    for key, row in db.data.items():
        workdir = row.benchmark_metadata["workdir"]
        params = row.workload_params
        duration = row.duration

        results["input"].append(params["smi"])
        results["environment"].append(row.environment)
        results["worker-threads"].append(row.environment_params["worker_threads"])
        results["screening-threads"].append(params["screening-threads"])
        results["molecules-per-task"].append(params["molecules-per-task"])
        results["duration"].append(duration)

        cluster_report = ClusterReport.load(Path(workdir))
        worker_node_utilizations = []
        worker_cpu_utilizations = []

        for node, records in cluster_report.monitoring.items():
            processes = node.processes
            worker_processes = tuple(proc.pid for proc in processes if proc.key.startswith("worker"))
            if len(worker_processes) > 0:
                for record in records:
                    avg_node_util = np.mean(record.resources.cpu)
                    worker_node_utilizations.append(avg_node_util)

                    worker_cpu_util = 0
                    for pid, process_resources in record.processes.items():
                        pid = int(pid)
                        if pid in worker_processes:
                            worker_cpu_util += process_resources.cpu + process_resources.cpu_children
                    worker_cpu_utilizations.append(worker_cpu_util)

        node_util = np.mean(worker_node_utilizations)
        worker_util = np.mean(worker_cpu_utilizations)

        results["worker-node-util"].append(node_util)
        results["worker-cpu-util"].append(worker_util)

    return pd.DataFrame(results)


def benchmark_aggregated_vs_separate_tasks():
    """
    This benchmark tests the performance of Ligen + HQ when we use a single task
    per input ligand, vs. when we use 4/8/16 ligands for each task.
    """
    hq_path = get_hq_binary()
    env = single_node_hq_cluster(hq_path, worker_threads=min(multiprocessing.cpu_count(), 64), version="base")
    input_smi = get_dataset_path(Path("ligen/artif-2.smi"))

    variants = [(1, 1)]  # , (4, 4), (8, 8)]
    descriptions = []
    for max_molecules, num_threads in variants:
        workload = LigenHQWorkload(smi_path=input_smi, max_molecules=max_molecules, screening_threads=num_threads)
        descriptions.append(
            BenchmarkDescriptor(env_descriptor=env, workload=workload, timeout=datetime.timedelta(minutes=10))
        )
    run_benchmarks_with_postprocessing(BENCH_WORKDIR, descriptions)
    df = analyze_results_utilization(BENCH_WORKDIR / DEFAULT_DATA_JSON)

    output_dir = get_output_path(Path("aggregated_vs_separate_tasks"))
    df.to_csv(output_dir / "results.csv", index=False)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s:%(asctime)s.%(msecs)03d:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # benchmark_aggregated_vs_separate_tasks()
    cli()

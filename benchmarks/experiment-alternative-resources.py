import dataclasses
import datetime
import enum
import glob
import itertools
import os
import random
import sys
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple, Union
import typing

from matplotlib import pyplot as plt
import pandas as pd

from src.clusterutils import ClusterInfo
from hyperqueue.ffi.protocol import ResourceRequest
import time

from src.postprocessing.common import (
    analyze_results_utilization,
)
from src.analysis.chart import render_chart
from hyperqueue.job import Job
from src.analysis.dataframe import DataFrameExtractor
from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.benchmark_defs import get_hq_binary
from src.cli import register_case, TestCase, create_cli
from src.clusterutils.node_list import Local, get_active_nodes
from src.environment.hq import HqEnvironment, HqClusterInfo, HqItemsWorkerResource, HqWorkerConfig
from src.utils import activate_cwd
from src.workloads import Workload
from src.workloads.utils import create_result, get_last_hq_job_duration
from src.workloads.workload import WorkloadExecutionResult

cli = create_cli()


@dataclasses.dataclass
class DeviceOnly:
    def get_cpu_per_task(self) -> int:
        return 1
    
    def format(self) -> str:
        return "device-only"


@dataclasses.dataclass
class CpuFixedRatio:
    cpu_per_task: int
    ratio: float

    def get_cpu_per_task(self) -> int:
        return self.cpu_per_task

    def format(self) -> str:
        return f"cpu-fixed-{self.ratio}"


@dataclasses.dataclass
class Variant:
    cpu_per_task: int

    def get_cpu_per_task(self) -> int:
        return self.cpu_per_task

    def format(self) -> str:
        return "variant"


ResourceMode = typing.Union[DeviceOnly]

def compute(duration: int, device_ratio: int, jitter: float):
    random.seed(int(os.environ.get("HQ_TASK_ID")))
    if jitter > 0:
        if random.random() < 0.5:
            jitter *= -1
        duration += duration * (random.random() * jitter)

    print(f"Sleeping for {duration} (jitter {jitter})")
    if os.environ.get("HQ_RESOURCE_VALUES_device", "") != "":
        print("Running on device")
        time.sleep(duration)
    else:
        print("Running on CPU")
        time.sleep(duration * device_ratio)


class DeviceVariants(Workload):
    def __init__(self, task_count: int, task_duration: int, gpu_perf_ratio: float,
                 mode: ResourceMode, jitter: float):
        self.task_count = task_count
        self.task_duration = task_duration
        self.gpu_perf_ratio = gpu_perf_ratio
        self.mode = mode
        self.jitter = jitter

    def name(self) -> str:
        return "device-variants"

    def parameters(self) -> Dict[str, Any]:
        return {
            "task-count": self.task_count,
            "task-duration": self.task_duration,
            "gpu-perf-ratio": self.gpu_perf_ratio,
            "mode": self.mode.format(),
            "cpu-per-task": self.mode.get_cpu_per_task(),
            "jitter": str(self.jitter)
        }

    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        from hyperqueue.task.function import PythonEnv

        def get_resources(task_id: int) -> Union[ResourceRequest, List[ResourceRequest]]:
            device_request = ResourceRequest(cpus=1, resources=dict(device=1))
            cpu_request = ResourceRequest(cpus=self.mode.get_cpu_per_task())

            if isinstance(self.mode, DeviceOnly):
                return device_request
            elif isinstance(self.mode, CpuFixedRatio):
                if task_id / self.task_count < self.mode.ratio:
                    return cpu_request
                return device_request
            elif isinstance(self.mode, Variant):
                return [
                    device_request,
                    cpu_request
                ]
            else:
                assert False

        with activate_cwd(env.workdir):
            python_env = PythonEnv(
                prologue=f"""export PYTHONPATH=$PYTHONPATH:{os.getcwd()}""",
                python_bin=sys.executable,
            )
            client = env.create_client(python_env=python_env)
            job = Job()
            for task_id in range(self.task_count):
                job.function(
                    compute,
                    args=(self.task_duration, self.gpu_perf_ratio, self.jitter),
                    resources=get_resources(task_id)
            )
            submitted = client.submit(job)
            client.wait_for_jobs([submitted])
        return create_result(get_last_hq_job_duration(env))


@register_case(cli)
class AlternativeResources(TestCase):
    """
    Benchmark the duration of the LiGen workflow between HQ and Dask.
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary()

        nodes = get_active_nodes()
        # nodes = Local()

        worker_count = 1
        task_count = 300
        task_duration = 1
        cpu_per_task = 10
        device_per_worker = 8
        cpus = device_per_worker * cpu_per_task + device_per_worker

        gpu_perf_ratios = [2, 5, 10]
        cpu_ratios = [0.1, 0.25, 0.5]
        jitters = [0]

        parameters = [(DeviceOnly(), 1)]
        parameters += [(CpuFixedRatio(cpu_per_task=cpu_per_task, ratio=cpu_ratio), device_ratio)
                       for (device_ratio, cpu_ratio) in itertools.product(gpu_perf_ratios, cpu_ratios)]
        parameters += [(Variant(cpu_per_task=cpu_per_task), ratio) for ratio in gpu_perf_ratios]

        def gen_descriptions():
            for (mode, gpu_perf_ratio) in parameters:
                for jitter in jitters:
                    env = HqClusterInfo(
                        cluster=ClusterInfo(node_list=nodes),
                        environment_params=dict(device_per_worker=device_per_worker),
                        workers=[HqWorkerConfig(
                            cpus=cpus,
                            resources=dict(device=HqItemsWorkerResource(items=[f"{i + 1}" for i in range(device_per_worker)]))
                        ) for _ in range(worker_count)],
                        binary=hq_path,
                        encryption=True
                    )

                    workload = DeviceVariants(
                        task_count=task_count,
                        task_duration=task_duration,
                        gpu_perf_ratio=gpu_perf_ratio,
                        mode=mode,
                        jitter=jitter
                    )
                    yield BenchmarkDescriptor(
                        env_descriptor=env, workload=workload, timeout=datetime.timedelta(seconds=3600),
                    )

        yield from gen_descriptions()

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration", "environment")
            .transform("device-count", lambda r: r.environment_params["device_per_worker"])
            .transform("task-count", lambda r: r.workload_params["task-count"])
            .transform("gpu-ratio", lambda r: r.workload_params["gpu-perf-ratio"])
            .transform("cpu-per-task", lambda r: r.workload_params["cpu-per-task"])
            .transform("mode", lambda r: r.workload_params["mode"])
            .transform("jitter", lambda r: r.workload_params["jitter"])
            .transform("workdir", lambda r: r.benchmark_metadata["workdir"])
            .build()
        )
        # def get_stats(directory: str) -> Tuple[int, int]:
        #     cpu_count = 0
        #     device_count = 0
        #     for path in glob.glob(f"{directory}/*.stdout"):
        #         with open(path) as f:
        #             data = f.read()
        #             if "device" in data:
        #                 device_count += 1
        #             else:
        #                 cpu_count += 1
        #     return (cpu_count, device_count)

        # for (_, row) in df.iterrows():
        #     (cpu_count, device_count) = get_stats(row["workdir"])
        #     print(row["gpu-ratio"], row["mode"], cpu_count, device_count)

        task_count = df["task-count"].iloc[0]
        device_count = df["device-count"].iloc[0]
        cpu_count = df["device-count"].max()

        baseline_row = df[df["mode"] == "device-only"].iloc[0].copy()
        baselines = [baseline_row]
        for ratio in df["gpu-ratio"].unique():
            if ratio != 1:
                row = baseline_row.copy()
                row["gpu-ratio"] = ratio
                baselines.append(row)
        df = pd.concat((df, pd.DataFrame(baselines, columns=df.columns)))

        df = df[df["gpu-ratio"] != 1]
        df["gpu-ratio"] = df["gpu-ratio"] * df["cpu-per-task"].max()

        def relabel_mode(mode: str) -> str:
            if mode == "variant":
                return "GPU or CPU (variant)"
            elif mode == "device-only":
                return "GPU only"
            else:
                ratio = float(mode.split("-")[2]) * 100
                return f"{100 - ratio:.0f}% GPU, {ratio:.0f}% CPU"


        def order_mode(mode: str) -> int:
            if mode == "device-only":
                return "0"
            elif mode == "variant":
                return "1"
            else:
                return mode

        modes = sorted(df["mode"].unique(), key=order_mode)

        def draw(data, **kwargs):
            ax = sns.barplot(data, hue="mode", y="duration",
                             hue_order=[relabel_mode(m) for m in modes],
                             gap=0.1)
            ax.set(ylabel="Duration [s]", ylim=(0, data["duration"].max() * 1.2))
            for axis in ax.containers:
                ax.bar_label(
                    axis,
                    fmt="%.0f",
                    padding=5
                )

        df["mode"] = df["mode"].map(relabel_mode)
        grid = sns.FacetGrid(
            df, col="gpu-ratio", sharey=True
        )
        grid.map_dataframe(draw)

        grid.add_legend(title="Resource mode")
        grid.set_titles(row_template="{row_name}", col_template="GPU to CPU perf. ratio: {col_name}x")
        grid.figure.subplots_adjust(top=0.8)
        grid.figure.suptitle(f"Load-balancing effect of resource variants ({task_count} tasks, {device_count} GPUs, {cpu_count} CPUs)")

        render_chart(workdir / "alternative-resources")


if __name__ == "__main__":
    cli()

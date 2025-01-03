from collections import defaultdict
import enum
import itertools
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Dict, Iterable

from src.postprocessing.common import format_large_int
from src.workloads.utils import measure_hq_tasks
import pandas as pd
from src.workloads.workload import Workload, WorkloadExecutionResult
from src.environment import Environment
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


class GenerateData(Workload):
    """
    Runs a task that outputs `size` bytes to stdout.
    Stdout is I/O streamed to server.
    """

    def __init__(self, task_count: int, size: int, stream: bool, scratch: bool):
        self.task_count = task_count
        self.size = size
        self.stream = stream
        self.scratch = scratch
        self.cat_bin = shutil.which("cat")
        assert self.cat_bin is not None

    def name(self) -> str:
        return "generate-data"

    def parameters(self) -> Dict[str, Any]:
        return dict(
            task_count=self.task_count,
            size=self.size,
            stream=self.stream,
            scratch=self.scratch,
        )

    def execute(self, env: Environment) -> WorkloadExecutionResult:
        assert isinstance(env, HqEnvironment)

        workdir = env.workdir
        if self.scratch:
            workdir = Path("/scratch/project/dd-23-154/hq-experiments") / workdir.name
            if workdir.is_dir():
                shutil.rmtree(workdir, ignore_errors=True)
            workdir.mkdir(parents=True, exist_ok=True)

        print(f"Executing at {workdir}")

        # data_path = workdir / "data.txt"
        # generate_data(data_path, self.size)

        log_path = workdir / "log.bin"
        args = dict()
        if self.stream:
            args["additional_args"] = ["--log", str(log_path)]

        result = measure_hq_tasks(
            env,
            [
                "/mnt/proj1/dd-23-154/beranekj/hyperqueue/benchmarks/outputter/target/release/outputter",
                str(self.size),
            ],
            stdout=None,
            task_count=self.task_count,
            workdir=workdir,
            **args,
        )

        time.sleep(5)

        log_size = 0
        stdout_size = 0
        if self.stream:
            log_size = os.path.getsize(log_path)
        else:

            def get_size() -> int:
                stdout_size = subprocess.run(["du", "-sh", workdir / "job-1"], stdout=subprocess.PIPE).stdout.decode(
                    "utf-8"
                )
                size = stdout_size.splitlines(keepends=False)[0].strip().split()[0].strip()
                multiplier = size[-1]
                size = float(size[:-1])
                if multiplier == "K":
                    size *= 1024
                elif multiplier == "M":
                    size *= 1024 * 1024
                elif multiplier == "G":
                    size *= 1024 * 1024 * 1024
                else:
                    raise Exception(f"Unknown multiplier {multiplier}")
                return math.ceil(size)

            # min_expected_size = self.task_count * self.size
            # size = get_size()
            # while size < min_expected_size:
            #     print(f"Got size {size}, minimum expected is {min_expected_size}")
            #     time.sleep(3)
            #     size = get_size()
            # time.sleep(3)
            stdout_size = get_size()

        stats = dict(log_size=log_size, stdout_size=stdout_size)
        print(f"Statistics: {stats}")
        # Store final file size statistics
        with open(env.workdir / "stats.json", "w") as f:
            json.dump(stats, f)

        # Delete the stored files
        if log_path.is_file():
            log_path.unlink()
        if not self.stream:
            shutil.rmtree(workdir / "job-1", ignore_errors=True)

        return result


def generate_data(path: Path, size: int):
    with open(path, "w") as f:
        data = "a" * size
        f.write(data)
    assert os.path.getsize(path) == size


class StreamMode(enum.Enum):
    Stdout = enum.auto()
    Stream = enum.auto()
    StreamNoWrite = enum.auto()


@register_case(cli)
class IoStreaming(TestCase):
    """
    Benchmark how much does I/O streaming help performance when outputting a lot of data
    from tasks.

    Should be run on 5 nodes (server + up to 4 workers).
    """

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        hq_path = get_hq_binary()

        # nodes = get_active_nodes()
        nodes = Local()

        # repeat_count = 3
        repeat_count = 1

        task_counts = [10000, 50000]
        sizes = [10000, 100000]
        worker_counts = [1]  # , 2, 4]
        modes = [StreamMode.Stdout, StreamMode.Stream, StreamMode.StreamNoWrite]
        scratch_values = [True, False]

        def gen_items():
            for task_count, size, worker_count, mode, scratch in itertools.product(
                task_counts, sizes, worker_counts, modes, scratch_values
            ):
                env = HqClusterInfo(
                    cluster=ClusterInfo(node_list=nodes),
                    environment_params=dict(
                        stream_mode={
                            StreamMode.Stdout: "stdout",
                            StreamMode.Stream: "stream",
                            StreamMode.StreamNoWrite: "stream-no-write",
                        }[mode],
                    ),
                    workers=[HqWorkerConfig() for _ in range(worker_count)],
                    binary=hq_path,
                    fast_spawn=True,
                    skip_stream_log_write=mode == StreamMode.StreamNoWrite,
                )
                yield BenchmarkDescriptor(
                    env_descriptor=env,
                    workload=GenerateData(
                        task_count=task_count,
                        size=size,
                        stream=mode in (StreamMode.Stream, StreamMode.StreamNoWrite),
                        scratch=scratch,
                    ),
                    repeat_count=repeat_count,
                )

        return list(gen_items())

    def postprocess(self, workdir: Path, database: Database):
        import seaborn as sns

        df = (
            DataFrameExtractor(database)
            .extract("index", "duration")
            .transform("task_count", lambda r: r.workload_params["task_count"])
            .transform("size", lambda r: r.workload_params["size"])
            .transform("stream", lambda r: r.environment_params["stream_mode"])
            .transform(
                "disk",
                lambda r: "SCRATCH" if r.workload_params["scratch"] else "PROJECT",
            )
            .transform("worker_count", lambda r: r.environment_params["worker_count"])
            .build()
        )

        def draw(data, **kwargs):
            data = data.copy()
            data["size"] = data["size"].map(format_large_int)
            ax = sns.barplot(data, x="size", y="duration", hue="stream")
            for axis in ax.containers:
                ax.bar_label(axis, rotation=90, fmt="%.1f", padding=5)
            ax.set(
                ylabel="Duration [s]",
                xlabel="Output size per task [B]",
                ylim=(0, data["duration"].max() * 1.3),
            )

        df["task_count"] = df["task_count"].map(lambda v: f"{format_large_int(v)} tasks")
        grid = sns.FacetGrid(df, col="task_count", row="disk", sharey=True, margin_titles=True)
        grid.map_dataframe(draw)

        remap_names = {
            "stdout": "Stdout",
            "stream": "Streaming",
            "stream-no-write": "Streaming (no write)",
        }
        data = {remap_names[k]: v for (k, v) in grid._legend_data.items()}
        grid.add_legend(title="Output mode", legend_data=data)
        grid.set_titles(col_template="{col_name}", row_template="{row_name} disk")
        grid.figure.subplots_adjust(top=0.85)
        grid.figure.suptitle("Effect of output streaming (1 worker)")

        render_chart(workdir / "io-streaming")
        # render_size_latex_table(database, workdir)


def render_size_latex_table(database: Database, workdir: Path):
    size_df = defaultdict(list)

    for value in database.data.values():
        run_workdir = value.benchmark_metadata["workdir"]
        path = workdir / Path(run_workdir).name
        with open(path / "stats.json") as f:
            stats = json.load(f)
        stream = value.workload_params["stream"]
        if stream:
            size = stats["log_size"]
        else:
            size = stats["stdout_size"]
        size_df["size"].append(size)
        size_df["task_count"].append(value.workload_params["task_count"])
        size_df["task_size"].append(value.workload_params["size"])
        size_df["worker_count"].append(value.environment_params["worker_count"])
        size_df["kind"].append("stream" if stream else "stdout")

    size_df = pd.DataFrame(size_df)
    size_df = size_df[size_df["worker_count"] == 1]
    size_df = size_df.sort_values("task_size")

    table = """
\\begin{table}[h]
\t\\centering
\t\\begin{tabular}{|r|r|r|r|r|}
\t\t\hline
\t\tOutput per task [B] & Task count & Size (stream) [MiB] & Size (stdout) [MiB] & Ratio \\\\ \hline
"""
    for _, row in size_df.iterrows():
        if row["kind"] != "stdout":
            continue
        task_count = row["task_count"]
        task_size = row["task_size"]
        size_stdout = math.ceil(row["size"] / (1024 * 1024))
        stream_df = size_df[size_df["task_count"] == task_count]
        stream_df = stream_df[stream_df["task_size"] == task_size]
        stream_df = stream_df[stream_df["kind"] == "stream"]
        assert len(stream_df["size"].unique()) == 1
        size_stream = math.ceil(stream_df["size"].mean() / (1024 * 1024))
        ratio = size_stdout / size_stream
        table += f"\t\t{task_size} & {task_count} & {size_stream} & {size_stdout} & {ratio:.2f}x \\\\ \hline\n"

    table += """\t\\end{tabular}
\t\\caption{Size of task output on disk with and without output streaming}
\t\\label{tab:hq-io-streaming-size}
\end{table}
"""
    print("On disk size LaTeX table\n")
    print(table)


if __name__ == "__main__":
    cli()

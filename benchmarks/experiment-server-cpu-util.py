from collections import defaultdict
import datetime
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List
import json

import tqdm

# from src.build.hq import Profile
from src.postprocessing.common import format_large_int
from src.utils import ensure_directory
from src.benchmark_defs import get_hq_binary

import matplotlib
import numpy as np
import pandas as pd
import psutil


def spawn_workers(
    hq_binary: Path, dir: str, worker_count: int
) -> List[subprocess.Popen]:
    processes = []
    for _ in range(worker_count):
        processes.append(
            subprocess.Popen(
                [
                    "salloc",
                    "-ADD-23-154",
                    "-pqcpu",
                    "--time=00:20:00",
                    "--",
                    "srun",
                    hq_binary,
                    "--server-dir",
                    dir,
                    "worker",
                    "start",
                ],
                # stdout=subprocess.DEVNULL,
                # stderr=subprocess.DEVNULL,
            )
        )
    return processes


def measure_server_util(
    hq_binary: Path,
    task_count: int,
    total_duration: datetime.timedelta,
    worker_count: int,
    worker_cpus: int,
    repeat: int,
):
    print(f"Measuring {task_count} tasks, {worker_count} workers")
    base_dir = Path(__file__).absolute().parent / "shared-workdir"
    base_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=base_dir) as dir:
        server = subprocess.Popen(
            [hq_binary, "--server-dir", dir, "server", "start"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        process = psutil.Process(server.pid)

        # warm up the cache
        process.cpu_times()
        process.memory_info()

        # wait for the server
        while True:
            if len(os.listdir(dir)) == 0:
                print("Waiting for HQ server to start")
                time.sleep(1)
            else:
                break

        workers = spawn_workers(hq_binary, dir, worker_count)
        # wait for the workers
        while True:
            output = subprocess.run(
                [
                    hq_binary,
                    "--server-dir",
                    dir,
                    "worker",
                    "list",
                    "--output-mode",
                    "json",
                ],
                stdout=subprocess.PIPE,
            ).stdout.decode()
            actual_worker_count = len(json.loads(output))
            if actual_worker_count == worker_count:
                break
            print(f"Got {actual_worker_count}/{worker_count} worker(s)")
            time.sleep(1)

        task_duration = total_duration.total_seconds() / task_count
        task_duration *= worker_count * worker_cpus
        # task_duration = 0.01
        task_duration = f"{task_duration:.4f}"

        print(
            f"tasks: {task_count}, total duration: {total_duration.total_seconds()}s, task duration: {task_duration}s"
        )

        mem_usage = None
        cpu_usages = []
        for _ in range(repeat):
            start = time.time()
            subprocess.run(
                [
                    hq_binary,
                    "--server-dir",
                    dir,
                    "submit",
                    "--array",
                    f"1-{task_count}",
                    "--progress",
                    "--stdout",
                    "none",
                    "--stderr",
                    "none",
                    "--",
                    "sleep",
                    task_duration,
                ]
            )
            duration = time.time() - start
            print(
                f"Run took {duration:.3f}s, should have been {total_duration.total_seconds():.3f}s"
            )
            if mem_usage is None:
                mem_usage = process.memory_info()
            cpu_usages.append(process.cpu_times())

        subprocess.run([hq_binary, "--server-dir", dir, "server", "stop"])
        server.wait()
        for worker in workers:
            worker.wait()
        return (cpu_usages, mem_usage, duration)


def create_chart_increase_tasks(df: pd.DataFrame):
    import seaborn as sns
    import matplotlib.pyplot as plt

    plt.clf()

    output_dir = ensure_directory(Path("outputs/charts"))
    df["utilization"] = df["cpu-usage-user"] + df["cpu-usage-system"]
    df = df[df["worker-count"] == 12]

    ax = sns.lineplot(df, x="task-count", marker="o", y=df["utilization"])
    ax.set(
        ylabel="CPU time [s]",
        xlabel="Task count",
        ylim=(0, df["utilization"].max() * 1.2),
        xlim=(0, df["task-count"].max() * 1.1),
        title="Server CPU consumption (12 workers, 1 minute span)",
    )
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, pos: format_large_int(int(v))))
                   
    for x in df["task-count"].unique():
        y = df[df["task-count"] == x]["utilization"].mean()
        ax.text(x + 4000, y - 0.15, f"{y:.2f}")

    rate = df["utilization"] / df["duration"]
    rate /= df["task-count"] / 1000
    print(rate)
    print(np.mean(rate))

    plt.tight_layout()
    plt.savefig(f"{output_dir}/server-utilization-tasks.png")
    plt.savefig(f"{output_dir}/server-utilization-tasks.pdf")


def create_chart_increase_workers(df: pd.DataFrame):
    import seaborn as sns
    import matplotlib.pyplot as plt

    plt.clf()

    task_count = 50000

    output_dir = ensure_directory(Path("outputs/charts"))
    df["utilization"] = df["cpu-usage-user"] + df["cpu-usage-system"]
    df = df[df["task-count"] == task_count]

    ax = sns.scatterplot(df, x="worker-count", y=df["utilization"])
    ax.set(
        ylabel="CPU time [s]",
        xlabel="Worker count",
        ylim=(0, df["utilization"].max() * 1.2),
        xlim=(0, df["worker-count"].max() * 1.1),
        title=f"Server CPU consumption ({task_count} tasks, 1 minute span)",
    )

    for x, y in zip(df["worker-count"], df["utilization"]):
        same_count = df[df["worker-count"] == x]
        if y == same_count["utilization"].max():
            ax.text(x, y + 0.05, f"{y:.2f}")

    plt.savefig(f"{output_dir}/server-utilization-workers.png")
    plt.savefig(f"{output_dir}/server-utilization-workers.pdf")


def dump_results(results):
    df = pd.DataFrame(results)
    outputs = ensure_directory(Path("outputs"))
    df.to_csv(f"{outputs}/server-cpu-util.csv", index=False)


def run():
    HQ_BINARY = get_hq_binary(profile=Profile.Dist)

    total_duration = datetime.timedelta(minutes=1)
    repeat = 3

    configurations = []

    # Scale tasks
    configurations.extend((tc, 12) for tc in [10000, 50000, 100000, 150000, 200000])

    # Scale workers
    configurations.extend((50000, wc) for wc in [1, 2, 4, 8, 12])

    print(f"Benchmarking {len(configurations)} configurations")

    worker_cpus = 128
    results = defaultdict(list)
    for task_count, worker_count in tqdm.tqdm(configurations):
        cpu_usages, mem_usage, duration = measure_server_util(
            HQ_BINARY,
            task_count=task_count,
            total_duration=total_duration,
            worker_count=worker_count,
            worker_cpus=worker_cpus,
            repeat=repeat,
        )
        print(cpu_usages, mem_usage)

        last_usage = None
        for usage in cpu_usages:
            if last_usage is not None:
                user_usage = usage.user - last_usage.user
                system_usage = usage.system - last_usage.system
            else:
                user_usage = usage.user
                system_usage = usage.system
            last_usage = usage

            results["task-count"].append(task_count)
            results["worker-count"].append(worker_count)
            results["worker-cpus"].append(worker_cpus)
            results["cpu-usage-user"].append(user_usage)
            results["cpu-usage-system"].append(system_usage)
            results["mem-rss"].append(mem_usage.rss)
            results["duration"].append(duration)
            results["total-duration"].append(total_duration.total_seconds())
        dump_results(results)


if __name__ == "__main__":
    # run()

    create_chart_increase_tasks(pd.read_csv("outputs/server-cpu-util.csv"))
    create_chart_increase_workers(pd.read_csv("outputs/server-cpu-util.csv"))

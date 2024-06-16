from collections import defaultdict
import datetime
import itertools
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List
import json

import tqdm

from src.utils import ensure_directory

import pandas as pd
import numpy as np

import psutil


def spawn_workers(hq_binary: Path, dir: str, worker_count: int) -> List[subprocess.Popen]:
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
            print(f"Run took {duration:.3f}s, should have been {total_duration.total_seconds():.3f}s")
            if mem_usage is None:
                mem_usage = process.memory_info()
            cpu_usages.append(process.cpu_times())

        subprocess.run([hq_binary, "--server-dir", dir, "server", "stop"])
        server.wait()
        for worker in workers:
            worker.wait()
        return (cpu_usages, mem_usage, duration)


def create_chart(df: pd.DataFrame):
    import seaborn as sns
    import matplotlib.pyplot as plt

    output_dir = ensure_directory(Path("outputs/charts"))
    df["utilization"] = df["cpu-usage-user"] + df["cpu-usage-system"]
    df = df[df["worker-count"] == 12]

    ax = sns.scatterplot(df, x="task-count", y=df["utilization"])
    ax.set(
        ylabel="CPU time [s]",
        xlabel="Task count",
        ylim=(0, 5),
        xlim=(0, 220000),
        title="Server CPU consumption (12 workers, 1 minute span)",
    )

    for x, y in zip(df["task-count"], df["utilization"]):
        same_count = df[df["task-count"] == x]
        if y == same_count["utilization"].max():
            ax.text(x + 0.1, y + 0.1, f"{y:.2f}")

    rate = df["utilization"] / df["duration"]
    rate /= df["task-count"] / 1000
    print(rate)
    print(np.mean(rate))

    plt.savefig(f"{output_dir}/server-utilization-tasks.png")
    plt.savefig(f"{output_dir}/server-utilization-tasks.pdf")


if __name__ == "__main__":
    HQ_BINARY = Path(__file__).absolute().parent.parent / "target" / "dist" / "hq"

    total_duration = datetime.timedelta(minutes=1)
    task_counts = [10000, 50000, 100000, 150000, 200000]
    worker_counts = [12]

    # total_duration = datetime.timedelta(seconds=5)
    # task_counts = [1000, 2000]  # 10000, 25000, 50000, 100000, 250000]
    # worker_counts = [1]  # , 4, 8, 12]

    def dump_results(results):
        df = pd.DataFrame(results)
        outputs = ensure_directory(Path("outputs"))
        df.to_csv(f"{outputs}/server-cpu-util.csv", index=False)

    worker_cpus = 128
    results = defaultdict(list)
    for task_count, worker_count in tqdm.tqdm(tuple(itertools.product(task_counts, worker_counts))):
        cpu_usages, mem_usage, duration = measure_server_util(
            HQ_BINARY,
            task_count=task_count,
            total_duration=total_duration,
            worker_count=worker_count,
            worker_cpus=worker_cpus,
            repeat=5,
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

    create_chart(pd.read_csv("outputs/server-cpu-util.csv"))

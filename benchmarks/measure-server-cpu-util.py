import datetime
import multiprocessing
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List

import psutil


def spawn_workers(hq_binary: Path, dir: str, worker_count: int) -> List[subprocess.Popen]:
    processes = []
    for _ in range(worker_count):
        processes.append(
            subprocess.Popen(
                [
                    "salloc",
                    "-ADD-23-154",
                    hq_binary,
                    "--server-dir",
                    dir,
                    "worker",
                    "start",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
    return processes


def measure_server_util(
    hq_binary: Path, task_count: int, total_duration: datetime.timedelta, worker_count: int, worker_cpus: int
):
    with tempfile.TemporaryDirectory() as dir:
        server = subprocess.Popen(
            [hq_binary, "--server-dir", dir, "server", "start"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        process = psutil.Process(server.pid)
        # warm up the cache
        process.cpu_times()
        process.memory_info()

        while True:
            if len(os.listdir(dir)) == 0:
                print("Waiting for HQ server to start")
                time.sleep(1)
            else:
                break

        workers = spawn_workers(hq_binary, dir, worker_count)

        task_duration = total_duration.total_seconds() / task_count
        task_duration *= worker_count * worker_cpus
        task_duration = f"{task_duration:.4f}"

        print(f"tasks: {task_count}, total duration: {total_duration.total_seconds()}s, task duration: {task_duration}")

        start = time.time()
        subprocess.run(
            [
                hq_binary,
                "--server-dir",
                dir,
                "submit",
                "--array",
                f"1-{task_count}",
                "--wait",
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

        cpu_usage = process.cpu_times()
        mem_usage = process.memory_info()
        print(cpu_usage, mem_usage)

        subprocess.run([hq_binary, "--server-dir", dir, "server", "stop"])
        server.wait()
        for worker in workers:
            worker.wait()


if __name__ == "__main__":
    HQ_BINARY = Path(__file__).absolute().parent.parent / "target" / "debug" / "hq"
    task_counts = [1000]
    worker_counts = [1]
    # total_duration = datetime.timedelta(minutes=5)
    total_duration = datetime.timedelta(seconds=10)
    measure_server_util(
        HQ_BINARY,
        task_count=task_counts[0],
        total_duration=total_duration,
        worker_count=worker_counts[0],
        worker_cpus=multiprocessing.cpu_count(),
    )

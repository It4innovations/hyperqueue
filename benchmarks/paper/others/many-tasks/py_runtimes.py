"""Submit N no-op tasks to a Python task runtime with 2 single-CPU workers.

Prints: tool N submit_s total_s
"""

import sys
import time


def noop(i):
    return i


def proc(i):
    import subprocess  # Parsl serializes the function without module globals

    subprocess.run(["true"], check=True)
    return i


FN = noop


def run_dask(n):
    from dask.distributed import Client, LocalCluster, wait

    with LocalCluster(n_workers=2, threads_per_worker=1, processes=True,
                      dashboard_address=None) as cluster, Client(cluster) as client:
        t0 = time.perf_counter()
        futs = client.map(FN, range(n), pure=False)
        t1 = time.perf_counter()
        wait(futs)
        t2 = time.perf_counter()
    return t1 - t0, t2 - t0


def run_ray(n):
    import ray

    ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False)
    f = ray.remote(num_cpus=1)(FN)
    # warm-up so that worker start is not counted
    ray.get([f.remote(0) for _ in range(2)])
    t0 = time.perf_counter()
    refs = [f.remote(i) for i in range(n)]
    t1 = time.perf_counter()
    ray.get(refs)
    t2 = time.perf_counter()
    ray.shutdown()
    return t1 - t0, t2 - t0


def run_parsl(n):
    import parsl
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.providers import LocalProvider

    cfg = Config(executors=[HighThroughputExecutor(
        label="htex", max_workers_per_node=2, cores_per_worker=1,
        provider=LocalProvider(init_blocks=1, max_blocks=1))])
    dfk = parsl.load(cfg)
    app = parsl.python_app(FN, executors=["htex"])
    app(0).result()  # warm-up
    t0 = time.perf_counter()
    futs = [app(i) for i in range(n)]
    t1 = time.perf_counter()
    for f in futs:
        f.result()
    t2 = time.perf_counter()
    dfk.cleanup()
    return t1 - t0, t2 - t0


if __name__ == "__main__":
    tool, n = sys.argv[1], int(sys.argv[2])
    base, _, variant = tool.partition("-")
    if variant == "proc":
        FN = proc
    sub, tot = {"dask": run_dask, "ray": run_ray, "parsl": run_parsl}[base](n)
    print(f"RESULT {tool} {n} {sub:.2f} {tot:.2f}", flush=True)

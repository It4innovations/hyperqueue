"""Run one scenario on Dask distributed (one `dask worker` process per fake node).

usage: run_dask.py ARM SCENARIO OUTPUT_JSON [DIV]
Scenarios with resource variants are not supported (Dask has no variants).
Each worker gets nthreads = its CPU count and abstract resources CPU/GPU;
each task asks for resources={"CPU": c, "GPU": g}.
"""

import json
import os
import subprocess
import sys
import tempfile
import time

from distributed import Client, get_worker, wait

import workload

BIN = os.path.dirname(sys.executable)


def task(tid, duration):
    s = time.time()
    time.sleep(duration)
    e = time.time()
    return tid, get_worker().name, s, e


def main():
    arm, name, output = sys.argv[1:4]
    div = float(sys.argv[4]) if len(sys.argv) > 4 else workload.DIV
    sc = workload.scenario(name, div)
    assert not sc.uses_variants, "Dask has no resource variants"

    work = tempfile.mkdtemp(prefix="dask-", dir=os.path.dirname(os.path.abspath(output)))
    sfile = os.path.join(work, "scheduler.json")
    log = open(os.path.join(work, "dask.log"), "w")
    procs = [subprocess.Popen([f"{BIN}/dask", "scheduler", "--scheduler-file", sfile,
                               "--port", "0", "--dashboard-address", ":0"],
                              stdout=log, stderr=subprocess.STDOUT)]
    while not os.path.exists(sfile):
        time.sleep(0.05)
    time.sleep(0.2)

    def start_worker(label, cpus, gpus):
        res = f"CPU={cpus}" + (f",GPU={gpus}" if gpus else "")
        procs.append(subprocess.Popen(
            [f"{BIN}/dask", "worker", "--scheduler-file", sfile, "--name", label,
             "--nthreads", str(cpus), "--nworkers", "1", "--resources", res,
             "--memory-limit", "0", "--no-dashboard"],
            stdout=log, stderr=subprocess.STDOUT))

    late = []
    for label, ntype, cpus, gpus in workload.nodes():
        if ntype in sc.late_nodes:
            late.append((sc.late_nodes[ntype], label, cpus, gpus))
        else:
            start_worker(label, cpus, gpus)
    expected = sum(1 for _, ntype, _, _ in workload.nodes() if ntype not in sc.late_nodes)

    with Client(scheduler_file=sfile) as client:
        client.wait_for_workers(expected)
        t0 = time.time()
        futs = []
        for t in sc.tasks:
            c, g, d = t.variants[0]
            res = {"CPU": c, "GPU": g} if g else {"CPU": c}
            futs.append(client.submit(task, t.tid, d, resources=res, pure=False,
                                      key=f"task-{t.tid}"))
        t_submit = time.time()
        for at, label, cpus, gpus in sorted(late):
            time.sleep(max(0.0, t0 + at - time.time()))
            start_worker(label, cpus, gpus)
        wait(futs)
        t_end = time.time()
        recs = [[tid, 0, label, s, e] for tid, label, s, e in client.gather(futs)]

    for p in reversed(procs):
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
    subprocess.run(["rm", "-rf", work])

    out = {"arm": arm, "scenario": name, "div": div, "t0": t0, "t_submit": t_submit,
           "t_end": t_end, "records": recs}
    with open(output, "w") as f:
        json.dump(out, f)
    print(f"{arm} {name}: tasks={len(recs)}/{len(sc.tasks)} "
          f"makespan={max(r[4] for r in recs) - t0:.2f}s submit={t_submit - t0:.3f}s")


if __name__ == "__main__":
    main()

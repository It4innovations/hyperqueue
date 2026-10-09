"""Compute metrics from results/<arm>/<scenario>/rep<k>.json and print markdown tables.

usage: analyze.py [RESULTS_DIR]
"""

import glob
import json
import os
import statistics
import sys
from collections import defaultdict

import numpy as np

import workload

STEP = 0.01  # time resolution of the utilization sweep [s]
ARMS = ["hq-head", "hq-v0.25.1", "ray", "dask"]
ARM_NAMES = {"hq-head": "HQ (HEAD)", "hq-v0.25.1": "HQ v0.25.1", "ray": "Ray", "dask": "Dask"}


def capacity(sc, t):
    """Available (cpus, gpus) at time t (relative to submission)."""
    cpus = gpus = 0
    for _, ntype, c, g in workload.nodes():
        if t >= sc.late_nodes.get(ntype, 0.0):
            cpus += c
            gpus += g
    return cpus, gpus


def metrics(path, lb_cache):
    d = json.load(open(path))
    sc = workload.scenario(d["scenario"], d["div"])
    tasks = {t.tid: t for t in sc.tasks}
    t0 = d["t0"]
    recs = d["records"]
    assert len(recs) == len(tasks), f"{path}: {len(recs)} of {len(tasks)} tasks"
    makespan = max(r[4] for r in recs) - t0

    key = (d["scenario"], d["div"])
    if key not in lb_cache:
        lb_cache[key] = workload.lower_bound(sc)
    lb = lb_cache[key]

    n = int(np.ceil(makespan / STEP)) + 1
    used_c = np.zeros(n)
    used_g = np.zeros(n)
    gpu_waiting = np.zeros(n)  # number of not-yet-started tasks that could use a GPU
    finish = defaultdict(float)
    flex_gpu = flex_total = 0
    for tid, v, _, s, e in recs:
        t = tasks[tid]
        c, g, _ = t.variants[v]
        a, b = int((s - t0) / STEP), int((e - t0) / STEP)
        used_c[a:b] += c
        used_g[a:b] += g
        if any(gg for _, gg, _ in t.variants):
            gpu_waiting[:a] += 1
        finish[t.cls] = max(finish[t.cls], e - t0)
        if t.cls == "flex":
            flex_total += 1
            flex_gpu += g > 0
    times = np.arange(n) * STEP
    cap = np.array([capacity(sc, x) for x in times])
    gpu_idle_waiting = np.sum(np.maximum(cap[:, 1] - used_g, 0) * (gpu_waiting > 0)) * STEP
    return {
        "arm": d["arm"],
        "scenario": d["scenario"],
        "makespan": makespan,
        "lb": lb,
        "ratio": makespan / lb,
        "gpu_util": used_g.sum() / cap[:, 1].sum(),
        "cpu_util": used_c.sum() / cap[:, 0].sum(),
        "gpu_idle_waiting": gpu_idle_waiting,
        "finish": dict(finish),
        "flex_gpu_share": flex_gpu / flex_total if flex_total else None,
    }


def med(values):
    return statistics.median(values) if values else None


def fmt_range(values, f="{:.2f}"):
    if not values:
        return "—"
    m = f.format(med(values))
    if len(values) == 1:
        return m
    return f"{m} ({f.format(min(values))}–{f.format(max(values))})"


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "results"
    lb_cache = {}
    runs = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(root, "*", "*", "rep*.json"))):
        m = metrics(path, lb_cache)
        runs[(m["scenario"], m["arm"])].append(m)
    scenarios = sorted({s for s, _ in runs}, key=lambda s: (s.split("-")[0], s))

    print("### Makespan [s]: median (min–max) and median makespan / lower bound\n")
    print("| scenario | lower bound | " + " | ".join(ARM_NAMES[a] for a in ARMS) + " |")
    print("|---" * (len(ARMS) + 2) + "|")
    for s in scenarios:
        lb = next(m["lb"] for (ss, _), ms in runs.items() if ss == s for m in ms)
        cells = []
        for a in ARMS:
            ms = runs.get((s, a), [])
            if not ms:
                cells.append("—")
                continue
            r = med([m["ratio"] for m in ms])
            cells.append(f"{fmt_range([m['makespan'] for m in ms])}, ×{r:.2f}")
        print(f"| {s} | {lb:.2f} | " + " | ".join(cells) + " |")

    print("\n### GPU utilization / GPU-seconds idle while GPU work waits / CPU utilization (medians)\n")
    print("| scenario | " + " | ".join(ARM_NAMES[a] for a in ARMS) + " |")
    print("|---" * (len(ARMS) + 1) + "|")
    for s in scenarios:
        cells = []
        for a in ARMS:
            ms = runs.get((s, a), [])
            if not ms:
                cells.append("—")
                continue
            cells.append(f"{med([m['gpu_util'] for m in ms]):.0%} / "
                         f"{med([m['gpu_idle_waiting'] for m in ms]):.1f} / "
                         f"{med([m['cpu_util'] for m in ms]):.0%}")
        print(f"| {s} | " + " | ".join(cells) + " |")

    print("\n### Finish time of each task class [s] (medians)\n")
    for s in scenarios:
        classes = sorted({c for (ss, _), ms in runs.items() if ss == s for m in ms for c in m["finish"]})
        print(f"**{s}**\n")
        print("| class | " + " | ".join(ARM_NAMES[a] for a in ARMS) + " |")
        print("|---" * (len(ARMS) + 1) + "|")
        for c in classes:
            cells = []
            for a in ARMS:
                ms = runs.get((s, a), [])
                cells.append(f"{med([m['finish'][c] for m in ms]):.2f}" if ms else "—")
            print(f"| {c} | " + " | ".join(cells) + " |")
        print()

    shares = [(s, a, med([m["flex_gpu_share"] for m in ms])) for (s, a), ms in sorted(runs.items())
              if s.endswith("-var")]
    if shares:
        print("### Share of flex tasks run with the GPU variant (variant scenarios, medians)\n")
        for s, a, share in shares:
            print(f"- {s}, {ARM_NAMES[a]}: {share:.0%}")


if __name__ == "__main__":
    main()

"""Workload definition for the heterogeneous-resource benchmark.

Everything is emulated on one machine: GPUs are plain counters and a task only
sleeps, so only the placement decisions of the scheduler matter.

All durations below are "full-scale" seconds and are divided by ``DIV`` when a
run is generated, so the same workload can be made shorter or longer.
"""

import random
from dataclasses import dataclass, field

import numpy as np
from scipy.optimize import linprog

DIV = 16.0
JITTER = 0.2
SEED = 1234

# name -> (count, cpus, gpus)
# Nodes with GPUs come first: all tools start (connect) workers in this order
NODE_TYPES = {
    "gpu": (2, 16, 4),
    "dense": (1, 8, 8),
    "cpu": (4, 32, 0),
}

# class -> list of variants (cpus, gpus, full-scale duration)
CLASSES = {
    "small-cpu": [(1, 0, 20)],
    "big-cpu": [(16, 0, 60)],
    "node-wide": [(32, 0, 60)],
    "gpu-mixed": [(4, 1, 20)],
    "gpu-heavy": [(1, 2, 30)],
    # variant 0 = GPU, variant 1 = CPU only
    "flex": [(2, 1, 20), (4, 0, 60)],
}

# full-scale time at which the "gpu" nodes join in the "B2" scenarios
LATE_JOIN = 150


@dataclass
class Task:
    tid: int
    cls: str
    variants: list  # [(cpus, gpus, duration_seconds)], already scaled and jittered


@dataclass
class Scenario:
    name: str
    tasks: list
    late_nodes: dict = field(default_factory=dict)  # node type -> join time [s]

    @property
    def uses_variants(self):
        return any(len(t.variants) > 1 for t in self.tasks)


def nodes():
    """Yield (label, type, cpus, gpus) of every worker."""
    for ntype, (count, cpus, gpus) in NODE_TYPES.items():
        for i in range(count):
            yield f"{ntype}-{i}", ntype, cpus, gpus


def _make_tasks(counts, order, flex_mode, div):
    """counts: class -> count; flex_mode: "var", "gpu", "cpu" or a GPU fraction in [0, 1]."""
    rng = random.Random(SEED)
    items = []
    for cls, count in counts.items():
        for _ in range(count):
            jitter = [1 + rng.uniform(-JITTER, JITTER) for _ in CLASSES[cls]]
            items.append((cls, jitter))
    if order == "shuffled":
        random.Random(SEED + 1).shuffle(items)
    elif order != "by-class":
        raise ValueError(order)

    flex_total = counts.get("flex", 0)
    flex_seen = 0
    tasks = []
    for tid, (cls, jitter) in enumerate(items):
        variants = [(c, g, d * j / div) for (c, g, d), j in zip(CLASSES[cls], jitter)]
        if cls == "flex":
            if flex_mode == "gpu":
                variants = variants[:1]
            elif flex_mode == "cpu":
                variants = variants[1:]
            elif flex_mode != "var":
                # Deterministic split: spread the GPU tasks evenly over the submission order
                gpu_count = round(flex_mode * flex_total)
                is_gpu = (flex_seen + 1) * gpu_count // flex_total > flex_seen * gpu_count // flex_total
                variants = variants[:1] if is_gpu else variants[1:]
            flex_seen += 1
        tasks.append(Task(tid, cls, variants))
    return tasks


A_COUNTS = {"small-cpu": 500, "big-cpu": 20, "node-wide": 4, "gpu-mixed": 120, "gpu-heavy": 40}
B_COUNTS = {"small-cpu": 500, "big-cpu": 20, "flex": 400}


def scenario(name, div=DIV):
    """Names: A-adv, A-shuf, B-var, B-gpu, B-cpu, B-split<pct>, and the same with B2."""
    if name == "A-adv":
        return Scenario(name, _make_tasks(A_COUNTS, "by-class", None, div))
    if name == "A-shuf":
        return Scenario(name, _make_tasks(A_COUNTS, "shuffled", None, div))
    prefix, mode = name.split("-", 1)
    if prefix not in ("B", "B2"):
        raise ValueError(name)
    if mode.startswith("split"):
        flex_mode = int(mode[len("split"):]) / 100
    else:
        flex_mode = mode
    late = {"gpu": LATE_JOIN / div} if prefix == "B2" else {}
    return Scenario(name, _make_tasks(B_COUNTS, "shuffled", flex_mode, div), late)


def lower_bound(sc, gpu_fraction=None):
    """Makespan lower bound from an LP relaxation over (task, variant, node type).

    Each task must be placed (fractionally) on node types where its variant fits
    one node; resource-time used per node type cannot exceed capacity times the
    time the type is available. With ``gpu_fraction`` the flex tasks are forced
    to that GPU share (used to find the best fixed split).
    """
    types = list(NODE_TYPES)
    tasks = sc.tasks
    var_index = []  # (task idx, variant idx, type idx)
    for i, t in enumerate(tasks):
        for v, (c, g, d) in enumerate(t.variants):
            for k, ty in enumerate(types):
                _, cpus, gpus = NODE_TYPES[ty]
                if c <= cpus and g <= gpus:
                    var_index.append((i, v, k))
    n = len(var_index) + 1  # last variable = T
    cost = np.zeros(n)
    cost[-1] = 1

    a_eq, b_eq = [], []
    for i in range(len(tasks)):
        row = np.zeros(n)
        for j, (ti, _, _) in enumerate(var_index):
            if ti == i:
                row[j] = 1
        a_eq.append(row)
        b_eq.append(1)
    if gpu_fraction is not None:
        row = np.zeros(n)
        flex = 0
        for j, (ti, v, _) in enumerate(var_index):
            if tasks[ti].cls == "flex" and v == 0:
                row[j] = 1
        flex = sum(1 for t in tasks if t.cls == "flex")
        a_eq.append(row)
        b_eq.append(gpu_fraction * flex)

    a_ub, b_ub = [], []
    for k, ty in enumerate(types):
        count, cpus, gpus = NODE_TYPES[ty]
        join = sc.late_nodes.get(ty, 0.0)
        for r, cap in ((0, cpus), (1, gpus)):
            # sum(req * dur * x) - count * cap * T <= -count * cap * join
            row = np.zeros(n)
            for j, (ti, v, kk) in enumerate(var_index):
                if kk == k:
                    c, g, d = tasks[ti].variants[v]
                    row[j] = (c, g)[r] * d
            row[-1] = -count * cap
            a_ub.append(row)
            b_ub.append(-count * cap * join)
    longest = max(min(d for _, _, d in t.variants) for t in tasks)
    bounds = [(0, None)] * (n - 1) + [(longest, None)]
    res = linprog(cost, A_ub=np.array(a_ub), b_ub=b_ub, A_eq=np.array(a_eq), b_eq=b_eq,
                  bounds=bounds, method="highs")
    assert res.status == 0, res.message
    return res.fun


def best_split(prefix, div=DIV):
    """GPU share (in %, multiple of 10) of flex tasks with the lowest LP bound."""
    best = None
    for pct in range(0, 101, 10):
        sc = scenario(f"{prefix}-split{pct}", div)
        lb = lower_bound(sc)
        if best is None or lb < best[1]:
            best = (pct, lb)
    return best


if __name__ == "__main__":
    for name in ("A-adv", "A-shuf", "B-var", "B-gpu", "B-cpu", "B2-var", "B2-gpu", "B2-cpu"):
        sc = scenario(name)
        print(f"{name:8} tasks={len(sc.tasks):5}  LB={lower_bound(sc):6.2f} s")
    for prefix in ("B", "B2"):
        pct, lb = best_split(prefix)
        print(f"{prefix} best fixed split: {pct}% GPU, LB={lb:.2f} s")

#!/usr/bin/env python3
"""The sampling schedule of pruning: the two tables of the supplementary material.

Reads `sampling.csv` from `run_all.sh`: the four `band-*` workloads crossed with the five
sampling shapes, three budgets G and two queue lengths, three rounds each. Every shape keeps the
same number of conditions for a given G, so the shape is the only variable.

The measure is the share of dispatched tasks placed in violation of a dropped condition,
`prune_misplaced_tasks / n_tasks_assigned`, averaged over rounds.

    python3 sampling.py --results results
"""

import argparse
import collections
import csv
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent

#: Column order of the table, with the paper's names for the shapes.
SHAPES = {
    "head": "truncation",
    "linear": "uniform",
    "quadratic": "quadratic",
    "exponential": "geometric",
    "random": "random",
}
SHIPPED = "quadratic"
WORKLOADS = {
    "band-all": "uniform",
    "band-head": "hot band, head",
    "band-middle": "hot band, middle",
    "band-tail": "hot band, tail",
}
PRODUCTION_LIMIT_S = 5.0


def load(path: Path) -> list[dict]:
    with open(path) as f:
        rows = [r for r in csv.DictReader(f) if r["sweep"] != "sweep"]
    if not rows:
        raise SystemExit(f"{path}: no rows")
    # Cuts that survived pruning are constraints of the model that produced the solution, so
    # violating one means the *evaluator* is wrong, not the scheduler.
    if any(r["kept_violated"] != "0" for r in rows):
        raise SystemExit(f"{path}: kept_violated != 0 -- the evaluator disagrees with the model")
    if any(int(r["prune_misplaced_tasks"]) > int(r["n_tasks_assigned"]) for r in rows):
        raise SystemExit(f"{path}: more misplaced than dispatched tasks -- deduplication is wrong")
    return rows


def share(row: dict) -> float:
    assigned = int(row["n_tasks_assigned"])
    return int(row["prune_misplaced_tasks"]) / assigned if assigned else 0.0


def config(row: dict) -> tuple:
    """One measured configuration: everything but the shape."""
    return row["scenario"], int(row["prune_g"]), int(row["n_tasks"]), int(row["round"])


def mean_share(rows: list[dict]) -> float:
    return statistics.mean(share(r) for r in rows) * 100


def wins(by_config: dict, a: str, b: str, keep=lambda c: True) -> tuple[int, int]:
    """Configurations where shape `a` misplaces a smaller share than `b`, and the reverse."""
    won = lost = 0
    for c, arms in by_config.items():
        if not keep(c) or a not in arms or b not in arms:
            continue
        sa, sb = share(arms[a]), share(arms[b])
        won += sa < sb
        lost += sa > sb
    return won, lost


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=HERE / "results")
    args = parser.parse_args()
    rows = load(args.results / "sampling.csv")

    by_config = collections.defaultdict(dict)
    for r in rows:
        by_config[config(r)][r["prune_schedule"]] = r
    kept = collections.defaultdict(set)
    for r in rows:
        kept[config(r)[:3]].add(r["n_cuts_after_prune"])
    if any(len(v) != 1 for v in kept.values()):
        raise SystemExit("the shapes keep different numbers of conditions -- not comparable")

    optimal = sum(r["is_optimal"] == "true" for r in rows)
    print(f"{len(by_config)} configurations, {len(rows)} measured rounds, "
          f"{optimal} solved to proven optimality")

    print("\n== Mean share of dispatched work misplaced, per workload")
    print(f"{'workload':<18}" + "".join(f"{name:>12}" for name in SHAPES.values()))
    for scenario, name in [*WORKLOADS.items(), (None, "all")]:
        selected = [r for r in rows if scenario is None or r["scenario"] == scenario]
        print(f"{name:<18}" + "".join(
            f"{mean_share([r for r in selected if r['prune_schedule'] == s]):>11.1f}%"
            for s in SHAPES))

    dispatched = {s: statistics.mean(int(r["n_tasks_assigned"]) for r in rows
                                     if r["prune_schedule"] == s) for s in SHAPES}
    print("\nmean dispatched tasks: " + ", ".join(f"{SHAPES[s]} {d:.0f}" for s, d in dispatched.items()))

    print(f"\n== {SHIPPED} against each other shape, over all configurations (won-lost)")
    for other in SHAPES:
        if other != SHIPPED:
            print(f"  vs {SHAPES[other]:<11} {'%d-%d' % wins(by_config, SHIPPED, other)}")
    for n_tasks in sorted({config(r)[2] for r in rows}):
        w, lo = wins(by_config, SHIPPED, "linear", keep=lambda c: c[2] == n_tasks)
        print(f"  vs uniform, queue of {n_tasks} tasks: {w}-{lo}")

    for g in sorted({int(r["prune_g"]) for r in rows}):
        spreading = {SHAPES[s]: mean_share([r for r in rows if r["prune_schedule"] == s
                                            and int(r["prune_g"]) == g])
                     for s in SHAPES if s != "head"}
        print(f"G = {g}: spreading shapes between {min(spreading.values()):.1f}% "
              f"and {max(spreading.values()):.1f}%")

    print(f"\n== What the budget is worth, {SHIPPED} schedule")
    print(f"{'G':>5} {'misplaced':>10} {'solve (mean)':>13} {'solve (max)':>12} {'rounds over 5 s':>16}")
    for g in sorted({int(r["prune_g"]) for r in rows}):
        selected = [r for r in rows if r["prune_schedule"] == SHIPPED and int(r["prune_g"]) == g]
        solves = [float(r["t_solve_us"]) / 1e6 for r in selected]
        over = sum(t > PRODUCTION_LIMIT_S for t in solves)
        print(f"{g:>5} {mean_share(selected):>9.1f}% {statistics.mean(solves):>12.2f}s "
              f"{max(solves):>11.2f}s {f'{over} of {len(selected)}':>16}")
    for g in sorted({int(r["prune_g"]) for r in rows}):
        selected = [r for r in rows if int(r["prune_g"]) == g]
        over = sum(float(r["t_solve_us"]) / 1e6 > PRODUCTION_LIMIT_S for r in selected)
        print(f"G = {g}, all shapes: {over} of {len(selected)} rounds over 5 s")


if __name__ == "__main__":
    main()

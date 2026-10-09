#!/usr/bin/env python3
"""Print the paper's simulation tables from the CSVs written by `run_all.sh`.

Every table is laid out as in the paper, followed by the side numbers its text quotes.

    python3 tables.py --results results            # every table
    python3 tables.py --results results ti pr      # only some

Tables: `ti` task independence, `wr` workers x resource requests, `ps` priority levels,
`pr` pruning, `path` the pathology of the reservation section.
"""

import argparse
import csv
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
TABLES = ["ti", "wr", "ps", "pr", "path"]


def load(results: Path, name: str) -> list[dict]:
    with open(results / f"{name}.csv") as f:
        return [r for r in csv.DictReader(f) if r["sweep"] != "sweep"]


def ms(row: dict, column: str) -> float:
    return float(row[column]) / 1000


def task_independence(results: Path) -> None:
    rows = load(results, "task_independence")
    print("== Independence of the task count: cold round and the median of the warm rounds (ms)")
    print(f"{'|T|':>10} {'vars':>5} {'solve (cold)':>13} {'solve (warm)':>13} {'round total (warm)':>19}")
    for n_tasks in sorted({int(r["n_tasks"]) for r in rows}):
        cell = [r for r in rows if int(r["n_tasks"]) == n_tasks]
        cold = next(r for r in cell if r["round"] == "0")
        warm = [r for r in cell if r["round"] != "0"]
        solve_warm = statistics.median(ms(r, "t_solve_us") for r in warm)
        total_warm = statistics.median(ms(r, "t_total_us") for r in warm)
        print(
            f"{n_tasks:>10} {cold['n_variables']:>5} {ms(cold, 't_solve_us'):>13.2f}"
            f" {solve_warm:>13.2f} {total_warm:>19.2f}"
        )
    partitioning = [ms(r, "t_batches_us") for r in rows]
    print(f"partitioning: {min(partitioning):.2f} to {max(partitioning):.2f} ms in every round")


def workers_requests(results: Path) -> None:
    rows = load(results, "workers_requests")
    by = {(r["n_workers"], r["n_request_types"], r["round"]): r for r in rows}
    request_types = sorted({int(r["n_request_types"]) for r in rows})
    print("\n== Workers and resource requests: solve ms, cold / mean of two warm rounds")
    print(f"{'|W|':>5} " + " ".join(f"{f'|R| = {r}':>17}" for r in request_types))
    truncated = []
    for workers in sorted({int(r["n_workers"]) for r in rows}):
        cells = []
        for rq in request_types:
            cold, *warm = (by.get((str(workers), str(rq), str(i))) for i in range(3))
            if cold is None or None in warm:
                cells.append("?")
                continue
            warm_ms = sum(ms(r, "t_solve_us") for r in warm) / len(warm)
            cells.append(f"{ms(cold, 't_solve_us'):.1f} / {warm_ms:.1f}")
            for r in (cold, *warm):
                if r["is_optimal"] != "true":
                    truncated.append(r)
        print(f"{workers:>5} " + " ".join(f"{c:>17}" for c in cells))
    for t in truncated:
        cell = [r for r in rows if (r["n_workers"], r["n_request_types"]) == (t["n_workers"], t["n_request_types"])]
        rounds = ", ".join(
            f"round {r['round']} {ms(r, 't_solve_us'):.0f} ms" + ("" if r["is_optimal"] == "true" else " (hit the limit)")
            for r in cell
        )
        print(f"{t['n_workers']} workers, {t['n_request_types']} requests: {rounds}")


def priority_scaling(results: Path) -> None:
    rows = load(results, "priority_scaling_g64")
    by = {(r["n_workers"], r["priority_levels"]): r for r in rows if r["round"] == "0"}
    levels = sorted({int(r["priority_levels"]) for r in rows})
    print("\n== Priority levels: solve s, first round, G = 64 (* = hit the limit, not optimal)")
    print(f"{'|W|':>5} " + " ".join(f"{lv:>10}" for lv in levels))
    for workers in sorted({int(r["n_workers"]) for r in rows}):
        cells = []
        for lv in levels:
            r = by.get((str(workers), str(lv)))
            if r is None:
                cells.append("?")
                continue
            solve = float(r["t_solve_us"]) / 1e6
            text = "<0.01 s" if solve < 0.005 else f"{solve:.2f} s"
            cells.append(text + ("" if r["is_optimal"] == "true" else "*"))
        print(f"{workers:>5} " + " ".join(f"{c:>10}" for c in cells))


def pruning(results: Path) -> None:
    rows = [r for r in load(results, "pruning_g") if r["round"] == "0"]
    print("\n== Pruning: first round, median solve over the repetitions")
    cuts = {r["n_cuts_before_prune"] for r in rows}
    print(f"cuts before pruning: {', '.join(sorted(cuts))}")
    print(f"{'G':>8} {'solve':>9} {'dispatched':>11} {'misplaced':>10} {'share':>7}  reps")
    for g in sorted({int(r["prune_g"]) for r in rows}):
        reps = [r for r in rows if int(r["prune_g"]) == g]
        solves = [float(r["t_solve_us"]) / 1e6 for r in reps]
        label = "unpruned" if g >= 1_000_000 else str(g)
        if all(r["n_tasks_assigned"] == "0" for r in reps):
            print(f"{label:>8} {'no solution':>9}")
            continue
        # The repetitions dispatch and misplace the same tasks; only the solve time varies.
        # Report a disagreement rather than hide it in a median.
        outcomes = {(int(r["n_tasks_assigned"]), int(r["prune_misplaced_tasks"])) for r in reps}
        if len(outcomes) != 1:
            print(f"{label:>8} repetitions differ: {sorted(outcomes)}")
            continue
        dispatched, misplaced = outcomes.pop()
        spread = (max(solves) - min(solves)) / min(solves) * 100
        print(
            f"{label:>8} {statistics.median(solves):>8.2f}s {dispatched:>11} {misplaced:>10}"
            f" {misplaced / dispatched * 100:>6.1f}%  {len(reps)}, solve spread {spread:.0f}%"
        )


def pathology(results: Path) -> None:
    rows = load(results, "pathology")
    print("\n== The pathology: tasks placed and CPUs left idle")
    names = {"strict": "strict rule", "strict+gaps": "gap filling", "all": "reservations"}
    for r in rows:
        idle = float(r["cpus_free"]) - float(r["cpus_assigned"])
        print(f"{names[r['relaxations']]:>13}: {r['n_tasks_assigned']:>3} placed, {idle:>3.0f} CPUs idle")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=HERE / "results")
    parser.add_argument("tables", nargs="*", metavar="TABLE", help=", ".join(TABLES))
    args = parser.parse_args()
    unknown = [t for t in args.tables if t not in TABLES]
    if unknown:
        parser.error(f"unknown table(s): {', '.join(unknown)}")
    run = {
        "ti": task_independence,
        "wr": workers_requests,
        "ps": priority_scaling,
        "pr": pruning,
        "path": pathology,
    }
    for name in args.tables or TABLES:
        run[name](args.results)


if __name__ == "__main__":
    main()

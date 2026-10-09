#!/usr/bin/env python3
"""`W*` analysis: what a one-shot loading window costs, and who pays it in the tail.

The `W*` scenarios give every worker a walltime and every task a time request of $95\\,\\%$ of it,
so a worker can be handed work only during the first $5\\,\\%$ of its life and afterwards merely
drains. Two consequences make the usual metrics insufficient:

* a scheduling decision is **irreversible**. Capacity not claimed inside the window is gone for
  that worker's entire lifetime, so "how full was each worker when its window shut" is the
  quantity that determines everything downstream;
* a task that misses a window waits for the **next worker**, not for the next free cpu, so queue
  waits are quantised by the worker arrival interval rather than by task durations.

Reported per arm, median over repetitions unless stated.

Usage:
    python3 walltime.py                       # reads ./results/walltime
    python3 walltime.py --results DIR
"""

import argparse
import collections
import statistics
from pathlib import Path

import journal

HERE = Path(__file__).resolve().parent
DEFAULT_RESULTS = HERE / "results" / "walltime"


def run_metrics(events: Path) -> dict:
    run = journal.load_run(events)
    meta = journal.load_meta(events)
    submitted = {job["job_id"]: job["submitted_at"] for job in meta["jobs"]}

    per_worker = collections.Counter()
    for task in run.tasks:
        per_worker[task.worker] += 1
    total_slots = sum(run.workers.values())
    used_workers = len(per_worker)

    # A worker's window shuts a fixed few seconds after it starts, so "tasks it ever ran" is
    # also "tasks it was given in its window" -- no later assignment is possible.
    loads = [per_worker.get(w, 0) for w in run.workers]
    fully_idle = sum(1 for n in loads if n == 0)

    waits = [task.start - submitted[task.job] for task in run.tasks]
    finishes = sorted(task.finish for task in run.tasks)
    makespan = max(finishes)
    # The tail proper: once the last task has *started*, everything left is execution.
    last_start = max(task.start for task in run.tasks)

    return {
        "tasks": len(run.tasks),
        "expected": meta["expected_tasks"],
        "stranded": meta["expected_tasks"] - len(run.tasks),
        "workers": len(run.workers),
        "slots": total_slots,
        "used_workers": used_workers,
        "fully_idle_workers": fully_idle,
        "slots_used": sum(loads),
        "max_load": max(loads),
        "makespan": makespan,
        "last_start": last_start,
        "wait_median": statistics.median(waits),
        "wait_p90": sorted(waits)[int(0.9 * len(waits))],
        "wait_max": max(waits),
        "tail_90_100": makespan - finishes[int(0.9 * len(finishes))],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    args = parser.parse_args()

    by_arm = collections.defaultdict(list)
    for scenario, version, rep, events in journal.iter_runs(args.results):
        by_arm[(scenario, version)].append(run_metrics(events))
    if not by_arm:
        print(f"no runs under {args.results}")
        return 1

    cols = [
        ("tasks", "finished", "{:.0f}"),
        ("stranded", "stranded", "{:.0f}"),
        ("used_workers", "workers used", "{:.0f}"),
        ("fully_idle_workers", "never used", "{:.0f}"),
        ("makespan", "makespan", "{:.1f}"),
        ("last_start", "last start", "{:.1f}"),
        ("wait_median", "wait med", "{:.1f}"),
        ("wait_p90", "wait p90", "{:.1f}"),
        ("wait_max", "wait max", "{:.1f}"),
        ("tail_90_100", "90->100%", "{:.1f}"),
    ]
    header = f"{'scenario':<9}{'version':<10}" + "".join(f"{label:>13}" for _, label, _ in cols)
    print(header)
    print("-" * len(header))
    for (scenario, version), runs in sorted(by_arm.items()):
        cells = []
        for key, _, fmt in cols:
            cells.append(f"{fmt.format(statistics.median(r[key] for r in runs)):>13}")
        print(f"{scenario:<9}{version:<10}" + "".join(cells))

    print("\nreps: " + ", ".join(f"{v}={len(r)}" for (_, v), r in sorted(by_arm.items())))
    print("wait = submit -> task start, in seconds. `never used` counts workers that expired")
    print("without running anything: capacity bought and lost, since a worker cannot be loaded")
    print("once its window has shut. `stranded` counts tasks that never started at all -- once")
    print("the last worker expires they have nowhere left to run, so the run cannot complete.")

    for (scenario, version), runs in sorted(by_arm.items()):
        spread = [f"{r['makespan']:.1f}" for r in runs]
        print(f"  {scenario}/{version:<9} makespan per rep: {', '.join(spread)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

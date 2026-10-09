#!/usr/bin/env python3
"""Measure what prefilling buys: worker idle time between consecutive tasks.

`paper.tex` §9 motivates prefilling as hiding the scheduler round-trip -- "with many identical
short tasks it is wasteful to run the scheduler after every completion; the round-trip also
leaves workers briefly idle". Throughput alone is a poor test of that, because with near-trivial
tasks it is dominated by process spawn. The direct measurement is the **gap between one task
finishing and the next starting on the same worker**, which is exactly the round-trip being hidden.

Usage:
    python3 prefill.py                          # every P* run under ./results
    python3 prefill.py --scenario P0
"""

import argparse
import collections
import statistics
from pathlib import Path

import journal

HERE = Path(__file__).resolve().parent
DEFAULT_RESULTS = HERE / "results"

#: Gaps longer than this are not round-trips -- they are the worker waiting for work that does
#: not exist yet (start-up, or the queue running dry at the end).
MAX_ROUNDTRIP_GAP_S = 1.0


def idle_gaps(run: journal.Run):
    """Per-worker gaps between consecutive tasks, in seconds."""
    by_worker = collections.defaultdict(list)
    for task in run.tasks:
        by_worker[task.worker].append((task.start, task.finish))

    gaps = []
    for spans in by_worker.values():
        spans.sort()
        # A worker runs several tasks concurrently (one per cpu), so "the next task" is the next
        # one to *start* after this one ended; track the running max finish to avoid counting
        # overlaps as negative gaps.
        latest_finish = None
        for start, finish in spans:
            if latest_finish is not None and start > latest_finish:
                gaps.append(start - latest_finish)
            latest_finish = max(latest_finish or finish, finish)
    return [g for g in gaps if g <= MAX_ROUNDTRIP_GAP_S]


def summarise(events: Path):
    run = journal.load_run(events)
    meta = journal.load_meta(events)
    gaps = idle_gaps(run)
    span = max(t.finish for t in run.tasks) - min(t.start for t in run.tasks)
    return {
        "prefill": meta.get("env", {}).get("HQ_SCHED_PREFILL_MAX", "default"),
        "tasks": len(run.tasks),
        "span": span,
        "rate": len(run.tasks) / span if span > 0 else float("nan"),
        "gap_mean_ms": statistics.mean(gaps) * 1000 if gaps else 0.0,
        "gap_median_ms": statistics.median(gaps) * 1000 if gaps else 0.0,
        "idle_total_s": sum(gaps),
        "n_gaps": len(gaps),
        "ok": len(run.tasks) == meta["expected_tasks"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--scenario", action="append", dest="scenarios")
    parser.add_argument("--version", action="append", dest="versions")
    args = parser.parse_args()

    rows = collections.defaultdict(list)
    for scenario, version, _rep, events in journal.iter_runs(args.results, args.scenarios, args.versions):
        if args.scenarios is None and not scenario.startswith("P"):
            continue
        summary = summarise(events)
        rows[(scenario, summary["prefill"])].append(summary)

    if not rows:
        print("no matching runs")
        return 1

    header = (
        f"{'scenario':<10}{'prefill':>9}{'tasks/s':>10}{'gap mean':>11}"
        f"{'gap median':>12}{'idle total':>12}{'gaps':>8}{'ok':>4}"
    )
    print(header)
    print("-" * len(header))

    def sort_key(item):
        scenario, prefill = item
        return (scenario, -1 if prefill == "default" else int(prefill))

    for key in sorted(rows, key=sort_key):
        scenario, prefill = key
        group = rows[key]
        med = lambda field: statistics.median(r[field] for r in group)  # noqa: E731
        print(
            f"{scenario:<10}{prefill:>9}{med('rate'):>10.0f}"
            f"{med('gap_mean_ms'):>10.1f}ms{med('gap_median_ms'):>11.1f}ms"
            f"{med('idle_total_s'):>11.1f}s{med('n_gaps'):>8.0f}"
            f"{('y' if all(r['ok'] for r in group) else 'NO'):>4}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

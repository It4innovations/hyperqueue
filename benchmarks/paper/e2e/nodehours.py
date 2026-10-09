#!/usr/bin/env python3
"""Measure node-seconds actually consumed when idle workers are allowed to stop.

The paper's second compaction rationale is that packing lets idle workers be terminated
early and returned to the system scheduler. `compaction.py` measures the *proxy* for this --
how many workers were free enough that they *could* have been released. This measures the
thing itself: the `N*` scenarios give every worker `--idle-timeout`, so a worker the
scheduler leaves empty stops on its own, and the run stops being billed for it.

The metric is **node-seconds**: the sum over workers of the time each was connected, charged
to the end of the run for workers that never stopped. A scheduler that packs the tail onto a
few workers releases the rest and pays less; a scheduler that spreads it keeps every worker
marginally busy and pays for the whole cluster.

**Read node-seconds together with makespan, which is printed beside it.** Releasing workers
is only a win if the work still finishes in the same time -- a scheduler that ran everything
on one worker would score wonderfully here and be useless. Neither number means anything
alone.

Two sanity checks guard the measurement rather than trusting the scenario's arithmetic:

* a worker lost for any reason other than `IdleTimeout` is a crash or a harness fault, and
  would otherwise read as a node-seconds saving;
* a worker that stops *before the last job is submitted* timed out during a lull in the
  timeline, which measures the scenario's own gaps rather than a scheduling decision.

Both are reported per run and neither is silently dropped.

Usage:
    python3 nodehours.py                    # every N* run under ./results
    python3 nodehours.py --scenario N1
    python3 nodehours.py --version head --version v0.25.1
"""

import argparse
import collections
from pathlib import Path

import journal

HERE = Path(__file__).resolve().parent
DEFAULT_RESULTS = HERE / "results"

#: The only reason that counts as the scheduler releasing a worker. Everything else is an
#: incident: `Stopped` means the harness killed it, the rest are failures.
RELEASE_REASON = "IdleTimeout"


def node_seconds(run: journal.Run):
    """-> (node_seconds, released, early, makespan).

    `released` is the number of workers that stopped on an idle timeout, `early` the number
    that did so before the last submit (a scenario artefact, not a result).
    """
    last_submit = max(run.job_created.values(), default=0.0)
    total = 0.0
    released = 0
    early = 0
    other = []
    for worker_id, connected in run.worker_connected.items():
        stop, reason = run.worker_lost.get(worker_id, (run.end, None))
        total += max(0.0, stop - connected)
        if reason is None:
            continue
        if reason != RELEASE_REASON:
            other.append(reason)
        else:
            released += 1
            if stop < last_submit:
                early += 1
    makespan = max((t.finish for t in run.tasks), default=0.0)
    return total, released, early, other, makespan


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--scenario", action="append", dest="scenarios")
    parser.add_argument("--version", action="append", dest="versions")
    args = parser.parse_args()

    rows = []
    for scenario, version, rep, events in journal.iter_runs(args.results, args.scenarios, args.versions):
        if args.scenarios is None and not scenario.startswith("N"):
            continue
        run = journal.load_run(events)
        meta = journal.load_meta(events)
        total, released, early, other, makespan = node_seconds(run)
        rows.append(
            {
                "scenario": scenario,
                "version": version,
                "rep": rep,
                "node_seconds": total,
                "released": released,
                "workers": len(run.worker_connected),
                "early": early,
                "other": other,
                "makespan": makespan,
                "tasks_ok": len(run.tasks) == meta["expected_tasks"],
            }
        )

    if not rows:
        print("no matching runs")
        return 1

    for row in sorted(rows, key=lambda r: (r["scenario"], r["version"], r["rep"])):
        flags = []
        if not row["tasks_ok"]:
            flags.append("TASK-COUNT-MISMATCH")
        if row["other"]:
            flags.append("LOST:" + ",".join(sorted(set(row["other"]))))
        if row["early"]:
            flags.append(f"{row['early']}-BEFORE-LAST-SUBMIT")
        print(
            f"{row['scenario']:<5} {row['version']:<10} {row['rep']:<8} "
            f"node-seconds {row['node_seconds']:8.1f}  "
            f"released {row['released']}/{row['workers']}  "
            f"makespan {row['makespan']:6.1f}s" + ("  " + " ".join(flags) if flags else "")
        )

    print()
    grouped = collections.defaultdict(list)
    for row in rows:
        grouped[(row["scenario"], row["version"])].append(row)
    for (scenario, version), group in sorted(grouped.items()):
        raw = " ".join(f"{r['node_seconds']:.0f}" for r in sorted(group, key=lambda r: r["node_seconds"]))
        rel = " ".join(str(r["released"]) for r in sorted(group, key=lambda r: r["released"]))
        span = " ".join(f"{r['makespan']:.0f}" for r in sorted(group, key=lambda r: r["makespan"]))
        print(f"{scenario:<5} {version:<10} n={len(group)}  node-seconds: {raw}   released: {rel}   makespan: {span}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

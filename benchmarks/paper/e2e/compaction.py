#!/usr/bin/env python3
"""Measure compaction: are tasks needlessly scattered across the cluster?

The claim under test:

    Tasks should not be needlessly scattered across the cluster: compaction eases the
    placement of large tasks and allows idle workers to be terminated early and
    returned to the system scheduler.

Compaction is only observable when the cluster has spare capacity, so this reads the
under-loaded ``C*`` scenarios. In the saturated ``S*`` scenarios every worker is busy
by construction and every arm scores identically.

All metrics come from the journal, i.e. the server's own clock, so they are comparable
across versions. The per-task cpu demand comes from ``meta.json``, since the journal
records where a task ran but not what it requested.

Usage:
    python3 compaction.py                       # table for every C* run
    python3 compaction.py --scenario C2
    python3 compaction.py --figure              # also write the paper figure
"""

import argparse
import collections
import statistics
from pathlib import Path

import journal

HERE = Path(__file__).resolve().parent
DEFAULT_RESULTS = HERE / "results"

#: An idle stretch shorter than this is not worth returning a worker for.
RELEASE_THRESHOLD = 2.0


def min_workers_for(demand: int, sizes) -> int:
    """Fewest workers whose combined cpus cover `demand`, largest first."""
    if demand <= 0:
        return 0
    total = 0
    for n, size in enumerate(sorted(sizes, reverse=True), start=1):
        total += size
        if total >= demand:
            return n
    return len(sizes)


def compaction_metrics(run: journal.Run, job_cpus) -> dict:
    all_workers = run.workers
    connected = run.worker_connected
    timeline = journal.occupancy_timeline(run, job_cpus)
    if not timeline:
        return {}

    busy_time = 0.0
    spread_acc = 0.0
    ideal_acc = 0.0
    releasable_acc = 0.0
    block_acc = 0.0
    whole_free_time = 0.0
    # worker -> time at which it went idle, while an idle stretch is open
    idle_since = {}
    idle_seconds = 0.0

    for (time, in_use), (next_time, _) in zip(timeline, timeline[1:]):
        span = next_time - time
        occupied = len(in_use)
        if occupied == 0 or span <= 0:
            # Cluster idle: not part of the average, and no worker is "wastefully" idle.
            idle_since.clear()
            continue

        # Only workers that have actually connected can be busy, idle or released.
        workers = {w: c for w, c in all_workers.items() if connected.get(w, 0.0) <= time}
        if not workers:
            continue
        demand = sum(in_use.values())
        largest_block = max(workers[w] - in_use.get(w, 0) for w in workers)

        busy_time += span
        spread_acc += occupied * span
        ideal_acc += min_workers_for(demand, list(workers.values())) * span
        releasable_acc += (len(workers) - occupied) * span
        block_acc += largest_block * span
        if any(w not in in_use for w in workers):
            whole_free_time += span

        # Accumulate per-worker idle stretches that are long enough to act on.
        for w in workers:
            if w in in_use:
                start = idle_since.pop(w, None)
                if start is not None and time - start >= RELEASE_THRESHOLD:
                    idle_seconds += time - start
            else:
                idle_since.setdefault(w, time)

    # Close out stretches still open when the cluster went quiet.
    end = timeline[-1][0]
    for start in idle_since.values():
        if end - start >= RELEASE_THRESHOLD:
            idle_seconds += end - start

    n_workers = len(all_workers)

    spread = spread_acc / busy_time if busy_time else float("nan")
    ideal = ideal_acc / busy_time if busy_time else float("nan")
    return {
        "worker_spread": spread,
        "ideal_workers": ideal,
        "scatter_ratio": spread / ideal if ideal else float("nan"),
        "releasable_workers": releasable_acc / busy_time if busy_time else float("nan"),
        "idle_worker_seconds": idle_seconds,
        "largest_free_block": block_acc / busy_time if busy_time else float("nan"),
        "p_whole_worker_free": whole_free_time / busy_time if busy_time else float("nan"),
        "n_workers": n_workers,
    }


def large_task_wait(run: journal.Run, meta: dict):
    """C2: submit-to-start of the single wide task, or None if there isn't one."""
    wide = [j for j in meta["jobs"] if j.get("cpus", 1) > 1]
    if len(wide) != 1:
        return None
    job_id = wide[0]["job_id"]
    starts = [t.start for t in run.tasks if t.job == job_id]
    if not starts or job_id not in run.job_created:
        return None
    return min(starts) - run.job_created[job_id]


def collect(results: Path, scenarios, versions):
    """-> {scenario: {version: [metrics, ...]}}"""
    out = collections.defaultdict(lambda: collections.defaultdict(list))
    for scenario, version, _rep, events in journal.iter_runs(results, scenarios, versions):
        if scenarios is None and not scenario.startswith("C"):
            continue
        run = journal.load_run(events)
        meta = journal.load_meta(events)
        metrics = compaction_metrics(run, journal.cpus_per_job(meta))
        if not metrics:
            continue
        metrics["large_task_wait"] = large_task_wait(run, meta)
        metrics["tasks_ok"] = len(run.tasks) == meta["expected_tasks"]
        out[scenario][version].append(metrics)
    return out


def _median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def print_table(data) -> None:
    header = (
        f"{'scenario':<9}{'version':<11}{'workers used':>13}{'ideal':>8}{'scatter':>9}"
        f"{'idle':>8}{'idle sec':>10}{'free block':>12}{'whole free':>12}{'big wait':>10}{'ok':>4}"
    )
    print(header)
    print("-" * len(header))
    for scenario in sorted(data):
        for version in sorted(data[scenario]):
            runs = data[scenario][version]
            wait = _median([r["large_task_wait"] for r in runs])
            print(
                f"{scenario:<9}{version:<11}"
                f"{_median([r['worker_spread'] for r in runs]):>13.2f}"
                f"{_median([r['ideal_workers'] for r in runs]):>8.2f}"
                f"{_median([r['scatter_ratio'] for r in runs]):>9.2f}"
                f"{_median([r['releasable_workers'] for r in runs]):>8.2f}"
                f"{_median([r['idle_worker_seconds'] for r in runs]):>10.1f}"
                f"{_median([r['largest_free_block'] for r in runs]):>12.1f}"
                f"{_median([r['p_whole_worker_free'] for r in runs]):>12.2f}"
                f"{('n/a' if wait is None else f'{wait:.1f}'):>10}"
                f"{('y' if all(r['tasks_ok'] for r in runs) else 'NO'):>4}"
            )


#: How an arm is named in the paper, where it is a release, versus in the harness, where it is a
#: directory under `results/` and the value you pass to `--version`. Only the figure is relabelled:
#: the console table stays on the harness names, since those are what you type to reproduce a row.
DISPLAY_NAMES = {"head": "v0.27.0"}


def display_name(version: str) -> str:
    return DISPLAY_NAMES.get(version, version)


def render_figure(results: Path, versions, scenarios, out_stem: Path) -> None:
    """Workers in use over time, one panel per (scenario, arm), with the ideal as a dashed rule.

    A step area: occupancy holds its value between events, so interpolating between them would
    invent worker counts that never existed.
    """
    import sys

    import altair as alt
    import pandas as pd

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    import viz_theme as vt

    # Cell height is fixed; cell width is fitted to the text block by `vt.fit_and_save`, since
    # the grid is as many columns wide as there are versions to compare.
    panel_h = 92

    rows = [s for s in scenarios if any(True for _ in journal.iter_runs(results, [s], versions))]
    if not rows:
        print("no runs to plot")
        return

    records, caps = [], {}
    for scenario in rows:
        for version in versions:
            runs = list(journal.iter_runs(results, [scenario], [version]))
            if not runs:
                continue
            events = runs[0][3]  # rep1
            run = journal.load_run(events)
            meta = journal.load_meta(events)
            timeline = journal.occupancy_timeline(run, journal.cpus_per_job(meta))
            connected = run.worker_connected
            for t, in_use in timeline:
                # The ideal is drawn per instant, not as the run's average. Averaging it would
                # put a flat rule across a time series whose floor moves between 1 and 8: during
                # the saturating burst every worker really is needed, so a mean line sits well
                # below the truth and reads as waste, and during the drain it sits well above it
                # and hides the waste that is the point. Measured on these runs, a flat line is
                # never once correct at any instant.
                sizes = [c for w, c in run.workers.items() if connected.get(w, 0.0) <= t]
                records.append({
                    "scenario": scenario,
                    "version": version,
                    "t": t,
                    "used": len(in_use),
                    "ideal": min_workers_for(sum(in_use.values()), sizes) if sizes else 0,
                })
            caps[scenario] = max(caps.get(scenario, 0), len(run.workers) + 0.5)

    if not records:
        print("no runs to plot")
        return
    df = pd.DataFrame(records)

    # In a small-multiples grid every repeated label is ink spent saying the same thing. On a
    # printed page that is also the difference between fitting the text width and not: the version
    # names ride the top row only, the time axis is named once at the bottom left, and "ideal" is
    # explained in the leftmost cell of each row instead of five times over.
    def build(panel_w):
        panels = []
        for scenario in rows:
            row_panels = []
            first_row = scenario == rows[0]
            last_row = scenario == rows[-1]
            for version in versions:
                cell = df[(df.scenario == scenario) & (df.version == version)]
                if cell.empty:
                    continue
                first_col = version == versions[0]
                y = alt.Y(
                    "used:Q",
                    scale=alt.Scale(domain=[0, caps[scenario]]),
                    axis=alt.Axis(title=f"{scenario} — workers in use" if first_col else None,
                                  labels=first_col),
                )
                x = alt.X(
                    "t:Q",
                    axis=alt.Axis(title="time (s)" if last_row and first_col else None,
                                  format="~s", tickCount=3),
                )
                area = alt.Chart(cell).mark_area(
                    interpolate="step-after", color=vt.SERIES[0], opacity=0.18,
                    line=alt.OverlayMarkDef(color=vt.SERIES[0], strokeWidth=1.2,
                                            interpolate="step-after"),
                ).encode(x=x, y=y)
                # Same step interpolation as the area: the floor holds its value between events
                # exactly as occupancy does, so the two are comparable at every instant.
                marks = [
                    area,
                    alt.Chart(cell)
                    .mark_line(interpolate="step-after", strokeDash=[4, 3],
                               color=vt.INK_MUTED, strokeWidth=1)
                    .encode(x=x, y=alt.Y("ideal:Q", scale=alt.Scale(domain=[0, caps[scenario]]))),
                ]
                if first_col:
                    # Anchored to the left edge, a real pixel position; anchoring right to
                    # `panel_w` misses whenever a title makes the cell wider than its plot area.
                    label_at = pd.DataFrame({"ideal": [cell["ideal"].min()]})
                    marks.append(
                        alt.Chart(label_at)
                        .mark_text(align="left", baseline="bottom", dy=-3, x=4,
                                   color=vt.INK_MUTED, fontSize=8, text="ideal")
                        .encode(y=alt.Y("ideal:Q", scale=alt.Scale(domain=[0, caps[scenario]])))
                    )
                row_panels.append(
                    alt.layer(*marks).properties(
                        width=panel_w,
                        height=panel_h,
                        title=display_name(version) if first_row else "",
                    )
                )
            if row_panels:
                panels.append(alt.hconcat(*row_panels, spacing=22))

        return (
            alt.vconcat(*panels, spacing=26)
            .configure_view(strokeOpacity=0)
            .properties(background=vt.SURFACE,
                        padding={"left": 8, "right": 16, "top": 6, "bottom": 6})
        )

    vt.fit_and_save(build, out_stem)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--scenario", action="append", dest="scenarios")
    parser.add_argument("--version", action="append", dest="versions")
    parser.add_argument("--figure", action="store_true", help="also write the paper figure")
    args = parser.parse_args()

    data = collect(args.results, args.scenarios, args.versions)
    if not data:
        print("no matching runs")
        return 1
    print_table(data)

    if args.figure:
        versions = args.versions or sorted({v for s in data for v in data[s]})
        scenarios = args.scenarios or sorted(data)
        render_figure(args.results, versions, scenarios, args.results / "compaction-workers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

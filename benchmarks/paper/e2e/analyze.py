#!/usr/bin/env python3
"""Turn exported HQ journals into scheduler-fairness metrics, plots and a report.

All timestamps come from the journal, i.e. from the server's own clock, so the
submit / start / finish times of a run are mutually consistent and independent
of the harness.
"""

import argparse
import dataclasses
import json
import os
import statistics
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

HERE = Path(__file__).resolve().parent


# --------------------------------------------------------------------------
# Journal parsing
# --------------------------------------------------------------------------


def parse_time(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


@dataclasses.dataclass
class JobRun:
    job_id: int
    name: str
    priority: int
    submitted: float
    starts: List[float] = dataclasses.field(default_factory=list)
    finishes: List[float] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Run:
    version: str
    scenario: str
    rep: int
    origin: float
    jobs: Dict[int, JobRun]
    # (start, finish, job_id, worker_id) per task, seconds relative to origin
    tasks: List[Tuple[float, float, int, int]]
    workers_connected: List[float]
    expected_tasks: int

    @property
    def makespan(self) -> float:
        return max(f for _, f, _, _ in self.tasks) - min(s for s, _, _, _ in self.tasks)

    @property
    def tasks_finished(self) -> int:
        return len(self.tasks)


def load_run(run_dir: Path) -> Run:
    meta = json.loads((run_dir / "meta.json").read_text())
    events_path = run_dir / "events.ndjson"

    jobs: Dict[int, JobRun] = {}
    job_meta = {j["job_id"]: j for j in meta["jobs"]}
    started: Dict[Tuple[int, int], Tuple[float, int]] = {}
    tasks: List[Tuple[float, float, int, int]] = []
    workers_connected: List[float] = []
    origin: Optional[float] = None

    with open(events_path) as f:
        for line in f:
            record = json.loads(line)
            t = parse_time(record["time"])
            event = record["event"]
            kind = event["type"]
            if kind == "server-start":
                origin = t
            elif kind == "worker-connected":
                workers_connected.append(t)
            elif kind == "job-created":
                job_id = event["job"]
                info = job_meta.get(job_id, {})
                jobs[job_id] = JobRun(
                    job_id=job_id,
                    name=info.get("name", event.get("job_desc", {}).get("name", str(job_id))),
                    priority=info.get("priority", 0),
                    submitted=t,
                )
            elif kind == "task-started":
                worker = event.get("worker")
                if worker is None:
                    worker = event.get("workers", [-1])[0]
                started[(event["job"], event["task"])] = (t, worker)
            elif kind == "task-finished":
                key = (event["job"], event["task"])
                if key not in started:
                    continue
                start, worker = started.pop(key)
                tasks.append((start, t, event["job"], worker))
                jobs[event["job"]].starts.append(start)
                jobs[event["job"]].finishes.append(t)

    if origin is None:
        origin = min(s for s, _, _, _ in tasks)

    for job in jobs.values():
        job.submitted -= origin
        job.starts = sorted(s - origin for s in job.starts)
        job.finishes = sorted(f - origin for f in job.finishes)

    return Run(
        version=meta["version"],
        scenario=meta["scenario"],
        rep=meta["rep"],
        origin=origin,
        jobs=jobs,
        tasks=[(s - origin, f - origin, j, w) for s, f, j, w in tasks],
        workers_connected=[t - origin for t in workers_connected],
        expected_tasks=meta["expected_tasks"],
    )


# --------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------


def percentile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        return float("nan")
    idx = min(len(sorted_values) - 1, int(q * len(sorted_values)))
    return sorted_values[idx]


def concurrent_jobs_timeavg(run: Run) -> float:
    """Average number of jobs sharing the cluster, while the cluster is busy.

    4.0 means all four jobs were in flight throughout (lockstep); 1.0 means the
    scheduler drained one job before starting the next. Idle stretches (e.g.
    before the first submit) are excluded, otherwise the number would mostly
    measure how long the cluster sat empty.
    """
    events: List[Tuple[float, int, int]] = []
    for start, finish, job_id, _ in run.tasks:
        events.append((start, +1, job_id))
        events.append((finish, -1, job_id))
    events.sort()

    active: Dict[int, int] = {}
    total = 0.0
    weighted = 0.0
    prev_t = events[0][0]
    for t, delta, job_id in events:
        span = t - prev_t
        n_active = sum(1 for c in active.values() if c > 0)
        if span > 0 and n_active > 0:
            weighted += n_active * span
            total += span
        active[job_id] = active.get(job_id, 0) + delta
        prev_t = t
    return weighted / total if total else float("nan")


def kendall_inversions(order: List[int]) -> int:
    """Number of pairs out of the expected order."""
    return sum(1 for i in range(len(order)) for j in range(i + 1, len(order)) if order[i] > order[j])


def run_metrics(run: Run) -> dict:
    per_job = []
    for job_id in sorted(run.jobs):
        job = run.jobs[job_id]
        if not job.finishes:
            continue
        last = job.finishes[-1]
        flow = last - job.submitted
        p99 = percentile(job.finishes, 0.99)
        p90 = percentile(job.finishes, 0.90)
        # How long the job takes to get through each 10% of its own tasks, once
        # it is genuinely running. A job served steadily has ten similar
        # deciles; the reported symptom is one decile taking far longer than the
        # rest -- the flat plateau in the completion curve, where the job barely
        # progresses while the cluster works on other jobs.
        #
        # Measured from the 5% mark rather than from the first task start, so
        # that the time a job legitimately spends queued behind an older job
        # (which is exactly what FIFO is supposed to do) is not counted as a
        # plateau.
        n = len(job.finishes)
        lo = n // 20
        bounds = [job.finishes[lo]] + [job.finishes[min(n - 1, lo + (k + 1) * (n - lo) // 10 - 1)] for k in range(10)]
        deciles = [b - a for a, b in zip(bounds, bounds[1:])]
        max_decile = max(deciles)
        median_decile = statistics.median(deciles)
        per_job.append(
            {
                "job_id": job_id,
                "name": job.name,
                "priority": job.priority,
                "submitted": job.submitted,
                "first_start": job.starts[0],
                "last_finish": last,
                "flow_time": flow,
                "tail_99": last - p99,
                "tail_90": last - p90,
                "tail_fraction": (last - p99) / flow if flow > 0 else float("nan"),
                "tail_90_fraction": (last - p90) / flow if flow > 0 else float("nan"),
                "max_decile": max_decile,
                "decile_ratio": max_decile / median_decile if median_decile > 0 else float("nan"),
            }
        )

    # Expected order: highest priority first, then oldest job id first.
    expected = sorted(per_job, key=lambda j: (-j["priority"], j["job_id"]))
    actual = sorted(per_job, key=lambda j: j["last_finish"])
    rank = {j["job_id"]: i for i, j in enumerate(expected)}
    inversions = kendall_inversions([rank[j["job_id"]] for j in actual])

    return {
        "version": run.version,
        "scenario": run.scenario,
        "rep": run.rep,
        "makespan": run.makespan,
        "tasks_finished": run.tasks_finished,
        "expected_tasks": run.expected_tasks,
        "mean_flow_time": statistics.mean(j["flow_time"] for j in per_job),
        # Flow time of the oldest job -- the one the user watched sit "almost done".
        "oldest_job_flow": min(per_job, key=lambda j: j["job_id"])["flow_time"],
        "max_tail_fraction": max(j["tail_fraction"] for j in per_job),
        "mean_tail_fraction": statistics.mean(j["tail_fraction"] for j in per_job),
        "max_tail_90": max(j["tail_90"] for j in per_job),
        "max_tail_90_fraction": max(j["tail_90_fraction"] for j in per_job),
        "max_decile": max(j["max_decile"] for j in per_job),
        "max_decile_ratio": max(j["decile_ratio"] for j in per_job),
        "concurrent_jobs": concurrent_jobs_timeavg(run),
        "order_inversions": inversions,
        "completion_order": [j["name"] for j in actual],
        "jobs": per_job,
    }


def aggregate(metrics: List[dict]) -> dict:
    def med(key: str) -> dict:
        values = [m[key] for m in metrics]
        return {"median": statistics.median(values), "min": min(values), "max": max(values)}

    return {
        "version": metrics[0]["version"],
        "scenario": metrics[0]["scenario"],
        "reps": len(metrics),
        "makespan": med("makespan"),
        "mean_flow_time": med("mean_flow_time"),
        "oldest_job_flow": med("oldest_job_flow"),
        "max_tail_fraction": med("max_tail_fraction"),
        "mean_tail_fraction": med("mean_tail_fraction"),
        "max_tail_90": med("max_tail_90"),
        "max_tail_90_fraction": med("max_tail_90_fraction"),
        "max_decile": med("max_decile"),
        "max_decile_ratio": med("max_decile_ratio"),
        "concurrent_jobs": med("concurrent_jobs"),
        "order_inversions": med("order_inversions"),
        "sane": all(m["tasks_finished"] == m["expected_tasks"] for m in metrics),
    }


# --------------------------------------------------------------------------
# Plots
# --------------------------------------------------------------------------



def plot_scenario(runs_by_version: Dict[str, List[Run]], scenario: str, out_dir: Path) -> List[Path]:
    """Four figures per scenario, drawn with Altair and faceted by scheduler version.

    Jobs are the categorical dimension throughout, and every figure assigns them the same slot in
    the same order -- so a job keeps its colour across the completion, occupancy and Gantt views
    and the three can be read together.
    """
    import sys

    import altair as alt
    import pandas as pd

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    import viz_theme as vt

    # Panel width is fitted per figure by `vt.fit_and_save`: these scripts emit the same figure
    # for scenarios with different numbers of versions, so one fixed width would land every one
    # of them at a different page width.
    PANEL_H = 128

    versions = sorted(runs_by_version)
    paths: List[Path] = []
    xmax = max(r.makespan for runs in runs_by_version.values() for r in runs) * 1.05

    # One colour per job, fixed across all four figures and every version panel.
    job_names: List[str] = []
    for version in versions:
        run = runs_by_version[version][0]
        for job_id in sorted(run.jobs):
            name = run.jobs[job_id].name
            if name not in job_names:
                job_names.append(name)
    job_color = alt.Color(
        "job:N",
        scale=alt.Scale(domain=job_names, range=vt.SERIES[: len(job_names)]),
        # Stroke symbols so the swatch shows each job's dash pattern -- the channel that survives
        # a monochrome print, where two adjacent palette slots can be 2 luminance points apart.
        legend=alt.Legend(title="job", orient="bottom", direction="horizontal", offset=6,
                          symbolType="stroke", symbolStrokeWidth=1.6, symbolSize=200),
    )
    x_time = alt.X(
        "t:Q", scale=alt.Scale(domain=[0, xmax], nice=False),
        axis=alt.Axis(title="time since server start (s)", format="~s"),
    )

    import copy

    def _grid_axes(index, count, x, y):
        """Axis titles for cell `index` of a `vt.grid` layout: y down the left column, x along
        the bottom row, nothing repeated in between."""
        per_row = vt.GRID_PER_ROW
        first_col = index % per_row == 0 if count > per_row else index == 0
        last_row = index >= count - per_row if count > per_row else True
        def stripped(channel, **axis):
            bare = copy.deepcopy(channel)
            bare.axis = alt.Axis(**axis)
            return bare

        return {
            "x": x if last_row else stripped(x, title=None, format="~s"),
            "y": y if first_col else stripped(y, title=None, labels=False),
        }

    def facet(df, mark_fn, y, title, subtitle=None, *, panel_w=140):
        cells = [(v, df[df.version == v]) for v in versions]
        cells = [(v, c) for v, c in cells if not c.empty]
        panels = []
        for i, (version, cell) in enumerate(cells):
            # In a grid, the y title belongs to the leftmost cell of each row and the x title to
            # the bottom row. Repeating both in every cell is ink spent saying the same thing,
            # and it is what pushes these figures past the text block.
            axes = _grid_axes(i, len(cells), x_time, y)
            panels.append(
                mark_fn(alt.Chart(cell)).encode(color=job_color, **axes).properties(
                    width=panel_w, height=PANEL_H, title=version
                )
            )
        return (
            vt.grid(panels)
            .properties(
                title=alt.TitleParams(title, subtitle=subtitle or "", subtitleColor=vt.INK_MUTED),
                background=vt.SURFACE,
                padding={"left": 8, "right": 16, "top": 6, "bottom": 6},
            )
            .configure_view(strokeOpacity=0)
        )

    def write(build, name):
        # `build` takes a panel width, so the figure can be fitted to the text block rather than
        # authored at a guess. Through `vt` for the same PDF-and-PNG, 300 ppi treatment as the
        # rest of the evaluation figures.
        stem = out_dir / f"{scenario}-{name}"
        vt.fit_and_save(build, stem)
        path = stem.with_suffix(".png")
        paths.append(path)
        return path

    # 1. Completion curves ------------------------------------------------
    rows = []
    for version in versions:
        run = runs_by_version[version][0]
        for job_id in sorted(run.jobs):
            job = run.jobs[job_id]
            for k, t in enumerate(job.finishes):
                rows.append(
                    {"version": version, "job": job.name, "t": t, "pct": 100 * (k + 1) / len(job.finishes)}
                )
    if rows:
        completion = pd.DataFrame(rows)
        write(
            lambda w: facet(
                completion,
                lambda c: c.mark_line(interpolate="step-after").encode(
                    strokeDash=vt.dash_scale("job", job_names)
                ),
                alt.Y("pct:Q", scale=alt.Scale(domain=[0, 100]), axis=alt.Axis(title="% of job's tasks finished")),
                "Per-job completion curves",
                "a flat stretch = the job barely progresses",
                panel_w=w,
            ),
            "completion",
        )

    # 2. Running tasks per job over time ----------------------------------
    rows = []
    grid = [i * xmax / 400 for i in range(401)]
    for version in versions:
        run = runs_by_version[version][0]
        for job_id in sorted(run.jobs):
            spans = [(s, f) for s, f, j, _ in run.tasks if j == job_id]
            for t in grid:
                rows.append(
                    {
                        "version": version,
                        "job": run.jobs[job_id].name,
                        "t": t,
                        "running": sum(1 for s, f in spans if s <= t < f),
                    }
                )
    if rows:
        occupancy = pd.DataFrame(rows)
        write(
            lambda w: facet(
                occupancy,
                # A hairline of surface colour between stacked segments, so adjacent jobs stay
                # separable even where their two palette slots are close in luminance.
                lambda c: c.mark_area(interpolate="step-after", stroke=vt.SURFACE, strokeWidth=1.2),
                alt.Y("running:Q", stack=True, axis=alt.Axis(title="running tasks")),
                "Which job occupies the cluster over time",
                panel_w=w,
            ),
            "occupancy",
        )

    # 3. Worker x time Gantt ----------------------------------------------
    rows = []
    for version in versions:
        run = runs_by_version[version][0]
        for start, finish, job_id, worker in run.tasks:
            rows.append(
                {
                    "version": version,
                    "job": run.jobs[job_id].name,
                    "t": start,
                    "t2": finish,
                    "worker": worker,
                }
            )
    if rows:
        gantt = pd.DataFrame(rows)

        def build_gantt(panel_w):
            cells = [(v, gantt[gantt.version == v]) for v in versions]
            cells = [(v, c) for v, c in cells if not c.empty]
            panels = []
            for i, (version, cell) in enumerate(cells):
                axes = _grid_axes(i, len(cells), x_time, alt.Y("worker:O", axis=alt.Axis(title="worker id")))
                panels.append(
                    alt.Chart(cell)
                    .mark_bar(height=3, cornerRadius=0.5)
                    .encode(x2="t2:Q", color=job_color, **axes)
                    .properties(width=panel_w, height=PANEL_H, title=version)
                )
            return (
                vt.grid(panels)
                .properties(
                    title=alt.TitleParams("Task placement per worker, coloured by job"),
                    background=vt.SURFACE,
                    padding={"left": 8, "right": 16, "top": 6, "bottom": 6},
                )
                .configure_view(strokeOpacity=0)
            )

        write(build_gantt, "gantt")

    # 4. Per-job flow time ------------------------------------------------
    rows = []
    for version in versions:
        metrics = [run_metrics(r) for r in runs_by_version[version]]
        names = [j["name"] for j in metrics[0]["jobs"]]
        for k, name in enumerate(names):
            rows.append(
                {
                    "version": version,
                    "job": name,
                    "flow": statistics.median([m["jobs"][k]["flow_time"] for m in metrics]),
                }
            )
    if rows:
        df = pd.DataFrame(rows)
        # Grouped bars: the scheduler version is the comparison, so it owns the colour; the job is
        # the category on the axis.
        def build_flowtime(panel_w):
            return (
                alt.Chart(df)
                .mark_bar(cornerRadiusEnd=2)
                .encode(
                    # Grouped on one axis via `xOffset`, not `column`. A column-faceted chart
                    # silently drops `width`, so the figure could not be sized at all and came
                    # out 30% wider than the text block.
                    x=alt.X("job:N", axis=alt.Axis(title=None, labelAngle=0, labelColor=vt.INK_SECONDARY)),
                    # paddingInner on the *offset* scale: `bandPaddingInner` spaces the job groups,
                    # not the bars inside one, which otherwise abut as a striped block.
                    xOffset=alt.XOffset("version:N", scale=alt.Scale(domain=versions, paddingInner=0.15)),
                    y=alt.Y("flow:Q", axis=alt.Axis(title="flow time (s)", format="~s")),
                    # Bars cannot take a dash pattern, so in monochrome identity rests on
                    # position: every group repeats the versions in the legend's order.
                    color=alt.Color(
                        "version:N",
                        scale=alt.Scale(domain=versions, range=vt.SERIES[: len(versions)]),
                        legend=alt.Legend(title=None, orient="bottom", direction="horizontal",
                                          offset=6, columns=3),
                    ),
                )
                .properties(
                    width=panel_w,
                    height=PANEL_H,
                    title=alt.TitleParams(f"Flow time per job — {scenario}"),
                    background=vt.SURFACE,
                    padding={"left": 8, "right": 16, "top": 6, "bottom": 6},
                )
                .configure_view(strokeOpacity=0)
                .configure_scale(bandPaddingInner=0.25)
            )

        vt.fit_and_save(build_flowtime, out_dir / f"{scenario}-flowtime")
        paths.append(out_dir / f"{scenario}-flowtime.png")

    return paths


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def render_report(summary: Dict[str, Dict[str, dict]], plots: Dict[str, List[Path]], out: Path):
    from scenarios import SCENARIOS

    rows = []
    for scenario in sorted(summary):
        for version in sorted(summary[scenario]):
            agg = summary[scenario][version]
            rows.append(
                f"<tr><td>{scenario}</td><td>{version}</td>"
                f"<td>{agg['makespan']['median']:.1f}</td>"
                f"<td>{agg['mean_flow_time']['median']:.1f}</td>"
                f"<td>{agg['oldest_job_flow']['median']:.1f}</td>"
                f"<td>{agg['max_tail_90']['median']:.1f}</td>"
                f"<td>{agg['max_tail_90_fraction']['median']:.2f}</td>"
                f"<td>{agg['max_decile']['median']:.1f}</td>"
                f"<td>{agg['concurrent_jobs']['median']:.2f}</td>"
                f"<td>{agg['order_inversions']['median']:.0f}</td>"
                f"<td>{'ok' if agg['sane'] else 'TASK COUNT MISMATCH'}</td></tr>"
            )

    # The plots live in the results directory, which is not necessarily where the
    # report is written, so reference them relative to the report itself.
    report_dir = out.resolve().parent
    sections = []
    for scenario in sorted(plots):
        desc = SCENARIOS[scenario].description if scenario in SCENARIOS else ""
        imgs = "\n".join(f'<img src="{os.path.relpath(p.resolve(), report_dir)}">' for p in plots[scenario])
        sections.append(f"<h2>{scenario} — {desc}</h2>\n{imgs}")

    out.write_text(
        f"""<!doctype html>
<meta charset="utf-8">
<title>HQ scheduler fairness: v0.25.1 vs v0.26.2</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 1200px; }}
table {{ border-collapse: collapse; margin: 1rem 0; }}
th, td {{ border: 1px solid #ccc; padding: 4px 10px; text-align: right; }}
th:first-child, td:first-child, td:nth-child(2) {{ text-align: left; }}
img {{ max-width: 100%; display: block; margin: 1rem 0; }}
</style>
<h1>HyperQueue scheduler fairness: v0.25.1 vs v0.26.2</h1>
<p>Medians over repetitions. <b>job1 flow</b> = submit-to-last-task of the oldest job.
<b>last 10%</b> = longest time any job needed to get from 90% to 100% complete -- the
reported symptom -- with <b>last 10% frac</b> the same as a share of the job's lifetime.
<b>slow decile</b> = longest time a job needed for any one 10% slice of its own tasks. <b>concurrent_jobs</b> = average number of jobs sharing the
cluster while it is busy (4 = full lockstep, 1 = jobs drained in order).
<b>makespan</b> is a control variable: if it differs a lot, the fairness comparison is
confounded by raw throughput.</p>
<table>
<tr><th>scenario</th><th>version</th><th>makespan (s)</th><th>mean flow (s)</th>
<th>job1 flow (s)</th><th>last 10% (s)</th><th>last 10% frac</th><th>slow decile (s)</th><th>concurrent_jobs</th>
<th>inversions</th><th>sanity</th></tr>
{chr(10).join(rows)}
</table>
{chr(10).join(sections)}
""",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path, nargs="?", default=HERE / "results")
    parser.add_argument("--scenario", action="append", dest="scenarios")
    parser.add_argument("--report", type=Path, help="write an HTML report here")
    parser.add_argument("--no-plots", action="store_true")
    args = parser.parse_args()

    runs: List[Run] = []
    for meta_path in sorted(args.results.glob("*/*/*/meta.json")):
        run_dir = meta_path.parent
        try:
            runs.append(load_run(run_dir))
        except Exception as e:  # noqa: BLE001
            print(f"skipping {run_dir}: {e}", file=sys.stderr)

    if args.scenarios:
        runs = [r for r in runs if r.scenario in args.scenarios]
    if not runs:
        print("no runs found", file=sys.stderr)
        return 1

    by_scenario: Dict[str, Dict[str, List[Run]]] = {}
    for run in runs:
        by_scenario.setdefault(run.scenario, {}).setdefault(run.version, []).append(run)

    summary: Dict[str, Dict[str, dict]] = {}
    print(
        f"{'scenario':<9}{'version':<11}{'makespan':>10}{'meanflow':>10}{'job1flow':>10}"
        f"{'tail90':>8}{'tail90%':>9}{'slowdec':>9}{'conc':>7}{'inv':>5}  order"
    )
    for scenario in sorted(by_scenario):
        summary[scenario] = {}
        for version in sorted(by_scenario[scenario]):
            metrics = [run_metrics(r) for r in by_scenario[scenario][version]]
            agg = aggregate(metrics)
            summary[scenario][version] = agg
            print(
                f"{scenario:<9}{version:<11}"
                f"{agg['makespan']['median']:>10.1f}"
                f"{agg['mean_flow_time']['median']:>10.1f}"
                f"{agg['oldest_job_flow']['median']:>10.1f}"
                f"{agg['max_tail_90']['median']:>8.1f}"
                f"{agg['max_tail_90_fraction']['median']:>9.2f}"
                f"{agg['max_decile']['median']:>9.1f}"
                f"{agg['concurrent_jobs']['median']:>7.2f}"
                f"{agg['order_inversions']['median']:>5.0f}"
                f"  {','.join(metrics[0]['completion_order'])}"
                f"{'' if agg['sane'] else '  !! TASK COUNT MISMATCH'}"
            )

    (args.results / "summary.json").write_text(json.dumps(summary, indent=2))

    if not args.no_plots:
        plots = {}
        for scenario in sorted(by_scenario):
            plots[scenario] = plot_scenario(by_scenario[scenario], scenario, args.results)
        if args.report:
            render_report(summary, plots, args.report)
            print(f"\nreport written to {args.report}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

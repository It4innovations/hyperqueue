#!/usr/bin/env python3
"""Gantt chart of a `W2` run under each scheduler: who got loaded inside their window.

The `W*` scenarios give a worker a $60$~s walltime and every task a $57$~s time request, so a
worker can be handed work only while its remaining lifetime still covers a request -- the first
three seconds of its life. After that it can only drain what it already holds. The chart makes
the consequence visible: the run ends when the *last* task finally gets a window, so what matters
is how full each worker was when its window shut, not how busy the cluster looked afterwards.

One row per worker, ordered by arrival. The pale band is the worker's lifetime, the dark tick at
its left edge is the three-second loading window, and each bar is one task on one of the worker's
eight cpus. The dashed rule marks the last task start; the run's makespan is that start plus the
task's own runtime.

Usage:
    python3 walltime_gantt.py                     # reads ./results/walltime, writes ./results/walltime
    python3 walltime_gantt.py --results DIR --out DIR --scenario W2 --rep rep1
"""

import argparse
import collections
import sys
from pathlib import Path

import altair as alt
import pandas as pd

import journal

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import viz_theme  # noqa: E402

#: The loading window: walltime minus the task time request (60 - 57).
WINDOW_S = 3.0
#: The four jobs, in submission order; they arrive one every 12 s.
JOBS = [f"job {i}" for i in range(1, 5)]

#: Arm label -> panel title. `head` is the scheduler of this paper.
ARMS = {"v0.25.1": "v0.25.1 (previous scheduler)", "head": "v0.27.0 (this paper)"}


def frames(results: Path, scenario: str, rep: str):
    """Per-arm task, worker and annotation frames, on a common clock that starts at 0."""
    tasks, workers, marks = [], [], []
    for arm, title in ARMS.items():
        events = results / arm / scenario / rep / "events.ndjson"
        run = journal.load_run(events)
        order = {w: i for i, w in enumerate(sorted(run.workers, key=lambda w: run.worker_connected[w]))}
        lanes = {}
        for task in sorted(run.tasks, key=lambda t: (t.worker, t.start, t.task)):
            lane = lanes.setdefault(task.worker, 0)
            lanes[task.worker] = lane + 1
            row = order[task.worker]
            cpus = run.workers[task.worker]
            tasks.append({
                "arm": title, "worker": row + 1, "start": task.start, "finish": task.finish,
                "job": f"job {task.job}",
                "y0": row + 0.08 + 0.84 * lane / cpus, "y1": row + 0.08 + 0.84 * (lane + 1) / cpus,
            })
        counts = collections.Counter(t.worker for t in run.tasks)
        for worker, row in order.items():
            conn = run.worker_connected[worker]
            lost = run.worker_lost.get(worker, (run.end, "up"))[0]
            n = counts.get(worker, 0)
            workers.append({"arm": title, "worker": row + 1, "conn": conn, "lost": lost,
                            "window_end": conn + WINDOW_S, "y0": row + 0.06, "y1": row + 0.94,
                            "n_tasks": n, "cpus": run.workers[worker],
                            "caught": f"{n}/{run.workers[worker]}" if n else ""})
        last_start = max(t.start for t in run.tasks)
        makespan = max(t.finish for t in run.tasks)
        marks.append({"arm": title, "last_start": last_start, "makespan": makespan,
                      "label": f"last start {last_start:.0f} s", "span": f"makespan {makespan:.0f} s"})
    return pd.DataFrame(tasks), pd.DataFrame(workers), pd.DataFrame(marks)


def panel(panel_w: int, title: str, tasks, workers, marks, n_workers: int, t_max: float, show_x: bool):
    """One arm: worker lifetimes, loading windows, task bars, and the last-start rule."""
    xscale = alt.Scale(domain=[0, t_max], nice=False)
    xaxis = alt.Axis(labels=show_x, ticks=show_x)
    xtitle = "time since run start (s)" if show_x else None
    yscale = alt.Scale(domain=[n_workers, 0], nice=False)
    y = alt.Y("y0:Q", title="worker (arrival order)",
              scale=yscale,
              axis=alt.Axis(values=[i + 0.5 for i in range(n_workers)],
                            labelExpr="format(floor(datum.value) + 1, 'd')", grid=False))
    life = alt.Chart(workers).mark_rect(color=viz_theme.GRID).encode(
        x=alt.X("conn:Q", scale=xscale, axis=xaxis, title=xtitle), x2="lost:Q", y=y, y2="y1:Q")
    window = alt.Chart(workers).mark_rect(color=viz_theme.INK_MUTED).encode(
        x=alt.X("conn:Q", scale=xscale, axis=xaxis, title=xtitle), x2="window_end:Q", y=y, y2="y1:Q")
    bars = alt.Chart(tasks).mark_rect().encode(
        x=alt.X("start:Q", scale=xscale, axis=xaxis, title=xtitle), x2="finish:Q", y=y, y2="y1:Q",
        color=alt.Color("job:N", title=None, sort=JOBS,
                        scale=alt.Scale(domain=JOBS, range=viz_theme.SERIES[:len(JOBS)]),
                        legend=None if show_x else alt.Legend(
                            orient="top", direction="horizontal", symbolType="square",
                            title=None, labelFontSize=8, offset=2)))
    rule = alt.Chart(marks).mark_rule(color=viz_theme.CRITICAL, strokeDash=[4, 3]).encode(
        x=alt.X("last_start:Q", scale=xscale, axis=xaxis, title=xtitle))
    label = alt.Chart(marks).mark_text(align="left", dx=4, dy=2, baseline="top",
                                       color=viz_theme.CRITICAL, fontSize=8).encode(
        x=alt.X("last_start:Q", scale=xscale, axis=xaxis, title=xtitle),
        y=alt.value(2), text="label:N")
    span = alt.Chart(marks).mark_rule(color=viz_theme.INK_SECONDARY, strokeDash=[2, 2]).encode(
        x=alt.X("makespan:Q", scale=xscale, axis=xaxis, title=xtitle))
    span_label = alt.Chart(marks).mark_text(align="right", dx=-4, dy=2, baseline="top",
                                            color=viz_theme.INK_SECONDARY, fontSize=8).encode(
        x=alt.X("makespan:Q", scale=xscale, axis=xaxis, title=xtitle),
        y=alt.value(2), text="span:N")
    caught = alt.Chart(workers).mark_text(align="left", dx=3, baseline="middle",
                                          color=viz_theme.INK_MUTED, fontSize=7).encode(
        x=alt.X("lost:Q", scale=xscale, axis=xaxis, title=xtitle),
        y=y, text="caught:N")
    return alt.layer(life, window, bars, rule, label, span, span_label, caught).properties(
        width=panel_w, height=viz_theme.PANEL_H,
        title=alt.Title(title, fontSize=9, anchor="start"))


def build(panel_w: int, tasks: pd.DataFrame, workers: pd.DataFrame, marks: pd.DataFrame):
    n_workers = int(workers["worker"].max())
    t_max = float(marks["makespan"].max()) + 4
    panels = []
    for i, title in enumerate(ARMS.values()):
        panels.append(panel(panel_w, title,
                            tasks[tasks["arm"] == title], workers[workers["arm"] == title],
                            marks[marks["arm"] == title], n_workers, t_max,
                            show_x=(i == len(ARMS) - 1)))
    return alt.vconcat(*panels, spacing=14).resolve_scale(x="shared", y="shared")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", type=Path, default=HERE / "results" / "walltime")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--scenario", default="W2")
    ap.add_argument("--rep", default="rep1")
    args = ap.parse_args()
    out = args.out or args.results
    out.mkdir(parents=True, exist_ok=True)
    tasks, workers, marks = frames(args.results, args.scenario, args.rep)
    viz_theme.fit_and_save(lambda w: build(w, tasks, workers, marks), out / f"walltime-gantt-{args.scenario.lower()}")
    for row in marks.itertuples():
        print(f"{row.arm}: last start {row.last_start:.1f}s, makespan {row.makespan:.1f}s")


if __name__ == "__main__":
    main()

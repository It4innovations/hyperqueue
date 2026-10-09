#!/usr/bin/env python3
"""How does the schedule improve while the solver keeps working?

Each panel is one benchmark cell: the same scheduling state solved under a growing MILP time
limit, three repetitions, median. Three measures share one percentage axis, because one of them
alone would mislead:

* **utilization** -- CPUs the round put to work, as a share of the CPUs that were free. What an
  operator sees, but blind to *where* the work went.
* **workers used** -- workers the round placed anything on. Compaction: the same CPUs on fewer
  workers is a better schedule, since idle workers can be released. Above 100 % means the work
  was spread wider than the full solve needs. Meaningful only where the ideal solution does not
  need every worker (the `_light` cell).
* **objective** -- the MILP objective, as a share of the best value any arm reached. The quantity
  the solver actually maximises; it moves when either of the other two does, and also when the
  solution merely packs better.

It also prints, per cell and time limit, the median solve time, the three measures and whether
every repetition proved the optimum: the numbers the paper's text quotes.

Usage:
    python3 plot_anytime.py --results results   # reads results/limits3, writes
                                                # results/anytime-utilization.{png,pdf}
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import altair as alt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import viz_theme as vt  # noqa: E402

HERE = Path(__file__).resolve().parent

CELLS = [
    ("pruning_g64", "pruning, G=64 (shipped)"),
    ("pruning_g256", "pruning, G=256"),
    ("wr_1000x64", "1000 workers × 64 requests"),
    ("ps_500x16", "500 workers, 16 levels"),
    ("ps_500x256", "500 workers, 256 levels"),
    ("ps_500x256_light", "500 workers, 256 levels, light load"),
]

SERIES = ["utilization", "workers used", "objective"]
PRODUCTION_LIMIT_S = 5


def load(limits: Path, stem: str) -> pd.DataFrame:
    rows = [r for r in csv.DictReader((limits / f"{stem}.csv").open())
            if r.get("sweep") and r["sweep"] != "sweep"]
    by = defaultdict(list)
    for r in rows:
        by[float(r["mip_time_limit_s"])].append(r)
    # Every measure is read against the longest solve, so one percentage axis carries all three.
    # For utilization and objective, higher is closer to the full solve. For workers used, 100 %
    # is the same spread as the full solve and *above* 100 % means the same work landed on more
    # workers -- worse compaction.
    med = lambda rs, f: statistics.median(f(r) for r in rs)  # noqa: E731
    ref = by[max(by)]
    full = {
        "utilization": med(ref, lambda r: float(r["cpus_assigned"])),
        "workers used": med(ref, lambda r: int(r["n_workers_used"])),
        "objective": med(ref, lambda r: float(r["objective"])),
    }
    # Stop at the first limit where every repetition proved the optimum: a longer limit
    # cannot change anything, so continuing the line would only repeat that point.
    proven = [limit for limit, rs in by.items() if all(r["is_optimal"] == "true" for r in rs)]
    last = min(proven, default=max(by))
    out = []
    for limit, rs in sorted(by.items()):
        if limit > last:
            break
        vals = {
            "utilization": med(rs, lambda r: float(r["cpus_assigned"])),
            "workers used": med(rs, lambda r: int(r["n_workers_used"])),
            "objective": med(rs, lambda r: float(r["objective"])),
        }
        for measure, v in vals.items():
            # x is the time the solve actually took, not the limit it was given: a proven
            # optimum ends before its limit, and a solve can also overrun a short limit.
            out.append({"limit": limit, "time": med(rs, lambda r: float(r["t_solve_us"]) / 1e6),
                        "measure": measure, "value": 100 * v / full[measure],
                        "optimal": all(r["is_optimal"] == "true" for r in rs)})
    return pd.DataFrame(out)


def panel(df, title, width, *, first, last_row, legend):
    x = alt.X(
        "time:Q",
        scale=alt.Scale(type="log", domain=[0.9, 150], nice=False),
        axis=alt.Axis(title="MILP solve time (s)" if last_row and first else None,
                      values=[1, 2, 5, 10, 30, 120], format="g"),
    )
    y = alt.Y("value:Q", scale=alt.Scale(domain=[0, 105], nice=False),
              axis=alt.Axis(title="% of the best solution" if first else None,
                            labels=first, grid=True, values=[0, 25, 50, 75, 100]))
    color = alt.Color("measure:N", scale=alt.Scale(domain=SERIES, range=vt.SERIES[:3]),
                      legend=alt.Legend(title=None, orient="top", direction="horizontal",
                                        offset=2) if legend else None)
    base = alt.Chart(df).encode(x=x, y=y, color=color, order="limit:Q",
                                strokeDash=vt.dash_scale("measure", SERIES, legend=None))
    rule = (alt.Chart(pd.DataFrame({"time": [PRODUCTION_LIMIT_S]}))
            .mark_rule(color=vt.AXIS, strokeDash=[3, 3], strokeWidth=0.8).encode(x=x))
    line = base.mark_line(strokeWidth=1.4)
    pts = base.mark_point(size=26, filled=True)
    layers = [rule, line, pts]
    # A line that never reached a proven optimum ends at the longest solve, whose 100 % is
    # only the best solution found. Ring that end point and say so in the panel.
    end = df[(df["limit"] == df["limit"].max()) & ~df["optimal"]]
    if not end.empty:
        end = end[end["measure"] == "utilization"]
        ring = (alt.Chart(end).mark_point(size=110, filled=False, color=vt.INK_SECONDARY, strokeWidth=1.2)
                .encode(x=x, y=y))
        # Below the lines, where the panel is empty, ending under the ringed point.
        note = (alt.Chart(end.assign(value=55.0, text="optimum not proven"))
                .mark_text(align="right", baseline="middle", fontSize=9, color=vt.INK_SECONDARY)
                .encode(x=x, y=y, text="text:N"))
        arrow = (alt.Chart(pd.concat([end.assign(value=62.0), end.assign(value=93.0)]))
                 .mark_line(color=vt.INK_SECONDARY, strokeWidth=0.8).encode(x=x, y=y))
        layers += [arrow, ring, note]
    return alt.layer(*layers).properties(width=width, height=108, title=title)


def print_table(frames: dict[str, pd.DataFrame]) -> None:
    for stem, title in CELLS:
        df = frames[stem]
        print(f"== {title}")
        print(f"  {'limit':>6} {'solve':>8} {'utilization':>12} {'workers used':>13} {'objective':>10}  optimal")
        for limit, rows in df.groupby("limit"):
            v = dict(zip(rows["measure"], rows["value"]))
            first = rows.iloc[0]
            print(f"  {limit:>5g}s {first['time']:>7.1f}s {v['utilization']:>11.1f}% "
                  f"{v['workers used']:>12.1f}% {v['objective']:>9.1f}%  {first['optimal']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=HERE / "results")
    args = parser.parse_args()

    frames = {stem: load(args.results / "limits3", stem) for stem, _ in CELLS}
    print_table(frames)

    def build(width: int):
        panels = [
            panel(frames[stem], title, width, first=(i % 3 == 0),
                  last_row=(i >= len(CELLS) - 3), legend=(i == 0))
            for i, (stem, title) in enumerate(CELLS)
        ]
        return vt.grid(panels, per_row=3, spacing=20, row_spacing=24)

    vt.fit_and_save(build, args.results / "anytime-utilization")


if __name__ == "__main__":
    main()

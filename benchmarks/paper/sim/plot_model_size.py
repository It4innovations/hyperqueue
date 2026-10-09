#!/usr/bin/env python3
"""The priority-levels sweep, plotted: how many variables and constraints the MILP has.

Two figures over the same data, which are alternatives rather than a pair:

`model-size*.pdf` -- **constraints alone**, against |W|, one line per priority-level count. Each
surviving cut emits a constraint per worker per blocker, and the cut count itself grows with |W|,
so this side is quadratic in the cluster size. Stripped of titles, to be captioned by the paper
rather than by the figure.

`model-size*-by-levels.pdf` -- **both sides**, against priority levels, one line per worker
count, on one shared log scale so they are directly comparable. The grey references are
`n_batches * |W|` placements, one flat line per cluster size that the variable series sit on
across a 256x sweep in levels, while the constraint panel climbs about two decades over the
identical axis.

Data: `priority_scaling_g64.csv` from `run_all.sh`, the shipped default G = 64. At G = 64 the
global budget caps surviving cuts, so the constraint fan is clipped and the 64- and 256-level
curves converge.

The paper's figure is `model-size-r0.pdf` (`--rounds zero`, the first round of each cell).

Usage:
    python3 plot_model_size.py --results results --rounds zero
"""

import argparse
import collections
import csv
import math
import statistics
import sys
from pathlib import Path

import altair as alt
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import viz_theme as vt  # noqa: E402


def load(path: Path):
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def cells(rows, which="steady"):
    """Medians per (|W|, levels), over the chosen rounds.

    `steady` drops round 0, which solves against an empty cluster: a cold start rather than the
    state a running scheduler sees, and what E1's other sweeps report. `zero` keeps only round 0,
    which is the *largest* model the sweep ever builds -- nothing is running, so every worker
    contributes a placement variable for every batch and no cut has yet been discharged. The paper
    quotes round 0, so a figure printed beside its tables has to be built the same way.
    """
    buckets = collections.defaultdict(list)
    for row in rows:
        rnd = int(row["round"])
        if which == "steady" and rnd == 0:
            continue
        if which == "zero" and rnd != 0:
            continue
        buckets[(int(row["n_workers"]), int(row["priority_levels"]))].append(row)
    out = []
    for (workers, levels), group in sorted(buckets.items()):
        med = lambda col: statistics.median(float(r[col]) for r in group)  # noqa: E731
        out.append(
            {
                "workers": workers,
                "levels": levels,
                "variables": med("n_variables"),
                "constraints": med("n_constraints"),
                "batches": med("n_batches"),
            }
        )
    return pd.DataFrame(out)


#: The two figures. `label` formats the series values; the colour-coded axis is named by the
#: legend title. `panels` selects which side(s) of the model are drawn, and `titles` switches the
#: figure and panel headings off for a figure the paper's own caption will introduce.
FIGURES = {
    "workers": {
        "x": "workers",
        "x_title": "workers |W|",
        # Not [10, 100, 1000]: a tick value outside the data stretches the scale domain to
        # reach it, and on a single wide panel that decade of dead space is a fifth of the
        # figure.
        "x_values": [10, 100, 500],
        "series": "levels",
        "label": str,
        "legend_title": "priority levels",
        "flat_note": "every level within {spread:.0%} of it",
        "panels": ("constraints",),
        "titles": False,
    },
    "levels": {
        "x": "levels",
        "x_title": "priority levels",
        "x_values": [1, 4, 16, 64, 256],
        "series": "workers",
        "label": str,
        # A legend rather than direct labels: under G = 64 the 100- and 250-worker constraint
        # curves converge at 256 levels and their end labels overprint. The legend's stroke
        # swatches carry the dash channel instead.
        "legend_title": "workers |W|",
        "flat_note": "flat across a 256x sweep in levels",
        "panels": ("variables", "constraints"),
        "titles": True,
    },
}


def panel(df, field, title, subtitle, panel_w, *, o, order, y_domain, y_values, reference=None):
    """One side of the model. `title` is None on a figure the paper captions itself."""
    x = alt.X(
        f"{o['x']}:Q",
        scale=alt.Scale(type="log"),
        # Explicit values, not `tickCount`: left to itself a log axis rules a line at every minor
        # tick, which reads as texture on screen and prints as a grey wash.
        axis=alt.Axis(title=o["x_title"], format="~s", values=o["x_values"]),
    )
    y = alt.Y(
        f"{field}:Q",
        # One domain for both panels, so the panels are directly comparable: the point is that the
        # constraint side overtakes the variable side, which separate scales would hide.
        scale=alt.Scale(type="log", domain=y_domain),
        axis=alt.Axis(title=field, format="~s", values=y_values),
    )
    legend = alt.Legend(
        title=o["legend_title"], orient="bottom", direction="horizontal", titleOrient="left",
        columns=len(order), symbolType="stroke", symbolStrokeWidth=1.6, symbolSize=200,
    )
    # Domain order pins each series to its palette slot, so the panels -- and the arms, and the two
    # orientations -- always colour the same series the same way.
    color = alt.Color(
        "series:N", scale=alt.Scale(domain=order, range=vt.SERIES[: len(order)]), legend=legend
    )
    base = alt.Chart(df).encode(x=x, y=y, color=color)
    # Third channel for the mono printer: adjacent palette slots are as little as 1.7 luminance
    # points apart, so in greyscale only the dash rhythm separates the series.
    marks = [
        base.encode(strokeDash=vt.dash_scale("series", order)).mark_line(
            point=alt.OverlayMarkDef(size=40, filled=True)
        )
    ]
    if reference is not None:
        # The formula's own term, drawn rather than asserted. Wide and pale, underneath: the series
        # sit exactly on it, so it can only show as a halo -- which is the finding.
        marks.insert(
            0,
            alt.Chart(reference)
            .mark_line(color=vt.GRID, strokeWidth=5)
            .encode(
                x=x,
                y=alt.Y(f"{field}:Q", scale=alt.Scale(type="log", domain=y_domain)),
                detail="workers:N",
            ),
        )
    chart = alt.layer(*marks).properties(width=panel_w, height=vt.PANEL_H)
    if title is None:
        return chart
    return chart.properties(
        title=alt.TitleParams(title, subtitle=subtitle, subtitleColor=vt.INK_MUTED)
    )


def figure(cell_df, out_stem, arm, o):
    df = cell_df.assign(series=cell_df[o["series"]].map(o["label"]))
    order = [o["label"](v) for v in sorted(cell_df[o["series"]].unique())]
    n_batches = int(statistics.median(df["batches"]))
    # `n_batches * |W|`: one placement variable per (worker, batch, variant), at one variant. Held
    # against every level, so on the levels axis it is one horizontal line per worker count and on
    # the workers axis the five copies coincide into a single diagonal.
    placements = df[["workers", "levels"]].assign(variables=lambda d: d["workers"] * n_batches)

    baseline = df[df["levels"] == df["levels"].min()]
    spread = df["variables"].max() / baseline["variables"].max() - 1.0

    # Over the fields actually drawn: two panels share one domain so they can be compared, while a
    # constraints-only figure has nothing to compare against and should fill its own decades.
    drawn = list(o["panels"])
    lo = min(df[f].min() for f in drawn)
    hi = max(df[f].max() for f in drawn)
    y_domain = [10 ** math.floor(math.log10(lo)), 10 ** math.ceil(math.log10(hi))]
    y_values = [10**e for e in range(int(math.log10(y_domain[0])), int(math.log10(y_domain[1])) + 1)]

    titles = {
        "variables": (
            "Variables: the term the formula has",
            f"grey: {n_batches}x|W| placements; " + o["flat_note"].format(spread=spread),
        ),
        "constraints": (
            "Constraints: the term it omits",
            "one per worker per blocker, per surviving cut",
        ),
    }

    def build(panel_w):
        common = dict(o=o, order=order, y_domain=y_domain, y_values=y_values)
        panels = [
            panel(
                df,
                field,
                *(titles[field] if o["titles"] else (None, None)),
                panel_w,
                reference=placements if field == "variables" else None,
                **common,
            )
            for field in drawn
        ]
        chart = (
            vt.grid(panels, per_row=2)
            .resolve_scale(color="shared", strokeDash="shared")
            .configure_view(strokeOpacity=0)
            .properties(
                background=vt.SURFACE,
                padding={"left": 8, "right": 10, "top": 6, "bottom": 6},
            )
        )
        if not o["titles"]:
            return chart
        return chart.properties(
            title=alt.TitleParams(
                "MILP size vs priority structure",
                subtitle=f"{arm}; steady-state medians, 64 cpus, 8 request types, 200k tasks",
                subtitleColor=vt.INK_MUTED,
            )
        )

    vt.fit_and_save(build, out_stem)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=HERE / "results")
    parser.add_argument(
        "--rounds",
        choices=("steady", "zero"),
        default="steady",
        help="steady (default) drops round 0, which solves against an empty cluster; "
        "zero keeps only it, which is what the paper's tables report -- written to a `-r0` stem",
    )
    args = parser.parse_args()

    arms = [("priority_scaling_g64.csv", "model-size", "shipped default, G = 64")]
    written = 0
    for name, stem, arm in arms:
        rows = load(args.results / name)
        if not rows:
            print(f"skipping {stem}: no {name} under {args.results}")
            continue
        df = cells(rows, args.rounds)
        rounds = "" if args.rounds == "steady" else "-r0"
        for key, o in FIGURES.items():
            suffix = "" if key == "workers" else f"-by-{key}"
            figure(df, args.results / f"{stem}{suffix}{rounds}", arm, o)
            written += 1
    if not written:
        print("no data; run run_all.sh first")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

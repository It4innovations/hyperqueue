#!/usr/bin/env python3
"""Plot the E3 prefill results produced by ``run.py --sweep-env HQ_SCHED_PREFILL_MAX=...``.

Two panels sized for a paper column, emitted as PNG **and** PDF:

Drawn with Altair. * left  -- throughput against prefill depth, one line per task duration. The knee moves with
           task length, which is the whole point: no single constant is right everywhere.
* right -- worker idle time against depth. This is the quantity `paper.tex` §9 actually claims
           to reduce ("the round-trip also leaves workers briefly idle"), and unlike throughput
           it is not dominated by process spawn on near-trivial tasks.

Both panels mark the shipped default of 40.

Usage:
    python3 plot_prefill.py                        # reads ./results/prefill
    python3 plot_prefill.py --results results/prefill --out results/prefill
    python3 plot_prefill.py --compact              # no legend, lower panels (the paper's version)
"""

import argparse
import collections
import statistics
import sys
from pathlib import Path

import altair as alt
import pandas as pd

import journal
import prefill

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import viz_theme as vt  # noqa: E402

#: Plot-area width per panel. Two panels plus their axes, legend and label overhang have to land
#: on `vt.TEXT_W_PT`; `vt.save()` prints the finished page width to check it.
PANEL_W = 100

#: Scenario id -> label on the duration axis. These are the points of the sweet-spot curve;
#: any other scenario in the tree (P3/P4/P5 probe different questions) is left out of the figure.
DURATION_SCENARIOS = {
    "P0": "`true`",
    "P1d005": "5 ms",
    "P1d02": "20 ms",
    "P1": "50 ms",
    "P1d2": "200 ms",
}

#: Panel height for `--compact`: about two thirds of `vt.PANEL_H`, which with the legend gone takes
#: the figure from 292 to 187 pt at the same width and font size.
COMPACT_PANEL_H = 130

#: The shipped `proactive_filling_max`.
DEFAULT_DEPTH = 40


def collect(results: Path):
    """-> {scenario: {depth: {metric: median over reps}}}"""
    runs = collections.defaultdict(list)
    for scenario, _version, _rep, events in journal.iter_runs(results):
        if scenario not in DURATION_SCENARIOS:
            continue
        summary = prefill.summarise(events)
        if summary["prefill"] == "default":
            # Only swept cells carry a depth; an untagged run cannot be placed on the x axis.
            continue
        runs[scenario].append((int(summary["prefill"]), summary))

    out = {}
    for scenario, entries in runs.items():
        by_depth = collections.defaultdict(list)
        for depth, summary in entries:
            by_depth[depth].append(summary)
        out[scenario] = {
            depth: {
                "rate": statistics.median(s["rate"] for s in group),
                "idle_total_s": statistics.median(s["idle_total_s"] for s in group),
                "n_gaps": statistics.median(s["n_gaps"] for s in group),
                "ok": all(s["ok"] for s in group),
            }
            for depth, group in sorted(by_depth.items())
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=HERE / "results" / "prefill")
    parser.add_argument("--out", type=Path, default=HERE / "results" / "prefill")
    parser.add_argument(
        "--compact",
        action="store_true",
        help="drop the legend (the left panel is directly labelled) and lower both panels",
    )
    args = parser.parse_args()
    panel_h = COMPACT_PANEL_H if args.compact else vt.PANEL_H

    data = collect(args.results)
    if not data:
        print(f"no prefill sweep data under {args.results}; run the sweep first")
        return 1

    bad = [(s, d) for s, depths in data.items() for d, v in depths.items() if not v["ok"]]
    if bad:
        # A cell that lost tasks is not a slower cell, it is a broken one; refuse to draw it as
        # if it were a measurement.
        print(f"WARNING: {len(bad)} cell(s) failed their task-count check: {bad}")

    # Shortest task first, so the series order matches the order the knees move in.
    order = [label for _, label in DURATION_SCENARIOS.items() if data.get(_)]
    records = []
    for scenario, label in DURATION_SCENARIOS.items():
        for depth, v in (data.get(scenario) or {}).items():
            records.append(
                {
                    # Depth 0 (prefill disabled) cannot sit on a log axis; it is drawn at a
                    # nominal 1 and the tick relabelled, so "off" stays visible as the leftmost
                    # point rather than vanishing.
                    "depth": depth if depth > 0 else 1,
                    "task length": label,
                    "rate": v["rate"],
                    "idle": v["idle_total_s"],
                }
            )
    df = pd.DataFrame(records)

    x = alt.X(
        "depth:Q",
        scale=alt.Scale(type="log"),
        axis=alt.Axis(
            title="prefill depth",
            values=[1, 4, 16, 40, 100, 400],
            # The nominal 1 is "prefill off", not a depth of one.
            labelExpr="datum.value == 1 ? 'off' : format(datum.value, 'd')",
        ),
    )
    color = alt.Color(
        "task length:N",
        scale=alt.Scale(domain=order, range=vt.SERIES[: len(order)]),
        # Below the panels: five series inside the plot area collided with the lines.
        # `symbolType="stroke"` turns the swatches into line segments, so the legend shows each
        # series' dash pattern -- the channel a reader falls back on once the page is monochrome.
        # With --compact the legend is dropped: the left panel is directly labelled, and the
        # right panel shares its colours and dash patterns.
        legend=None if args.compact else alt.Legend(
            # Wrapped to 3 columns: a single row of five entries is wider than the two panels
            # and was setting the figure's width all by itself.
            title="task length", orient="bottom", direction="horizontal", offset=6, columns=3,
            symbolType="stroke", symbolStrokeWidth=1.6, symbolSize=200,
        ),
    )

    def panel(field, axis_title, title, log_y, *, label_y, labelled, panel_w):
        y = alt.Y(
            f"{field}:Q",
            scale=alt.Scale(type="log") if log_y else alt.Scale(zero=True, domainMin=0),
            # Explicit decades on the log panel: the default draws a minor gridline per tick and
            # labels 3k/2k/300/200/30/20, which is texture on screen and a grey wash in print.
            axis=alt.Axis(
                title=axis_title, format="~s", values=[10, 100, 1000, 10000] if log_y else alt.Undefined
            ),
        )
        base = alt.Chart(df).encode(x=x, y=y, color=color)
        default_rule = (
            alt.Chart(pd.DataFrame({"depth": [DEFAULT_DEPTH]}))
            .mark_rule(strokeDash=[4, 3], color=vt.INK_MUTED, strokeWidth=1)
            .encode(x="depth:Q")
        )
        default_text = (
            alt.Chart(pd.DataFrame({"depth": [DEFAULT_DEPTH]}))
            .mark_text(
                align="left", dx=4, y=label_y, color=vt.INK_MUTED, fontSize=8,
                text=f"default ({DEFAULT_DEPTH})",
            )
            .encode(x="depth:Q")
        )
        # Dash rhythm as well as hue: adjacent slots here are ~2 luminance points apart, which
        # is nothing once the page goes through a mono printer.
        lines = base.encode(strokeDash=vt.dash_scale("task length", order)).mark_line(
            point=alt.OverlayMarkDef(size=32, filled=True)
        )
        marks = [default_rule, default_text, lines]
        if labelled:
            # Relief rule: three of these five slots sit below 3:1 on this surface, so identity
            # needs a channel besides hue. Only the left panel is labelled -- the right panel's
            # lines converge on zero, where five labels would pile up.
            marks.append(vt.direct_labels(base, "depth", field, "task length"))
        return alt.layer(*marks).properties(width=panel_w, height=panel_h, title=title)

    def build(panel_w):
        return (
            alt.hconcat(
                panel("rate", "tasks / s", "Task throughput", True,
                      label_y=panel_h - 6, labelled=True, panel_w=panel_w),
                panel("idle", "total worker idle (s)", "Worker idle time", False,
                      label_y=8, labelled=False, panel_w=panel_w),
                spacing=34,
            )
            .configure_view(strokeOpacity=0)
            .properties(background=vt.SURFACE,
                        padding={"left": 8, "right": 44, "top": 6, "bottom": 6})
        )

    vt.fit_and_save(build, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

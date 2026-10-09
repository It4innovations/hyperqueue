#!/usr/bin/env python3
"""Figures for the paper's comparison with other task runtimes (many-tasks/ and resource-mix/).

Two figures, each FIG_W pt wide:

* overhead  -- time per task (total time / N) against N, one line per tool
               (`many-tasks/results.csv`).
           Flat means linear scaling; Snakemake's line climbs.
* placement -- makespan relative to the LP lower bound per scenario
               (`resource-mix/results/metrics.csv`, written by `resource-mix/export_csv.py`):
               median column rising from the bound, min--max whisker; full text width.

Each tool keeps one colour in both figures.

Usage:
    (cd resource-mix && .venv/bin/python export_csv.py results)
    python3 plot_others.py      # -> figures/others-{overhead,placement}.{png,pdf}

Needs altair, pandas and vl-convert-python (../requirements.txt).
"""

import statistics
import sys
from pathlib import Path

import altair as alt
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import viz_theme as vt  # noqa: E402

PANEL_H = 170
#: Each figure is included at FIG_W / vt.TEXT_W_PT of \\linewidth, so 8pt text stays 8pt.
FIG_W = 260
#: The placement figure spans the full text width (vt.TEXT_W_PT) and is kept low.
PLACEMENT_H = 110

#: Tool -> (label, palette slot). Fixed slots so an entity has the same colour in both panels.
TOOLS = {
    "hq": ("HQ v0.27.0", 0),
    "hq-head": ("HQ v0.27.0", 0),
    "ray-proc": ("Ray", 1),
    "ray": ("Ray", 1),
    "dask-proc": ("Dask", 2),
    "dask-proc-timeout600": ("Dask", 2),
    "dask": ("Dask", 2),
    "parsl-proc": ("Parsl", 3),
    "snakemake": ("Snakemake", 4),
    "hq-v0.25.1": ("HQ v0.25.1", 5),
}
SCENARIOS = ["A-adv", "A-shuf", "B-gpu", "B-split60", "B-var", "B2-gpu", "B2-split60", "B2-var"]


#: Identical colour and shape legends merge into one, so the swatches show the shapes.
LEGEND = alt.Legend(title=None, orient="bottom", direction="horizontal", offset=4, columns=2, symbolSize=40)


def color(domain, legend=None):
    slots = {label: slot for label, slot in TOOLS.values()}
    return alt.Color("tool:N", scale=alt.Scale(domain=domain, range=[vt.SERIES[slots[d]] for d in domain]),
                     legend=legend)


def per_task_frame():
    df = pd.read_csv(HERE / "many-tasks" / "results.csv", header=None,
                     names=["tool", "n", "rep", "submit", "total", "status"])
    df = df[df.status == "ok"]
    # Dask at 10^6 only finished with raised comm timeouts; that run stands in for the default.
    df = df[~((df.tool == "dask-proc") & (df.n == 1_000_000))]
    rows = []
    for (tool, n), g in df.groupby(["tool", "n"]):
        rows.append({"tool": TOOLS[tool][0], "n": n, "ms": 1000 * statistics.median(g.total) / n})
    return pd.DataFrame(rows)


#: Scenario -> the scenario whose lower bound normalizes it. B-gpu, B-split60 and B-var are one
#: workload submitted three ways; the bound of B-var may pick any split of the flex tasks, so it
#: bounds all three. Dividing each by its own bound made B-gpu look better than B-split60.
BOUND_OF = {"A-adv": "A-adv", "A-shuf": "A-shuf",
            "B-gpu": "B-var", "B-split60": "B-var", "B-var": "B-var",
            "B2-gpu": "B2-var", "B2-split60": "B2-var", "B2-var": "B2-var"}


def ratio_frame():
    df = pd.read_csv(HERE / "resource-mix" / "results" / "metrics.csv")
    lb = df.groupby("scenario").lb.first()
    rows = []
    for (arm, sc), g in df.groupby(["arm", "scenario"]):
        ratio = g.makespan / lb[BOUND_OF[sc]]
        rows.append({"tool": TOOLS[arm][0], "scenario": sc, "med": statistics.median(ratio),
                     "lo": ratio.min(), "hi": ratio.max()})
    return pd.DataFrame(rows)


def main():
    pt = per_task_frame()
    rt = ratio_frame()
    left_dom = ["HQ v0.27.0", "Ray", "Dask", "Parsl", "Snakemake"]
    right_dom = ["HQ v0.27.0", "HQ v0.25.1", "Ray", "Dask"]

    # Hand-placed end labels: Ray and Parsl end 8 % apart, which collides on a log axis.
    last = pt.loc[pt.groupby("tool").n.idxmax()].copy()
    last["dy"] = last.tool.map({"Dask": -3, "Parsl": -1, "Ray": 7, "HQ v0.27.0": 2}).fillna(0)

    def left(w):
        x = alt.X("n:Q", scale=alt.Scale(type="log", domain=[100, 1_000_000]),
                  axis=alt.Axis(title="number of tasks", values=[100, 1000, 10000, 100000, 1000000],
                                labelExpr="'10' + ['⁰','¹','²','³','⁴','⁵','⁶'][round(log(datum.value)/log(10))]"))
        y = alt.Y("ms:Q", scale=alt.Scale(type="log", domain=[0.5, 300]),
                  axis=alt.Axis(title="total time / task (ms)", values=[1, 3, 10, 30, 100, 300]))
        base = alt.Chart(pt).encode(x=x, y=y, color=color(left_dom))
        lines = base.encode(strokeDash=vt.dash_scale("tool", left_dom)).mark_line(
            point=alt.OverlayMarkDef(size=26, filled=True))
        labels = [alt.Chart(last[last.tool == t]).mark_text(align="left", dx=5, dy=dy, fontSize=8,
                                                            fontWeight="bold")
                  .encode(x=x, y=y, color=color(left_dom), text="tool:N")
                  for t, dy in zip(last.tool, last.dy)]
        return alt.layer(lines, *labels).properties(width=w, height=PANEL_H,
                                                   title="Many trivial tasks (2 workers)")

    def right(w):
        # Scenarios along x, one column per tool rising from the bound (1.0), min--max whisker.
        # Wide gaps between scenario groups keep neighbouring groups apart; a tool keeps its slot
        # even where it has no data (Ray and Dask on the variant scenarios).
        x = alt.X("scenario:N", sort=SCENARIOS, scale=alt.Scale(paddingInner=0.3, paddingOuter=0.15),
                  axis=alt.Axis(title=None, labelAngle=0, ticks=False, domain=False))
        off = alt.XOffset("tool:N", scale=alt.Scale(domain=right_dom, paddingInner=0.12))
        yscale = alt.Scale(domain=[1, 3.1], zero=False, nice=False)
        yaxis = alt.Axis(title="makespan / lower bound", values=[1, 1.5, 2, 2.5, 3], format=".1f", grid=True)
        legend = alt.Legend(title=None, orient="top", direction="horizontal", offset=4, columns=4,
                            symbolType="square", symbolSize=60)
        bars = alt.Chart(rt).mark_bar().encode(
            x=x, xOffset=off, y=alt.Y("med:Q", scale=yscale, axis=yaxis), y2=alt.datum(1),
            color=color(right_dom, legend=legend))
        whisker = alt.Chart(rt).mark_rule(color=vt.INK, strokeWidth=0.8).encode(
            x=x, xOffset=off, y=alt.Y("lo:Q", scale=yscale, axis=yaxis), y2="hi:Q")
        return alt.layer(bars, whisker).properties(width=w, height=PLACEMENT_H,
                                                   title="CPU+GPU placement (7 workers)")

    def finish(chart, right_pad):
        return (chart.configure_view(strokeOpacity=0)
                .properties(background=vt.SURFACE,
                            padding={"left": 6, "right": right_pad, "top": 6, "bottom": 6}))

    # Two figures, one per paper subsection; the caption names each, so no panel titles.
    (HERE / "figures").mkdir(exist_ok=True)
    vt.fit_and_save(lambda w: finish(left(w).properties(title=""), 46),
                    HERE / "figures" / "others-overhead", target=FIG_W)
    vt.fit_and_save(lambda w: finish(right(w).properties(title=""), 10),
                    HERE / "figures" / "others-placement", target=vt.TEXT_W_PT)

if __name__ == "__main__":
    main()

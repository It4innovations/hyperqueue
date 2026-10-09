#!/usr/bin/env python3
"""Read HiGHS MIP logs and report how solution quality develops over time.

A solve that hits the production cut-off tells us nothing by itself: the question is whether the
solution at the cut-off was already good and the remaining time went into *proving* it. HiGHS
prints one line per improved solution with a timestamp and both bounds, which answers that.

    python3 anytime.py results/miplogs/*.log --cutoff 5
"""
import argparse
import re
from pathlib import Path

# HiGHS MIP log rows end with: ... <primal bound> <dual bound> <gap%> ... <time>s
ROW = re.compile(
    r"^\s*\S*\s+\d+\s+\d+\s+\d+\s+[\d.]+%\s+"
    r"(?P<dual>[-\d.e+]+)\s+(?P<primal>[-\d.e+]+|inf)\s+"
    r"(?P<gap>[\d.]+%|Large|inf)\s+.*?(?P<time>[\d.]+)s\s*$"
)

def parse(path):
    """-> list of (time_s, primal, dual, gap_percent_or_None)"""
    out = []
    for line in Path(path).read_text(errors="replace").splitlines():
        m = ROW.match(line)
        if not m:
            continue
        primal = m.group("primal")
        if primal in ("inf", "-inf"):
            continue
        gap = m.group("gap")
        gap = None if gap in ("Large", "inf") else float(gap.rstrip("%"))
        out.append((float(m.group("time")), float(primal), float(m.group("dual")), gap))
    return out

def summarise(path, cutoff):
    rows = parse(path)
    if not rows:
        return None
    best = max(r[1] for r in rows)
    first = rows[0]
    at_cutoff = [r for r in rows if r[0] <= cutoff]
    cut = at_cutoff[-1] if at_cutoff else None
    def time_to(frac):
        for t, primal, _d, _g in rows:
            if primal >= frac * best:
                return t
        return None
    return {
        "file": Path(path).name,
        "incumbents": len(rows),
        "t_first": first[0],
        "t_99": time_to(0.99),
        "t_999": time_to(0.999),
        "t_best": next(t for t, p, _d, _g in rows if p >= best),
        "obj_at_cutoff": None if cut is None else cut[1] / best,
        "gap_at_cutoff": None if cut is None else cut[3],
        "gap_final": rows[-1][3],
        "t_last": rows[-1][0],
    }

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("logs", nargs="+")
    ap.add_argument("--cutoff", type=float, default=5.0, help="production time limit, seconds")
    args = ap.parse_args()
    fmt = "{file:<44} {incumbents:>5} {t_first:>8} {t_99:>8} {t_999:>8} {t_best:>8} {obj:>9} {gapc:>9} {gapf:>8}"
    print(fmt.format(file="log", incumbents="inc", t_first="first", t_99="t@99%",
                     t_999="t@99.9%", t_best="t@best", obj=f"obj@{args.cutoff:g}s",
                     gapc=f"gap@{args.cutoff:g}s", gapf="gap end"))
    for path in args.logs:
        s = summarise(path, args.cutoff)
        if s is None:
            print(f"{Path(path).name:<44} (no solution rows parsed)")
            continue
        def f(x, suffix="s"):
            return "-" if x is None else f"{x:.2f}{suffix}"
        print(fmt.format(file=s["file"], incumbents=s["incumbents"], t_first=f(s["t_first"]),
                         t_99=f(s["t_99"]), t_999=f(s["t_999"]), t_best=f(s["t_best"]),
                         obj="-" if s["obj_at_cutoff"] is None else f"{100 * s['obj_at_cutoff']:.2f}%",
                         gapc=f(s["gap_at_cutoff"], "%"), gapf=f(s["gap_final"], "%")))

if __name__ == "__main__":
    main()

"""Print markdown tables (medians over repetitions) from results.csv."""

import csv
import statistics
import sys
from collections import defaultdict

TOOLS = ["hq", "ray-proc", "dask-proc", "parsl-proc", "snakemake"]
NAMES = {"hq": "HQ", "ray-proc": "Ray", "dask-proc": "Dask", "parsl-proc": "Parsl",
         "snakemake": "Snakemake"}
NS = [100, 1000, 10000, 100000, 1000000]
EXP = {100: "10²", 1000: "10³", 10000: "10⁴", 100000: "10⁵", 1000000: "10⁶"}

rows = defaultdict(list)
fails = defaultdict(list)
for tool, n, rep, sub, tot, status in csv.reader(open(sys.argv[1])):
    key = (tool, int(n))
    if status == "ok":
        rows[key].append((float(sub), float(tot)))
    else:
        fails[key].append(status)


def fmt(x):
    if x < 10:
        return f"{x:.2f}"
    if x < 100:
        return f"{x:.1f}"
    return f"{x:.0f}"


def cell(tool, n, idx):
    vals = [r[idx] for r in rows[(tool, n)]]
    if vals:
        med = fmt(statistics.median(vals))
        if len(vals) > 1:
            return f"{med} ({fmt(min(vals))}–{fmt(max(vals))})"
        return f"{med} (1 run)"
    if fails[(tool, n)]:
        return "failed: " + ", ".join(fails[(tool, n)])
    return "not run"


for title, idx in (("Total time [s]", 1), ("Submit time [s]", 0)):
    print(f"### {title}: median (min–max)\n")
    print("| N | " + " | ".join(NAMES[t] for t in TOOLS) + " |")
    print("|---" * (len(TOOLS) + 1) + "|")
    for n in NS:
        print(f"| {EXP[n]} | " + " | ".join(cell(t, n, idx) for t in TOOLS) + " |")
    print()

extra = sorted({k for k in list(rows) + list(fails) if k[0] not in TOOLS})
if extra:
    print("### Other runs\n")
    for tool, n in extra:
        print(f"- {tool} N={n}: total {cell(tool, n, 1)}, submit {cell(tool, n, 0)}")

"""Write per-run metrics of results/ to a flat CSV (input of ../plot_others.py).

usage: export_csv.py [RESULTS_DIR] [OUT_CSV]
"""

import csv
import glob
import os
import sys

from analyze import metrics


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "results"
    out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(root, "metrics.csv")
    lb_cache = {}
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["arm", "scenario", "rep", "makespan", "lb", "ratio", "gpu_util", "gpu_idle_waiting"])
        for path in sorted(glob.glob(os.path.join(root, "*", "*", "rep*.json"))):
            m = metrics(path, lb_cache)
            rep = os.path.basename(path)[3:-5]
            w.writerow([m["arm"], m["scenario"], rep, f"{m['makespan']:.3f}", f"{m['lb']:.3f}",
                        f"{m['ratio']:.4f}", f"{m['gpu_util']:.4f}", f"{m['gpu_idle_waiting']:.2f}"])
    print(f"wrote {out}")


if __name__ == "__main__":
    main()

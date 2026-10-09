#!/bin/bash
# Part 1 of the comparison with other task runtimes: N trivial tasks (each starts `true`)
# on 2 single-CPU workers in HQ, Ray, Dask, Parsl and Snakemake. Writes results.csv and
# logs/ next to this script. Takes about 4–5 hours (N = 10^6 and Snakemake 10^4 dominate).
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE"

(cd ../../e2e && python3 fetch_binaries.py --build-local)
[ -x .venv/bin/python ] || { python3 -m venv .venv && .venv/bin/pip install -r requirements.txt; }

.venv/bin/python -u driver.py
python3 summarize.py results.csv

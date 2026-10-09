#!/bin/bash
# Part 2 of the comparison with other task runtimes: heterogeneous CPU/GPU requests and
# resource variants, emulated on one machine (HQ head, HQ v0.25.1, Ray, Dask). Writes
# results/ and analysis.md next to this script. Takes about 1.5 hours.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE"

(cd ../../e2e && python3 fetch_binaries.py --build-local)
[ -x .venv/bin/python ] || { python3 -m venv .venv && .venv/bin/pip install -r requirements.txt; }

.venv/bin/python -u driver.py 3
.venv/bin/python analyze.py results > analysis.md
cat analysis.md

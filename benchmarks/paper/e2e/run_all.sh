#!/bin/bash
# Every end-to-end run behind a number, table or figure of the paper, on the two arms it compares:
# v0.25.1 (the release tarball) and head (this branch, built by fetch_binaries.py). Results land
# in ./results next to this script (or in $RESULTS). See ../README.md for what each produces.
#
# The workers are local processes, so the host should be otherwise idle; the prefill scenarios
# run real short tasks and are the most sensitive to other load. A run takes about two hours.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${RESULTS:-$HERE/results}
mkdir -p "$OUT"
cd "$HERE"

python3 fetch_binaries.py --build-local

settle() {
  for _ in $(seq 60); do
    awk -v l="$(cut -d' ' -f1 /proc/loadavg)" 'BEGIN { exit !(l < 1.0) }' && return
    sleep 5
  done
}
go() {
  settle
  echo "[$(date +%T)] $*"
  python3 run.py "$@"
}

# Comparison with the previous scheduler: job tails, narrow and wide jobs, and compaction.
go --version v0.25.1 --version head --scenario S3 --scenario S4 --scenario S6 --scenario S7 \
    --scenario S5 --scenario C3 --scenario C5 --reps 3 --results "$OUT"
# Weights exist only in the new scheduler.
go --version head --scenario S5W --reps 3 --results "$OUT"
# Node-hours, five repetitions.
go --version v0.25.1 --version head --scenario N1 --reps 5 --results "$OUT"
# Worker walltimes and time requests.
go --version v0.25.1 --version head --scenario W2 --reps 3 --results "$OUT/walltime"
# Prefilling, on the new scheduler only: every depth of a repetition runs back to back, so the
# cells of one sweep share the machine's power and thermal state.
go --version head --scenario P0 --scenario P1d005 --scenario P1d02 --scenario P1 --scenario P1d2 \
    --scenario P3 --scenario P4 --scenario P5 --reps 3 \
    --sweep-env HQ_SCHED_PREFILL_MAX=0,4,16,40,100,400 --results "$OUT/prefill"

echo "[$(date +%T)] done; now run the analysis scripts listed in ../README.md"

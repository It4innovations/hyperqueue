#!/bin/bash
# Every sched_sim sweep behind a number, table or figure of the paper. Run from anywhere; results
# land in ./results next to this script (or in $RESULTS). See ../README.md for what each produces.
#
# The microbenchmarks raise the MILP time limit to 120 s, so large instances report their true
# cost; only the limits3 cells vary it. A run takes several hours, dominated by the 500-worker
# priority-scaling cells and the limits3 repetitions.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
OUT=${RESULTS:-$HERE/results}
mkdir -p "$OUT/limits3" "$OUT/miplogs"

cargo build --release --manifest-path "$ROOT/Cargo.toml" -p tako --features sim --bin sched_sim
BIN=$ROOT/target/release/sched_sim

# Wall-clock numbers are only comparable on a quiet machine: wait for a low load before each sweep.
settle() {
  for _ in $(seq 60); do
    awk -v l="$(cut -d' ' -f1 /proc/loadavg)" 'BEGIN { exit !(l < 1.0) }' && return
    sleep 5
  done
}
run() {
  name=$1; shift
  settle
  echo "[$(date +%T)] $name"
  "$BIN" "$@" > "$OUT/$name.csv"
}

# MILP size and solve time: the three tables of "MILP size and solve time" and model-size.pdf.
run task_independence --sweep task_independence --workers 100 --cpus 16 --request-types 8 \
    --tasks 100000,300000,1000000,3000000,10000000 --rounds 5 --mip-time-limit 120
run workers_requests --sweep workers_requests --workers 10,50,100,250,500,1000 --cpus 64 \
    --request-types 1,4,16,64 --tasks 200000 --rounds 3 --mip-time-limit 120
run priority_scaling_g64 --sweep priority_scaling --workers 10,50,100,250,500 --cpus 64 \
    --request-types 8 --priority-levels 1,4,16,64,256 --tasks 200000 --rounds 3 --prune-g 64 \
    --mip-time-limit 120

# Pruning table: three repetitions, which dispatch and misplace the same tasks.
settle
echo "[$(date +%T)] pruning_g"
for rep in 1 2 3; do
  "$BIN" --sweep pruning --workers 100 --cpus 32 --request-types 4 --priority-levels 800 \
      --tasks 3200 --rounds 3 --prefill-max 0 --prune-g 16,32,64,128,256,1000000 --prune-f 4 \
      --check-pruning --mip-time-limit 120 $([ $rep = 1 ] || echo --no-header)
done > "$OUT/pruning_g.csv"

# The pathology of the reservation section: strict rule, gap filling, reservations.
run pathology --sweep pathology --scenario reservation --workers 10 --cpus 8 --occupied-cpus 4 \
    --tasks 100 --rounds 1 --relaxations strict,strict+gaps,all

# Quality of a truncated solve (anytime-utilization.pdf): the first round of six states, solved
# under growing time limits, three repetitions each.
limits_cell() {
  name=$1; shift
  settle
  echo "[$(date +%T)] limits3/$name"
  header=""
  for rep in 1 2 3; do
    for limit in 1 2 5 10 30 120; do
      "$BIN" "$@" --rounds 1 --mip-time-limit $limit $header
      header=--no-header
    done
  done > "$OUT/limits3/$name.csv"
}
limits_cell pruning_g64 --sweep pruning --workers 100 --cpus 32 --request-types 4 \
    --priority-levels 800 --tasks 3200 --prefill-max 0 --prune-g 64 --prune-f 4
limits_cell pruning_g256 --sweep pruning --workers 100 --cpus 32 --request-types 4 \
    --priority-levels 800 --tasks 3200 --prefill-max 0 --prune-g 256 --prune-f 4
limits_cell wr_1000x64 --sweep workers_requests --workers 1000 --cpus 64 --request-types 64 \
    --tasks 200000
limits_cell ps_500x16 --sweep priority_scaling --workers 500 --cpus 64 --request-types 8 \
    --priority-levels 16 --tasks 200000 --prune-g 64
limits_cell ps_500x256 --sweep priority_scaling --workers 500 --cpus 64 --request-types 8 \
    --priority-levels 256 --tasks 200000 --prune-g 64
limits_cell ps_500x256_light --sweep priority_scaling --workers 500 --cpus 64 --request-types 8 \
    --priority-levels 256 --tasks 5000 --prune-g 64

# The HiGHS log of the cell that never proves its optimum, read by anytime.py: the objective at the
# 5 s limit and the gap left at 120 s.
run miplogs/priority_scaling --sweep priority_scaling --workers 500 --cpus 64 --request-types 8 \
    --priority-levels 256 --tasks 200000 --prune-g 64 --rounds 1 --mip-time-limit 120 \
    --mip-log-dir "$OUT/miplogs"

# The sampling schedule (supplementary material): four workloads x five shapes x three budgets x
# two queue lengths (matching the cluster's 3200 cpus, and four times longer), three rounds each.
run sampling --sweep sampling --scenario band-all --workers 100 --cpus 32 --request-types 4 \
    --priority-levels 800 --tasks 3200,12800 --rounds 3 --prefill-max 0 --prune-g 32,64,128 \
    --prune-f 4 --prune-schedule head,linear,quadratic,exponential,random --check-pruning \
    --mip-time-limit 120
for band in head middle tail; do
  settle
  echo "[$(date +%T)] sampling band-$band"
  "$BIN" --sweep sampling --scenario band-$band --workers 100 --cpus 32 --request-types 4 \
      --priority-levels 800 --tasks 3200,12800 --rounds 3 --prefill-max 0 --prune-g 32,64,128 \
      --prune-f 4 --prune-schedule head,linear,quadratic,exponential,random --check-pruning \
      --mip-time-limit 120 --no-header >> "$OUT/sampling.csv"
done

echo "[$(date +%T)] done; now run tables.py, sampling.py, anytime.py and the plot scripts"

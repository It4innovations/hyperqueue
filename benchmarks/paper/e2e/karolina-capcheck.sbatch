#!/bin/bash -l
#SBATCH --job-name=hq-capcheck
#SBATCH --partition=qcpu
#SBATCH --nodes=9
#SBATCH --time=00:06:00
#SBATCH --output=results/karolina/capcheck-%j.out
# Is the ~1750 tasks/s cap of deep prefill in Part A per worker or in the server?
# P0 (4 workers) and P0W8 (8 workers), one worker per node, depth 0 and 100, 2 reps.
# Submit from benchmarks/paper/e2e: mkdir -p results/karolina && sbatch -A <project> karolina-capcheck.sbatch
set -eu
ml load Python/3.13.5-GCCcore-14.3.0
cd "$SLURM_SUBMIT_DIR"

me=$(hostname -s)
workers=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | grep -vx "$me" | paste -sd,)
echo "server on $me, workers on $workers"

export HQ_BENCH_WORKER_NODES=$workers
export HQ_BENCH_COMMIT=4bb6ff1e4+karolina-harness
OUT=results/karolina/capcheck-$SLURM_JOB_ID
timeout 300 python3 run.py --version head --scenario P0 --scenario P0W8 --reps 2 \
    --sweep-env HQ_SCHED_PREFILL_MAX=0,100 --results "$OUT"
python3 prefill.py --results "$OUT"
echo done

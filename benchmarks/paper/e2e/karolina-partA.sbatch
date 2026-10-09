#!/bin/bash -l
#SBATCH --job-name=hq-prefill-A
#SBATCH --partition=qcpu
#SBATCH --nodes=5
#SBATCH --time=00:50:00
#SBATCH --output=results/karolina/partA-%j.out
# Part A: the paper's prefill sweep (laptop shape, 4 workers x 4 cpus) with one worker per node.
# The server runs alone on the batch node.
# Submit from benchmarks/paper/e2e: mkdir -p results/karolina && sbatch -A <project> karolina-partA.sbatch
set -eu
ml load Python/3.13.5-GCCcore-14.3.0
cd "$SLURM_SUBMIT_DIR"

me=$(hostname -s)
workers=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | grep -vx "$me" | paste -sd,)
echo "server on $me, workers on $workers"
srun --overlap -N4 -n4 --ntasks-per-node=1 --nodelist="$workers" bash -c 'echo "$(hostname -s) $(nproc) load $(cut -d" " -f1 /proc/loadavg)"'

export HQ_BENCH_WORKER_NODES=$workers
export HQ_BENCH_COMMIT=4bb6ff1e4+karolina-harness
OUT=results/karolina/partA-$SLURM_JOB_ID
timeout 2700 python3 run.py --version head \
    --scenario P0 --scenario P1d005 --scenario P1d02 --scenario P1 --scenario P1d2 --reps 3 \
    --sweep-env HQ_SCHED_PREFILL_MAX=0,4,16,40,100,400 --results "$OUT"
python3 prefill.py --results "$OUT"
echo done

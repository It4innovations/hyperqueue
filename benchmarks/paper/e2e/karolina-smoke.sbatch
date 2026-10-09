#!/bin/bash -l
#SBATCH --job-name=hq-smoke
#SBATCH --partition=qcpu_exp
#SBATCH --nodes=2
#SBATCH --time=00:10:00
#SBATCH --output=results/karolina/smoke-%j.out
# Smoke test: server on the batch node, 4 workers x 4 cpus on the other node, P1 at depth 0 and 40.
# Submit from benchmarks/paper/e2e: mkdir -p results/karolina && sbatch -A <project> karolina-smoke.sbatch
set -eu
ml load Python/3.13.5-GCCcore-14.3.0
cd "$SLURM_SUBMIT_DIR"

me=$(hostname -s)
workers=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | grep -vx "$me" | paste -sd,)
echo "server on $me, workers on $workers"
srun --overlap -N1 -n1 --nodelist="$workers" bash -c 'hostname; nproc; ls -la "$SLURM_SUBMIT_DIR/bin/hq-head"'

export HQ_BENCH_WORKER_NODES=$workers
export HQ_BENCH_COMMIT=4bb6ff1e4+karolina-harness
timeout 420 python3 run.py --version head --scenario P1 --reps 1 \
    --sweep-env HQ_SCHED_PREFILL_MAX=0,40 --results results/karolina/smoke
python3 prefill.py --results results/karolina/smoke
echo done

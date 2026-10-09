#!/bin/bash -l
#SBATCH --job-name=hq-xalt
#SBATCH --partition=qcpu_exp
#SBATCH --nodes=2
#SBATCH --time=00:10:00
#SBATCH --output=results/karolina/xalt-%j.out
# Is the ceiling of deep prefill in Part A (~1700 tasks/s vs 5291 on the laptop) the XALT
# LD_PRELOAD that wraps every task process? Spawn cost on a compute node, then P0 at depth 0 and
# 100 with and without XALT. 4 workers x 4 cpus all on the second node.
# Submit from benchmarks/paper/e2e: mkdir -p results/karolina && sbatch -A <project> karolina-xalt.sbatch
set -eu
ml load Python/3.13.5-GCCcore-14.3.0
cd "$SLURM_SUBMIT_DIR"

me=$(hostname -s)
workers=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | grep -vx "$me" | paste -sd,)
echo "server on $me, workers on $workers"
srun --overlap -N1 -n1 --nodelist="$workers" bash -c '
  cd /tmp
  t0=$(date +%s%N); for i in $(seq 1000); do /bin/true; done
  t1=$(date +%s%N); for i in $(seq 1000); do LD_PRELOAD= /bin/true; done
  t2=$(date +%s%N); for i in $(seq 1000); do /bin/sleep 0; done
  t3=$(date +%s%N); for i in $(seq 1000); do LD_PRELOAD= /bin/sleep 0; done
  t4=$(date +%s%N)
  echo "spawn us/exec: true+xalt $(( (t1-t0)/1000000 )) true $(( (t2-t1)/1000000 )) sleep0+xalt $(( (t3-t2)/1000000 )) sleep0 $(( (t4-t3)/1000000 ))"'

export HQ_BENCH_WORKER_NODES=$workers
export HQ_BENCH_COMMIT=4bb6ff1e4+karolina-harness
OUT=results/karolina/xalt-$SLURM_JOB_ID
timeout 200 python3 run.py --version head --scenario P0 --reps 2 \
    --sweep-env HQ_SCHED_PREFILL_MAX=0,100 --results "$OUT/xalt"
env -u LD_PRELOAD timeout 200 python3 run.py --version head --scenario P0 --reps 2 \
    --sweep-env HQ_SCHED_PREFILL_MAX=0,100 --results "$OUT/noxalt"
echo "== with XALT";    python3 prefill.py --results "$OUT/xalt"
echo "== without XALT"; python3 prefill.py --results "$OUT/noxalt"
echo done

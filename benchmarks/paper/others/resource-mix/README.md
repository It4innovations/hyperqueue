# Heterogeneous resources: HQ vs. HQ v0.25.1 vs. Ray vs. Dask

Emulation on one machine: GPUs are plain counters and each task only sleeps,
so the benchmark measures **placement decisions**, not compute speed.
All workers run on the local machine.

## Cluster (`workload.NODE_TYPES`)

| type | count | CPUs | GPUs |
|---|---|---|---|
| gpu | 2 | 16 | 4 |
| dense | 1 | 8 | 8 |
| cpu | 4 | 32 | 0 |

Total 168 CPUs, 16 GPUs. Workers with GPUs are started first in every tool.

## Task classes (`workload.CLASSES`, full-scale durations)

| class | request | duration | count in A | count in B |
|---|---|---|---|---|
| small-cpu | 1 CPU | 20 s | 500 | 500 |
| big-cpu | 16 CPU | 60 s | 20 | 20 |
| node-wide | 32 CPU (fits only cpu nodes) | 60 s | 4 | — |
| gpu-mixed | 4 CPU + 1 GPU | 20 s | 120 | — |
| gpu-heavy | 1 CPU + 2 GPU | 30 s | 40 | — |
| flex | variant 0: 2 CPU + 1 GPU, 20 s; variant 1: 4 CPU, 60 s | | — | 400 |

Durations get ±20 % seeded jitter (identical for all tools and repetitions) and
are divided by `DIV = 16`, so a run takes roughly 20–50 s.

## Scenarios

- `A-adv`: fixed requests, submitted by class with small-cpu first (a greedy
  scheduler fills the CPUs of GPU nodes with small tasks).
- `A-shuf`: the same tasks in random order.
- `B-var`: flex tasks with both variants (HQ only; Ray and Dask have no variants).
- `B-gpu`: flex tasks fixed to the GPU variant (what a user would naturally submit).
- `B-split60`: 60 % of flex tasks fixed to GPU, 40 % to CPU. 60 % is the best
  10 %-grid split according to the LP bound (`workload.best_split`), i.e. the
  best static split chosen with hindsight.
- `B2-*`: like `B-*`, but the two `gpu` nodes join only at 150 s (full scale).

## Metrics (`analyze.py`)

- makespan (from submission to the end of the last task) and makespan / lower bound;
  the lower bound is an LP relaxation over (task, variant, node type) with
  per-type resource-time capacity (`workload.lower_bound`);
- GPU and CPU utilization;
- GPU-seconds idle while a task that could use a GPU is still waiting;
- finish time of each task class;
- share of flex tasks run with the GPU variant (variant scenarios).

Each task records start, end, worker and chosen variant itself (`task.sh` for
HQ, return value for Ray and Dask), so all tools are measured the same way.

## Tools

- **HQ HEAD**: `../../e2e/bin/hq-head` (`--profile dist` build of this branch, made by
  `../../e2e/fetch_binaries.py --build-local`).
  Job file with one `[[task]]` per task and one `[[task.request]]` per variant.
- **HQ v0.25.1**: `../../e2e/bin/hq-v0.25.1` (official release tarball, same script).
  **Workaround:** v0.25.1 panics (`workerload.rs:175`, index out of bounds) in the
  variant scenarios: when no variant fits right now it picks a variant round-robin
  and may account the GPU variant on a CPU-only worker whose resource vector has
  no slot for `gpus`. CPU-only workers therefore get an unused resource
  `pad=sum(1)` registered after `gpus`; this is applied to both HQ arms.
- **Ray 2.59.0**: `ray.cluster_utils.Cluster` with one fake node per worker
  (head node with 0 CPUs). Before measuring, every CPU slot is filled once so
  Ray starts its worker processes outside the measurement. Peak memory about 27 GB.
- **Dask 2026.8.0**: one `dask worker` per node with `--nthreads <cpus>` and
  `--resources CPU=..,GPU=..`; tasks use `resources={"CPU": c, "GPU": g}`.

## Running

```bash
./run_all.sh                          # binaries, venv, 3 repetitions, analysis.md (about 1.5 h)
```

or step by step:

```bash
(cd ../../e2e && python3 fetch_binaries.py --build-local)
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python workload.py          # lower bounds of all scenarios
.venv/bin/python driver.py 3          # 3 repetitions -> results/<arm>/<scenario>/rep<k>.json
.venv/bin/python driver.py 1 A-adv    # or only some scenarios
.venv/bin/python analyze.py results   # markdown tables
```

Ray needs about 27 GB of memory (one Python process per running task).

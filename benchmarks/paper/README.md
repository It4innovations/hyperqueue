# Evaluation of the MILP scheduler: scripts to reproduce the paper

This directory regenerates every measured number, table and figure of the paper *A Hybrid
MILP-Based Scheduler for Dynamic Task Workloads on Heterogeneous HPC Clusters* and of its
supplementary material.

The paper compares exactly two schedulers:

* **v0.25.1**, the last release with the previous scheduler, downloaded as the official release
  tarball;
* **head**, the scheduler described in the paper: this branch, built from source.

This branch is HyperQueue `main` plus the evaluation harness. Two Rust additions sit next to the
scheduler, and neither changes what a normal build does:

* `tako`'s opt-in `sim` feature (`crates/tako/src/internal/sim.rs`, `crates/tako/src/bin/sched_sim.rs`)
  drives the real scheduling pipeline in-process on synthetic states. It is used for the
  microbenchmarks.
* `SchedulerConfig::from_env` reads `HQ_SCHED_*` variables at server start. Unset, every one of
  them keeps the production default. Only the prefill sweep sets one (`HQ_SCHED_PREFILL_MAX`); the
  simulation sets the others through `sched_sim` flags.

## Layout

| path | content |
| --- | --- |
| `e2e/` | end-to-end experiments: a real `hq` server and local workers running `sleep` |
| `sim/` | microbenchmarks of the scheduling pipeline, driven by `sched_sim` |
| `viz_theme.py` | shared figure style |

## Requirements

* Linux, a Rust toolchain (see the repository's minimum version) and Python 3.11 or newer.
* `pip install -r requirements.txt`, needed only for the figures.
* An otherwise idle machine. The paper's numbers were measured on one laptop (Intel Core Ultra 7
  255H, 32 GB, Linux 6.17). Absolute times change with the machine; the comparisons should not.
  Both drivers wait for the load average to drop below 1 before each step.

## Running

```bash
benchmarks/paper/sim/run_all.sh   # several hours; writes sim/results/
benchmarks/paper/e2e/run_all.sh   # about two hours; writes e2e/results/
```

`e2e/run_all.sh` first runs `fetch_binaries.py --build-local`, which downloads v0.25.1 and builds
the **committed** HEAD of this repository with `--profile dist` in a temporary git worktree.
Uncommitted changes never reach a measured binary. Every run records the commit in its
`meta.json`.

Both drivers accept `RESULTS=<dir>` to write elsewhere. Single experiments can be run by copying
the matching line out of the driver.

## Where each result comes from

Sections refer to the paper. The analysis commands are run from `e2e/` or `sim/` respectively,
with `--results` pointing to the results directory of the driver.

### Comparison with the previous scheduler (end-to-end)

| result | scenarios | analysis |
| --- | --- | --- |
| Job tails table (makespan, oldest-job flow, worst 90 → 100 %, worst decile) | S3, S4, S6, S7 | `python3 analyze.py results --scenario S3 --scenario S4 --scenario S6 --scenario S7 --no-plots`: columns `makespan`, `job1flow`, `tail90`, `slowdec` |
| Narrow and wide jobs, weight 2.0 | S5 (both arms), S5W (head) | `python3 analyze.py results --scenario S5 --scenario S5W --no-plots`; per-run values in `results/<version>/<scenario>/rep*/` |
| Node-hours | N1, five repetitions | `python3 nodehours.py --results results` |
| Worker walltimes table | W2 | `python3 walltime.py --results results/walltime` |
| Walltime trace (supplementary) | W2, v0.25.1 | `python3 walltime_gantt.py --results results/walltime`, and the journals and server logs in `results/walltime/v0.25.1/W2/rep*/` |
| Compaction figure | C3, C5 | `python3 compaction.py --results results --version v0.25.1 --version head --figure` → `results/compaction-workers.pdf` |

### MILP size and solve time (microbenchmarks)

| result | sweep (`sim/results/`) | analysis |
| --- | --- | --- |
| Independence of the task count, and the partitioning time | `task_independence.csv` | `python3 tables.py --results results ti` |
| Workers and resource requests, with the truncated cell of the footnote | `workers_requests.csv` | `python3 tables.py --results results wr` |
| Priority levels | `priority_scaling_g64.csv` | `python3 tables.py --results results ps` |
| Model size figure | `priority_scaling_g64.csv` | `python3 plot_model_size.py --results results --rounds zero` → `results/model-size-r0.pdf` |

### Quality of a truncated solve

| result | sweep | analysis |
| --- | --- | --- |
| Anytime figure, and the utilization and solve times in the text | `limits3/*.csv` | `python3 plot_anytime.py --results results` → `results/anytime-utilization.pdf`, and a table per cell |
| Objective at 5 s and the remaining gap of the 500-worker, 256-level cell | `miplogs/*.log` | `python3 anytime.py results/miplogs/*.log --cutoff 5`: columns `obj@5s`, `gap end` |

### Prefilling (end-to-end, head only)

| result | scenarios | analysis |
| --- | --- | --- |
| Throughput table, round-trips and idle time | P0, P1d005, P1d02, P1, P1d2 | `python3 prefill.py --results results/prefill` |
| Prefill figure | same | `python3 plot_prefill.py --compact --results results/prefill --out results/prefill-compact` |
| Priority restriction: speedup at depth 40 against depth 0 with (P3) and without (P1) a denied queue; priority churn | P3, P1, P5 | `python3 prefill.py --results results/prefill --scenario P3 --scenario P1 --scenario P5` |

P4 (duration imbalance) belongs to the same sweep.

### Pruning the priority conditions

| result | sweep | analysis |
| --- | --- | --- |
| Pruning table, cut count, repeatability | `pruning_g.csv` | `python3 tables.py --results results pr` |
| Example of the misplaced count | none | `python3 misplaced_example.py` |
| Sampling schedule: both supplementary tables, the win counts and the rounds over the 5 s limit | `sampling.csv` | `python3 sampling.py --results results` |

The sampling workloads are the `band-*` scenarios of `sched_sim`. `band-all` puts every request at
every priority level. `band-head`, `band-middle` and `band-tail` interleave all requests only in a
band of a tenth of the levels and give every other level one request, alternating.

### The pathology of the reservation section

| result | sweep | analysis |
| --- | --- | --- |
| Tasks placed and CPUs left idle under the strict rule, with gap filling and with reservations | `pathology.csv` | `python3 tables.py --results results path` |

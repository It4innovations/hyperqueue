# Comparison with other task runtimes

Two benchmarks that compare HQ with other easily installable task runtimes.
Both run everything on one machine and use the same two HQ binaries as `../e2e`
(`../e2e/bin/hq-head` and `../e2e/bin/hq-v0.25.1`, made by
`../e2e/fetch_binaries.py --build-local`). Each directory has its own
`requirements.txt`; `run_all.sh` creates a `.venv` from it.

| directory | what | tools | time |
| --- | --- | --- | --- |
| `many-tasks/` | N trivial tasks (each starts `true`), N = 10² … 10⁶, 2 workers with 1 CPU | HQ head, Ray, Dask, Parsl, Snakemake | about 4–5 h |
| `resource-mix/` | CPU and GPU requests on 3 worker types, resource variants; emulated (sleeping tasks, fake GPUs) | HQ head, HQ v0.25.1, Ray, Dask | about 1.5 h |

## many-tasks

```bash
many-tasks/run_all.sh        # -> many-tasks/results.csv, tables printed by summarize.py
```

- `driver.py` runs every case in its own process group with a 45-minute limit,
  appends `tool,N,repetition,submit_s,total_s,status` to `results.csv`.
- Tasks: HQ submits one array `--array 1-N -- true`; Snakemake has one rule per
  task with output `touch("out/{i}.txt")` and `shell: "true"`; in Ray, Dask and
  Parsl the task is a Python function running `subprocess.run(["true"])`
  (`py_runtimes.py`, tool names `ray-proc`, `dask-proc`, `parsl-proc`; the names
  without `-proc` run an empty Python function instead).
- Concurrency is 2 tasks in every tool: HQ 2 workers with `--cpus 1`; Ray
  `ray.init(num_cpus=2)`; Dask `LocalCluster(n_workers=2, threads_per_worker=1)`;
  Parsl HTEX `max_workers_per_node=2, cores_per_worker=1`; Snakemake `--cores 2`.
- The plan: N = 10²…10⁵ three times (Snakemake only up to 10⁴), N = 10⁶ once
  (not Snakemake), and Dask at 10⁶ once more with connection timeouts of 600 s
  (`dask-proc-timeout600`), because it fails at 10⁶ with the default settings.
- The reported numbers were measured with a plain `--release` build of `main` at
  `77603bfab`, the base of this branch; `run_hq.sh` takes another binary with
  `HQ=/path/to/hq`.

## resource-mix

```bash
resource-mix/run_all.sh      # -> resource-mix/results/, resource-mix/analysis.md
```

See `resource-mix/README.md` for the cluster, the task classes, the scenarios
(A: fixed requests; B, B2: tasks with a GPU and a CPU variant), the lower
bound and the metrics. Note the workaround for a v0.25.1 panic with resource
variants, described there and in `run_hq.py`.

## Figures

```bash
(cd resource-mix && .venv/bin/python export_csv.py results)   # -> resource-mix/results/metrics.csv
python3 plot_others.py                                       # -> figures/others-{overhead,placement}.pdf
```

`plot_others.py` reads `many-tasks/results.csv` and `resource-mix/results/metrics.csv` and
needs the packages of `../requirements.txt` (Altair, vl-convert). Both figures are 260 pt wide
and are included at `0.58\linewidth`.

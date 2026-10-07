# HyperQueue for Nibi

This fork runs a shared HyperQueue server and requests Nibi SLURM allocations as tasks arrive. Tasks run inside those allocations, allowing many short jobs to reuse the same worker. `configs/nibi.sh` defines the worker sizes and limits below; no SLURM partitions are selected.

## Install

Run in zsh on a Nibi login node with `SLURM_ACCOUNT` already exported:

```zsh
source <(curl -fsSL https://raw.githubusercontent.com/jaredfischbach/hyperqueue/main/configs/install_nibi.sh)
```

The installer downloads the latest published stable Linux x86-64 release and its matching config and launcher. Changes on `main` become available as binaries after a release tag is published. It requires Python 3, `curl`, `tar`, and SLURM client commands.

The tool and allocation config go in `/project/$SLURM_ACCOUNT/tools/hyperqueue/`. The server script goes in `$HOME/hyperqueue/hyperqueue_server.sh`. `$HOME/.bashrc.d/hyperqueue.sh` contains exactly three exports: `PATH`, `HQ_JOURNAL_DIR`, and `HQ_SERVER_DIR`. The installer sources that file and `.zshrc` in your current shell.

## Start the shared server

The installer adds these functions to `.zshrc`, replacing the old `hqstart` function:

| Command | Where the server runs | Output |
| --- | --- | --- |
| `hqstart_shell` | A SLURM batch job: one CPU, 4096M, up to 168 hours | `$HOME/logs/hyperqueue_server_<jobid>.log` |
| `hqstart_tmux` | A detached `hyperqueue` tmux session on the current login node | The tmux session |
| `hqstart_nohup` | A background process on the current login node | `$HOME/logs/hyperqueue_server_nohup.log` |

All three run the same server script, journal, and allocation config. Use one shared server for combined demand and limits across pipelines. Tmux remains on the login node where it was started; nohup also stays on that node. The seven-day SLURM walltime applies to the batch option.

The server stops after 30 minutes without waiting, running, or open jobs. Before stopping, it flushes `$HQ_JOURNAL_DIR/hq.journal` and exports timestamped HTML to `$HQ_JOURNAL_DIR/reports/`. It does not prune the journal, so reports include retained history. The batch launcher also stops five minutes before its walltime expires, even if tasks are still running.

Queues are registered when the server has no allocation queues. A restored journal restores its existing queues; installing a newer config does not replace them automatically. Journal recovery restores jobs and queues, but workers must reconnect through new allocations. See the official [server and recovery documentation](https://it4innovations.github.io/hyperqueue/stable/deployment/server/).

## Worker options

Every row has five walltime queues: **3, 12, 24, 72, and 168 hours**. Names append the duration, for example `cpu_base_quarter-72h`. Each SLURM allocation starts one worker on one node with one task and one thread per core. CPU full-node allocations are exclusive; fractional CPU allocations and all GPU allocations are not.

Memory is in MiB, matching SLURM `--mem=<value>M`. The total limit counts queued plus running workers. Backlog limits queued allocations and is part of that total. Both limits apply across all five durations for that row, including paused queues.

| Worker group | CPUs | Memory (MiB) | Task class (`--resource`) | Minimum requested CPUs for a new allocation | Total limit | Backlog | Exclusive |
| --- | ---: | ---: | --- | ---: | ---: | ---: | --- |
| `cpu_base` | 192 | 766000 | `worker/cpu=1` | 96 (50%) | 100 | 25 | Yes |
| `cpu_base_half` | 96 | 383000 | `worker/cpu=1` | 48 (50%) | 1 | 1 | No |
| `cpu_base_quarter` | 48 | 191500 | `worker/cpu=1` | 24 (50%) | 1 | 1 | No |
| `cpu_base_eighth` | 24 | 95750 | `worker/cpu=1` | None | 1 | 1 | No |
| `cpu_base_sixteenth` | 12 | 47875 | `worker/cpu=1` | 6 (50%) | 1 | 1 | No |
| `cpu_large` | 192 | 6144000 | `worker/cpuLarge=1` | 96 (50%) | 4 | 2 | Yes |
| `cpu_large_half` | 96 | 3072000 | `worker/cpuLarge=1` | 48 (50%) | 1 | 1 | No |
| `cpu_large_quarter` | 48 | 1536000 | `worker/cpuLarge=1` | 24 (50%) | 1 | 1 | No |
| `mi300a` | 24 | 126750 | `worker/mi300a=1` | None | 8 | 4 | No |
| `h100_full` | 14 | 256000 | `worker/h100=1` | None | 16 | 8 | No |
| `h100_1g.10gb` | 2 | 31744 | `worker/h100mig10=1` | None | 48 | 24 | No |
| `h100_2g.20gb` | 4 | 63488 | `worker/h100mig20=1` | None | 24 | 12 | No |
| `H100-3g.40gb` | 6 | 126976 | `worker/h100mig40=1` | None | 24 | 12 | No |

GPU workers each reserve **one** matching GPU or MIG instance via SLURM `--gres=gpu:<type>:1`. GPU tasks must request their exact class **and** `gpus=1`. They cannot run on CPU workers. Every CPU task must request its CPU class, so it cannot run on GPU workers.

## Allocation and scheduling rules

Resources are explicit (`--detect-resources none`). Missing classes stay absent during allocation planning, including before the first worker connects. There are 65 allocation queues; registering them does not start 65 workers. Allocations are submitted only when eligible pending work exists.

Within a CPU class, larger workers are preferred when enough fitting work meets their CPU threshold and shared limits. The base eighth has no minimum, so it can serve small demand before a sixteenth worker; the sixteenth is another capped option when eligible. CPU demand means requested CPUs, not measured CPU activity or memory usage.

Every **new large-memory allocation** must contain at least one task requesting **strictly more than 766000 MiB**, with `worker/cpuLarge=1`. Equality does not qualify, and the combined memory of several smaller tasks cannot trigger it. Each allocation must also reach 50% requested CPU demand. Smaller tasks with the same large class can contribute to that demand, but cannot cause extra large allocations on their own. Connected large workers can accept smaller tasks with `worker/cpuLarge`; ordinary `worker/cpu` tasks stay on base workers.

For a **new SLURM submission**, the allocator chooses the first walltime tier strictly longer than the task's `--time-request`:

| Task time request | New allocation walltime |
| --- | --- |
| Less than 3h | 3h |
| 3h to less than 12h | 12h |
| 12h to less than 24h | 24h |
| 24h to less than 72h | 72h |
| 72h to less than 168h | 168h |
| 168h or more | No eligible configured tier |

Connected workers accept tasks that fit their available resources and remaining time, without the extra time tier or allocation CPU/memory thresholds. A worker exits after **five minutes with no running tasks**. Falling below 50% does not retire it; a running task still counts as active even when its measured CPU usage is zero. SLURM allocation status is refreshed every five minutes.

These allocation policies are specific to this fork. Standard resource requests and worker behavior are described in the official [resources](https://it4innovations.github.io/hyperqueue/stable/jobs/resources/) and [automatic allocation](https://it4innovations.github.io/hyperqueue/stable/deployment/allocation/) documentation.

## Resource pools and submissions

Memory and CPU class resources use **sum pools**. For example, a 48-CPU worker supplies `worker/cpu=sum(48)` and `mem=sum(191500)`. `worker/cpu=1` selects that class and consumes one logical class unit; `--cpus` and `--resource mem=...` declare the task's actual CPU and memory needs. The scheduler keeps simultaneous requests within those capacities.

GPU class resources and `gpus` use **indexed pools**, each `[0]` for a single allocated device. Requesting one reserves that index exclusively for the task. These are logical pools; the config preserves SLURM's GPU visibility environment rather than treating `0` as a physical device number. See the official [resource pool examples](https://it4innovations.github.io/hyperqueue/stable/jobs/resources/#worker-resources).

For a mock script requesting **37 CPUs, 123456 MiB, and 24h**:

```bash
cat > mock_task.sh <<'EOF'
#!/usr/bin/env bash
hostname
sleep 30
EOF

hq submit \
    --name selection-example \
    --cpus 37 \
    --resource mem=123456 \
    --resource worker/cpu=1 \
    --time-request 24h \
    --time-limit 24h \
    /bin/bash ./mock_task.sh
```

With no connected workers or other demand, this selects a 48-CPU, 191500-MiB base quarter allocation at **72h**. It exceeds the quarter's 24-CPU minimum; the half and full minimums are not met.

A large-memory example requests **24 CPUs and 800000 MiB**. With no other demand it selects the large quarter at 72h:

```bash
hq submit --cpus 24 --resource mem=800000 --resource worker/cpuLarge=1 \
    --time-request 24h --time-limit 24h /bin/bash ./mock_task.sh
```

A 20-GB H100 MIG example requests **4 CPUs, 63488 MiB, and one device**:

```bash
hq submit --cpus 4 --resource mem=63488 --resource worker/h100mig20=1 \
    --resource gpus=1 --time-request 1h --time-limit 1h /bin/bash ./mock_task.sh
```

`--time-request` checks worker eligibility and chooses the new allocation tier. `--time-limit` is the task's execution limit. These examples reserve real SLURM resources even though the mock script only sleeps. More examples are in the official [submission documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/jobs/).

For Nextflow, use the `hq` executor and pass the class and `--time-request` through `clusterOptions`. A process that replaces `clusterOptions` must include its own time request. GPU processes also set `accelerator` so Nextflow requests `gpus`. Set the applicable Nextflow memory limits above 766000 MiB when using the large class. See the official [Nextflow HyperQueue executor](https://docs.seqera.io/nextflow/executor/hyperqueue).

## Monitor and releases

Run `hq dashboard` to view jobs, workers, and allocations. Journalling is enabled by the launcher. See the official [dashboard documentation](https://it4innovations.github.io/hyperqueue/stable/cli/dashboard/). `hq alloc list`, `hq worker list`, and `hq job list` also show current state.

Only pushing a `v*` release tag triggers CI. Linux x86-64 tests and allocation regressions must pass before the Linux binary is built and published. There are no macOS, ARM, PowerPC, Python-wheel, nightly, container, or documentation-deployment builds.

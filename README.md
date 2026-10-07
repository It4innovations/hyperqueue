# HyperQueue for Nibi HPC

This fork runs a shared HyperQueue server and requests Nibi SLURM allocations as tasks arrive. Tasks run inside those allocations, allowing many short jobs to reuse the same worker. `configs/nibi.sh` defines the worker sizes and limits below; no SLURM partitions are selected.

## Install

Run in zsh on a Nibi login node with `SLURM_ACCOUNT` already exported:

```zsh
source <(curl -fsSL https://raw.githubusercontent.com/jaredfischbach/HyperNibi/main/configs/install_nibi.sh)
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

Each SLURM allocation starts one worker on one node with one task and one thread per core. CPU full-node allocations are exclusive; fractional CPU allocations and all GPU allocations are not.

Memory is in MiB, matching SLURM `--mem=<value>M`. Note: `1G = 1024M`.

| Worker group | CPUs | Memory (MiB) | Task class (`--resource`) | Minimum requested CPUs for a new allocation | Total limit | Backlog | Exclusive |
| --- | ---: | ---: | --- | ---: | ---: | ---: | --- |
| `cpu_base_full` | 192 | 766000 | `worker/cpu=1` | 96 (50%) | 100 | 25 | Yes |
| `cpu_base_half` | 96 | 383000 | `worker/cpu=1` | 48 (50%) | 1 | 1 | No |
| `cpu_base_quarter` | 48 | 191500 | `worker/cpu=1` | 24 (50%) | 1 | 1 | No |
| `cpu_base_eighth` | 24 | 95750 | `worker/cpu=1` | 12 (50%) | 1 | 1 | No |
| `cpu_base_sixteenth` | 12 | 47875 | `worker/cpu=1` | None | 1 | 1 | No |
| `cpu_large_full` | 192 | 6144000 | `worker/cpuLarge=1` | 96 (50%) | 4 | 2 | Yes |
| `cpu_large_half` | 96 | 3072000 | `worker/cpuLarge=1` | 48 (50%) | 1 | 1 | No |
| `cpu_large_quarter` | 48 | 1536000 | `worker/cpuLarge=1` | 24 (50%) | 1 | 1 | No |
| `mi300a` | 24 | 126750 | `worker/mi300a=1` | N/A | 8 | 4 | No |
| `h100_full` | 14 | 256000 | `worker/h100=1` | N/A | 16 | 8 | No |
| `h100_1g.10gb` | 2 | 31744 | `worker/h100mig10=1` | N/A | 48 | 24 | No |
| `h100_2g.20gb` | 4 | 63488 | `worker/h100mig20=1` | N/A | 24 | 12 | No |
| `H100-3g.40gb` | 6 | 126976 | `worker/h100mig40=1` | N/A | 24 | 12 | No |

GPU workers each reserve **one** matching GPU or MIG instance via SLURM `--gres=gpu:<type>:1`. GPU tasks must explicitly request the exact GPU or MIG class from the table, **`--resource gpus=1`**, including matching their defined CPU and memory allotments. For example, `--resource worker/h100mig20=1` selects the 20-GB H100 MIG type; `gpus=1` alone does not select a model or MIG size. Each task reserves the worker's one indexed device. Every task must request its resource class using `--resource`, so it can run only on workers that provide that class.

This Nibi setup currently supports **one GPU or MIG instance and one node per task**. Multi-GPU requests such as `:2` or `:4`, and individual tasks spanning multiple nodes, are not supported.

## Allocation and scheduling rules

Resources are explicit (`--detect-resources none`). Missing classes stay absent during allocation planning, including before the first worker connects. There are 65 allocation queues; registering them does not start 65 workers. Allocations are submitted only when eligible pending work exists.

Within a CPU class, larger workers are preferred when enough fitting work meets their CPU threshold and shared limits. The base sixteenth has no minimum and can serve small demand below the eighth's 12-CPU minimum. CPU demand means requested CPUs, not measured CPU activity or memory usage.

Every **new large-memory allocation** must contain at least one task requesting **strictly more than 766000 MiB**, with `worker/cpuLarge=1`. Equality does not qualify, and the combined memory of several smaller tasks cannot trigger it. Each allocation must also reach 50% requested CPU demand. Smaller tasks with the same large class can contribute to that demand, but cannot cause extra large allocations on their own. Connected large workers can accept smaller tasks with `worker/cpuLarge`; ordinary `worker/cpu` tasks stay on base workers.

Every row has five walltime queues: **3, 12, 24, 72, and 168 hours**. Names append the duration, for example `cpu_base_quarter-72h`.

The total limit counts queued plus running workers. Backlog limits queued allocations and is part of that total. Both limits apply across all five durations for that row, including paused queues.

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

The two task time options have separate purposes:

- **`--time-request`** is the minimum remaining worker lifetime needed to start the task. In this fork it also selects the next longer walltime tier when a new SLURM allocation is needed. A 24h request therefore needs at least 24h remaining on an existing worker, or starts a new 72h allocation. It does not stop the task after 24h.
- **`--time-limit`** is the task's execution limit, counted from when that task starts. HQ terminates the task if it reaches this limit. It does not select a worker or allocation tier.

These examples reserve real SLURM resources even though the mock script only sleeps. See the official [time management and submission documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/jobs/#time-management).

## HQ task arrays

Use `hq submit --array` to create one HQ job containing multiple independent tasks. The range is inclusive, and each task receives its own `HQ_TASK_ID`. For example, this creates 48 tasks, **each** requesting 4 CPUs, 4096 MiB, the base CPU class, and one hour:

```bash
hq submit \
    --name array-example \
    --array 1-48 \
    --cpus 4 \
    --resource mem=4096 \
    --resource worker/cpu=1 \
    --time-request 1h \
    --time-limit 1h \
    -- /bin/bash -c 'printf "Task %s on %s\n" "$HQ_TASK_ID" "$(hostname)"; sleep 30'
```

HQ schedules these tasks inside eligible workers using the same allocation rules and shared limits. The time limit applies separately to each task. Default stdout and stderr files are separate for each task under `job-<jobid>/`.

You can also create tasks from a file with `--each-line inputs.txt` or a JSON array with `--from-json inputs.json`; each task receives its input through `HQ_ENTRY`. See the official [HQ task array documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/arrays/).

## Nextflow

Start the shared server before launching a pipeline. Nextflow submits its process tasks to HQ; HQ packs eligible tasks into workers and submits SLURM allocations when more capacity is needed. Pipelines using the same `HQ_SERVER_DIR` share those workers and allocation limits. Nextflow still manages dependencies, containers, task work directories, outputs, and resume caching.

The Nextflow launch environment needs the installed `hq` on `PATH` and the same server directory as the server launcher. For example, in a pipeline batch script:

```bash
export PATH="/project/$SLURM_ACCOUNT/tools/hyperqueue/0.26.2:$PATH"
export HQ_SERVER_DIR="$HOME/hyperqueue/server"

hq server info >/dev/null 2>&1 || exit 1
```

`HQ_JOURNAL_DIR` is used by the server launcher; Nextflow does not need it to submit tasks. Keep the pipeline work directory and inputs on storage accessible to the workers. See the official [Nextflow HQ executor documentation](https://docs.seqera.io/nextflow/executor/hyperqueue).

### Base CPU configuration

Add this to your custom Nextflow config and pass it with `-c`. This sets the base CPU class for every process; pipeline process selectors can still supply their own CPU, memory, and time values.

```groovy
process {
    executor = 'hq'
    cpus     = 1
    memory   = 4096.MB
    time     = 4.h

    clusterOptions = { "--resource worker/cpu=1 --time-request ${task.time.toSeconds()}sec" }
}
```

Nextflow translates the resolved task directives into HQ submission options:

| Nextflow setting | HQ submission option | Purpose |
| --- | --- | --- |
| `cpus` | `--cpus <count>` | Reserve task CPU capacity |
| `memory` | `--resource mem=<MiB>` | Reserve task memory capacity |
| `time` | `--time-limit <seconds>sec` | Stop a task that reaches its execution limit |
| `accelerator = 1` | `--resource gpus=1` | Reserve one indexed GPU |
| `clusterOptions` | Additional HQ options | Select the resource class and supply `--time-request` |

Nextflow uses binary memory units: `4096.MB` becomes `mem=4096`, matching the MiB values in the worker table. See [Nextflow memory units](https://docs.seqera.io/nextflow/reference/stdlib-types/memory-unit).

Nextflow's `time` directive supplies **`--time-limit`**, so the closure separately supplies **`--time-request`** from the same resolved `task.time`. With `time = 4.h`, both are 14400 seconds: a connected worker needs at least four hours remaining, while a new allocation uses the 12h tier. The task itself has a four-hour execution limit. Dynamic time values, including retry increases, are evaluated for each task. See the [Nextflow HQ submission implementation](https://github.com/nextflow-io/nextflow/blob/master/modules/nextflow/src/main/groovy/nextflow/executor/HyperQueueExecutor.groovy) and [dynamic directives](https://docs.seqera.io/nextflow/process#dynamic-directives).

### Large-memory and GPU processes

Choose these classes explicitly with `withName` or `withLabel` selectors. A larger memory request alone does not change `worker/cpu` to `worker/cpuLarge`. The labels below are examples: replace them with labels your pipeline actually uses, or target the required processes with `withName`.

```groovy
process {
    withLabel: process_large_memory {
        cpus   = 24
        memory = 800000.MB
        time   = 24.h

        clusterOptions = { "--resource worker/cpuLarge=1 --time-request ${task.time.toSeconds()}sec" }
    }

    withLabel: process_gpu {
        cpus        = 14
        memory      = 256000.MB
        time        = 4.h
        accelerator = 1

        clusterOptions = { "--resource worker/h100=1 --time-request ${task.time.toSeconds()}sec" }
    }
}
```

The large-memory example can trigger the large quarter: its individual request exceeds 766000 MiB and reaches that worker's 24-CPU minimum. A new allocation uses 72h for its 24h time request. Any applicable `process.resourceLimits` or pipeline-specific memory cap must permit the requested memory; a cap that lowers it to 766000 MiB or less prevents a new large allocation. See [process selectors](https://docs.seqera.io/nextflow/config#process-selectors) and [resource limits](https://docs.seqera.io/nextflow/reference/process/directives/resource-limits).

The GPU example selects the full H100 class. `accelerator = 1` supplies `gpus=1`; the class identifies the model. For a MIG or MI300A task, replace `worker/h100` with its exact class from the worker table and keep CPU and memory requests within that row's capacities. Each task uses one device and one node.

**A selector that sets `clusterOptions` replaces the default value.** Include both its class and its time request in that replacement. These options go to `hq submit`, so use HQ flags there; SLURM options such as the account, `--gres`, and exclusivity belong in the HQ allocation config. See [clusterOptions](https://docs.seqera.io/nextflow/reference/process/directives/cluster-options).

### Launch and submission controls

For example, with the HQ settings saved in `nextflow-hq.config` and your pipeline parameters in `params.json`:

```bash
nextflow run nf-core/rnaseq -c nextflow-hq.config -params-file params.json -resume
```

Add your usual pipeline revision and container profile. Each Nextflow task is submitted as its own HQ job; the HQ task-array example above is for direct `hq submit` use. Nextflow's `process.array` directive does not support the HQ executor. See the official [supported array executors](https://docs.seqera.io/nextflow/reference/process/directives/array).

Nextflow's `executor.pollInterval`, `queueSize`, and `submitRateLimit` control its task monitoring and submissions to HQ. They do not change HQ's SLURM polling, worker limits, or backlog. Those are controlled by this fork and `configs/nibi.sh`. See the official [executor settings](https://docs.seqera.io/nextflow/reference/config/executor).

## Monitor and releases

Run `hq dashboard` to view jobs, workers, and allocations. Journalling is enabled by the launcher. See the official [dashboard documentation](https://it4innovations.github.io/hyperqueue/stable/cli/dashboard/). `hq alloc list`, `hq worker list`, and `hq job list` also show current state.

Only the Nibi-compatible Linux x86-64 build is provided.

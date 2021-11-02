Workers connect to a running instance of a HyperQueue [server](server.md) and wait for task assignments. Once some task
is assigned to them, they will compute it and notify the server of its completion.

## Starting workers
Workers should be started on machines that will actually execute the submitted computations, e.g. computing nodes on an
HPC cluster. You can either use the automatic allocation system of HyperQueue to start workers as needed, or deploy
workers manually.

### Automatic worker deployment (recommended)
If you are using a job manager (PBS or Slurm) on an HPC cluster, the easiest way of deploying workers is to use
[**Automatic allocation**](allocation.md). It is a component of HyperQueue that takes care of submitting PBS/Slurm jobs
and spawning HyperQueue workers.

### Manual worker deployment
If you want to start a worker manually, you can use the following command:

```
$ hq worker start
```

Each worker will be assigned a unique ID that you can use in later commands to query information about the worker or to
[stop](#stopping-worker) it.

By default, the worker will try to connect to a server using the default [server directory](server.md#server-directory).
If you want to connect to a different server, use the `--server-dir` option.

!!! important "Sharing the server directory"

    When you start a worker, it will need to read the server directory to find out how to connect to the server. The
    directory thus has to be accesible both by the server and the worker machines. On HPC clusters, it is common that
    login nodes and compute nodes use a shared filesystem, so this shouldn't be a problem.

    However, if a shared filesystem is not available on your cluster, you can just copy the server directory from the
    server machine to the worker machine and access it from there. The worker machine still has to be able to initiate
    a TCP/IP connection to the server machine though.

#### Deploying a worker using PBS/Slurm
If you want to manually start a worker using PBS or Slurm, simply use the corresponding submit command (`qsub` or `sbatch`)
and run the `hq worker start` command inside the allocated job. If you want to start a worker on each allocated node,
you can run this command on each node using e.g. `mpirun`.

Example submission script:

=== "PBS"

    ```bash
    #!/bin/bash
    #PBS -q <queue>

    # Run a worker on the main node
    /<path-to-hyperqueue>/hq worker start --manager pbs

    # Run a worker on all allocated nodes
    ml OpenMPI
    mpirun /<path-to-hyperqueue>/hq worker start --manager pbs
    ```

=== "Slurm"

    ```bash
    #!/bin/bash
    #SBATCH --partition <partition>

    # Run a worker on the main node
    /<path-to-hyperqueue>/hq worker start --manager slurm

    # Run a worker on all allocated nodes
    ml OpenMPI
    mpirun /<path-to-hyperqueue>/hq worker start --manager slurm
    ```

The worker will try to automatically detect that it is started under a PBS/Slurm job, but you can also explicitly pass
the option `--manager <pbs/slurm>` to tell the worker that it should expect a specific environment.

#### Stopping workers
If you have started a worker manually, and you want to stop it, you can use the `hq worker stop` command[^2]:

```bash
# Stop a specific worker
$ hq worker stop <worker-id>

# Stop all workers
$ hq worker stop all
```

[^2]: You can use various [shortcuts](../tips/cli-shortcuts.md#id-selector) to select multiple workers at once.

## Time limit
HyperQueue workers are designed to be volatile, i.e. it is expected that they will be stopped from time to time, because
they are often started inside PBS/Slurm jobs that have a limited duration.

It is very useful for the workers to know how much remaining time ("lifetime") do they have until they will be stopped.
This duration is called the `Worker time limit`.

When a worker is started inside a PBS or Slurm job, it will automatically calculate the time limit from the job's
metadata. If you want to set time limit manually for workers started outside of PBS/Slurm jobs or if you want to
override the detected settings, you can use the `--time-limit=<DURATION>` option[^1] when starting the worker.

[^1]: You can use various [shortcuts](../tips/cli-shortcuts.md#duration) for the duration value.

When the time limit is reached, the worker is automatically terminated.

## Idle timeout
When you deploy *HQ* workers inside a PBS or Slurm job, keeping the worker alive will drain resources from your
accounting project (unless you use a free queue). If a worker has nothing to do, it might be better to terminate it
sooner to avoid paying these costs for no reason.

You can achieve this using `Worker idle timeout`. If you use it, the worker will automatically stop if it receives no
task to compute for the specified duration. For example, if you set the idle duration to five minutes, the worker will
stop once it hadn't received any task to compute for five minutes.

You can set the idle timeout using the `--idle-timeout` option[^1] when starting the worker.

!!! tip

    Workers started [automatically](allocation.md#behavior) have the idle timeout set to five minutes.

Idle timeout can also be configured globally for all workers using the `--idle-timeout` option when starting a server:

```bash
$ hq server start --idle-timeout=<TIMEOUT>
```

This value will be then used for each worker that does not explicitly specify its own idle timeout.

## Worker state
Each worker can be in one of the following states:

* **Running** Worker is running and is able to process tasks
* **Connection lost** Worker lost connection to the server. Probably someone manually killed the worker or job walltime
  in its PBS/Slurm job was [reached](#time-limit).
* **Heartbeat lost** Communication between server and worker was interrupted. It usually signifies a network problem or
  a hardware crash of the computational node.
* **Stopped** Worker was [stopped](#stopping-worker).
* **Idle timeout** Worker was terminated due to [Idle timeout](#idle-timeout).

## Useful worker commands
Here is a list of useful worker commands:

### Display all workers
```bash
$ hq worker list
```

You can filter workers by their state (`running`, `offline`) using the `--filter` option.

### Display information about a specific worker
```bash
$ hq worker info <worker-id>
```

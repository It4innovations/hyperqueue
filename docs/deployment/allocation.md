**Automatic allocation** is one of the core features of HyperQueue. When you run HyperQueue on an HPC cluster, it allows
you to autonomously ask the job manager (PBS/Slurm) for computing resources and spawn HyperQueue [workers](worker.md)
on the provided nodes.

Using this mechanism, you can submit computations into HyperQueue without caring about the underlying PBS/Slurm jobs.

!!! Note "Job terminology"

    It is common to use the term "job" for jobs created by an HPC job manager, such as PBS or Slurm, which are used to
    perform computations on HPC clusters. However, HyperQueue also uses the term "job" for [ensembles of tasks](../jobs/jobs.md).

    To differentiate between these two, we will refer to jobs created by PBS or Slurm as `allocations`. We will also refer
    to PBS/Slurm as a `job manager`.

## Allocation queue
To enable automatic allocation, you have to create an `Allocation queue`. It describes a specific configuration that
will be used by HyperQueue to request computing resources from the job manager on your behalf.

Each allocation queue has a set of [parameters](#parameters). You can use them to modify the behavior of automatic
allocation, but for start you can simply use the defaults. However, you will almost certainly need to specify some
credentials to be able to ask for computing resources using PBS/Slurm.

To create a new allocation queue, use the following command and pass any required credentials (queue/partition name,
account ID, etc.) after `--`. These trailing arguments will then be passed directly to `qsub`/`sbatch`:

=== "PBS"

    ```bash
    $ hq alloc add pbs --time-limit 1h -- -qqprod -AAccount1
    ```

=== "Slurm"

    ``` bash
    $ hq alloc add slurm --time-limit 1h -- --partition=p1
    ```

!!! tip

    Make sure that a HyperQueue server is running when you execute this command. Allocation queues are not persistent,
    so you have to set them up each time you (re)start the server.

!!! warning

    Do not pass the number of nodes that should be allocated or the allocation walltime using these trailing arguments.
    These parameters are configured using other means, see [below](#parameters).

Once the queue is created, HyperQueue will start asking for allocations in order to provide computing resources
(HyperQueue workers). The exact behavior of the automatic allocation process is described [below](#behavior). You can
create multiple allocation queues, and you can even combine PBS queues with Slurm queues.

### Parameters
In addition to arguments that are passed to `qsub`/`sbatch`, you can also use several other command line options when
creating a new allocation queue:

- **`--time-limit <duration>`** Sets the walltime of created allocations[^1].
  
    This parameter is required, as HyperQueue must know the duration of the individual allocations.
    Make sure that you pass a  time limit that does not exceed the limit of the PBS/Slurm queue
    that you intend to use, otherwise the allocation submissions will fail. You can use the
    [`dry-run` command](#dry-run-command) to debug this.

- `--backlog <count>` How many allocations should be queued (waiting to be started) in PBS/Slurm at any given time.
- `--workers-per-alloc <count>` How many nodes should be requested in each allocation.
- `--name <name>` Name of the allocation queue. Will be used to name allocations. Serves for debug purposes only.
- **Worker resources** You can specify [CPU](../jobs/cresources.md) and [generic](../jobs/gresources.md)
  resources of workers spawned in the created allocation queue. The name and syntax of these parameters
  is the same as when you create a worker manually:

=== "PBS"

    ```bash
    $ hq alloc add pbs --time-limit 1h --cpus 4x4 --resource "gpu=indices(1-2)" -- -qqprod -AAccount1
    ```

=== "Slurm"

    ``` bash
    $ hq alloc add slurm --time-limit 1h --cpus 4x4 --resource "gpu=indices(1-2)" -- --partition=p1
    ```

[^1]: You can use various [shortcuts](../tips/cli-shortcuts.md#duration) for the duration value.

## Behavior
The automatic allocator is a periodic process that runs every few seconds and does the following:

1. If there are no waiting tasks in HQ, it immediately ends. This avoids queuing allocations if there is nothing to
compute.

2. Otherwise, the allocator queues new allocations to make sure that there are is a specific number of allocations waiting
to be started by the job manager. This number is called **backlog** and you can [set it](#parameters) when creating the
queue.

    For example, if **backlog** was set to `4` and there is currently only one allocation queued into the job manager,
    the allocator would queue three more allocations.

    The backlog serves to pre-queue allocations, because it can take some time before the job manager starts them, and
    also as a load balancing factor, since it will allocate as many resources as the job manager allows.

    !!! note

        The **backlog** value does not limit the number of running allocations, only the number of queued allocations.

3. When an allocation starts, a HyperQueue [worker](worker.md) will start and connect to the HyperQueue server that
queued the allocation. The worker has the [idle timeout](worker.md#idle-timeout) set to five minutes, therefore it will
terminate if it doesn't receive any new tasks for five minutes.

## Stopping automatic allocation
If you want to remove an allocation queue, use the following command:

```bash
$ hq alloc remove <queue-id>
```

When an allocation queue is removed, all its corresponding queued and running allocations will be
canceled immediately.

By default, HQ will not allow you to remove an allocation queue that contains a running allocation.
If you want to force its removal, use the `--force` flag.

When the HQ server stops, it will automatically remove all allocation queues and cleanup all
allocations.

## Debugging automatic allocation
Since the automatic allocator is a "background" process that interacts with an external job manager, it can be challenging
to debug its behavior. To aid with this process, HyperQueue provides a "dry-run" command that you can
use to test allocation parameters. HyperQueue also provides various sources of information that can help you
find out what is going on.

### Dry-run command
To test whether PBS/Slurm will accept the submit parameters that you provide to the auto allocator,
you can use the `dry-run` command. It accepts the same parameters as `hq alloc add`, which it will use
to immediately submit an allocation and print any encountered errors.

```bash
$ hq alloc dry-run pbs --timelimit 2h -- q qexp -A Project1
```

If the allocation was submitted successfully, it will be canceled immediately to avoid wasting resources.

### Finding information about allocations
- **[`Basic queue information`](#display-information-about-an-allocation-queue)** This command will show you details about
allocations created by the automatic allocator.
- **[`Allocator events`](#display-events-of-an-allocation-queue)** Each time the allocator performs some action or notices
that a status of some allocation was changed, it will create a corresponding event. You can use this command to list
most recent events to see what was the allocator doing.
- **Extended logging** To get more information about what is happening inside the allocator, start the HyperQueue
[server](server.md) with the following environment variable:

    ```bash
    $ RUST_LOG=hyperqueue::server::autoalloc=debug hq server start
    ```

    The log output of the server will then contain a detailed trace of allocator actions.

- **Allocation files** Each time the allocator queues an allocation into the job manager, it will write the submitted
bash script, allocation ID and `stdout` and `stderr` of the allocation to disk. You can find these files inside the
server directory:

    ```bash
    $ ls <hq-server-dir>/hq-current/autoalloc/<queue-id>/<allocation-num>/
    stderr
    stdout
    job-id
    hq-submit.sh
    ```

## Useful autoalloc commands
Here is a list of useful commands to manage automatic allocation:

### Display a list of all allocation queues
```bash
$ hq alloc list
```

### Display information about an allocation queue
```bash
$ hq alloc info <queue-id>
```

You can filter allocations by their state (`queued`, `running`, `finished`, `failed`) using the `--filter` option.

### Display events of an allocation queue
```bash
$ hq alloc events <queue-id>
```

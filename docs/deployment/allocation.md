**Automatic allocation** is one of the core features of HyperQueue. When you run HyperQueue on an HPC cluster, it allows
you to autonomously ask the job manager (PBS/Slurm) for computing resources and spawn HyperQueue [workers](worker.md)
on the provided nodes.

Using this mechanism, you can submit computations into HyperQueue without caring about the underlying PBS/Slurm allocations.

!!! Note "Job terminology"

    It is common to use the term "job" for allocations created by an HPC job manager, such as PBS or Slurm, which are used to
    perform computations on HPC clusters. However, HyperQueue also uses the term "job" for [ensembles of tasks](../jobs/jobs.md).

    To differentiate between these two, we will refer to jobs created by PBS or Slurm as `allocations`. We will also refer
    to PBS/Slurm as a `job manager`.

## Allocation queue
To enable automatic allocation, you have to create an `Allocation queue`. It describes a specific configuration that
will be used by HyperQueue to request computing resources from the job manager on your behalf.

Each allocation queue has a set of [parameters](#parameters). You can use them to modify the behavior of automatic
allocation, but for start you can simply use the defaults. However, you will almost certainly need to specify some
credentials to be able to ask for computing resources using PBS/Slurm.

To create a new allocation queue, you can use the following command. Any trailing arguments (passed after `--`) will be passed verbatim directly to `qsub`/`sbatch` when requesting a new allocation.

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
    so you have to set them up each time you (re)start the server (unless you use [journaling](server.md#resuming-stoppedcrashed-server)).

The HyperQueue automatic allocator decides the number of nodes that will be requested and also the walltime of the allocation. Other parameters, such as project credentials, account ID, queue/partition name or the number of requested CPU or GPU cores per node have to be specified manually by the user using the trailing arguments.

!!! warning

    The corollary of the paragraph above is that you **should not** pass the number of nodes that should be allocated or the allocation walltime in the trailing arguments. Instead, you can specify various [parameters](#parameters) that tell the automatic allocator how to configure these options.

Once the queue is created, HyperQueue will start asking for allocations in order to provide computing resources
(HyperQueue workers). The exact behavior of the automatic allocation process is described [below](#behavior). You can
create multiple allocation queues, and you can even combine PBS queues with Slurm queues.

!!! warning

    Note that the HQ server needs to have access to `qsub` or `sbatch` binaries on the node where it is executed. If you
    want to submit PBS/Slurm allocations on a remote cluster, you will need to use e.g. a proxy to redirect the commands
    to that cluster. See [this issue](https://github.com/It4innovations/hyperqueue/issues/695) for more information. If you
    have a use-case for such remote PBS/Slurm allocation submission, please [let us know](https://github.com/It4innovations/hyperqueue/issues),
    as we could try to make that easier in HyperQueue if there was enough interest in it.

### Parameters
In addition to arguments that are passed to `qsub`/`sbatch`, you can also use several other command line options when
creating a new allocation queue:

#### Time limit
Format[^1]: **`--time-limit <duration>`**

Sets the walltime of created allocations.

This parameter is **required**, as HyperQueue must know the duration of the individual allocations.
Make sure that you pass a time limit that does not exceed the limit of the PBS/Slurm queue
that you intend to use, otherwise the allocation submissions will fail. You can use the
[`dry-run` command](#dry-run-command) to debug this.

Workers in this allocation queue will be by default created with a time limit equal to the time limit of the queue
(unless overridden with [Worker time limit](#worker-time-limit)).

!!! Important
    If you specify a [time request](../jobs/jobs.md#time-management)
    for a task, you should be aware that the time limit for the allocation queue **should be larger than the time request**
    if you want to run this task on workers created by this allocations queue, because it will always take some time before
    a worker is fully initialized. For example, if you set `--time-request 1h` when submitting a task, and `--time-limit 1h`
    when creating an allocation queue, this task will never get scheduled on workers from this queue.

#### Backlog
Format: `--backlog <count>`

How many allocations should be queued (waiting to be started) in PBS/Slurm at any given time. Has to be a positive integer.

#### Maximum number of workers per allocation
Format: `--max-workers-per-alloc <count>`

The maximum number of workers that will be requested in each allocation. Note that if there is not enough computational demand,
the automatic allocator can create allocations with a smaller number of workers.

!!! notice

    If you use [multi-node](../jobs/multinode.md) tasks, you will probably want to use this parameter to set the maximum
    number of workers in a queue to the size of your [groups](../jobs/multinode.md#groups).

Note that the number of workers always corresponds to the number of requested nodes, as the allocator
always creates a single worker per node in a single allocation request.

#### Maximum worker count
Format: `--max-worker-count <count>`

Maximum number of workers that can be queued or running in the allocation queue. The total amount of workers will be usually
limited by the manager (PBS/Slurm), but you can use this parameter to make the limit smaller, for example if you also want
to manage allocations outside HyperQueue.

#### Worker resources
You can specify [CPU](../jobs/cresources.md) and [generic](../jobs/resources.md) resources of workers spawned by the
allocation queue. The name and syntax of these parameters is the same as when you create a
[worker manually](../jobs/resources.md#worker-resources):

=== "PBS"

    ```bash
    $ hq alloc add pbs --time-limit 1h --cpus 4x4 --resource "gpus/nvidia=range(1-2)" -- -qqprod -AAccount1
    ```

=== "Slurm"

    ```bash
    $ hq alloc add slurm --time-limit 1h --cpus 4x4 --resource "gpus/nvidia=range(1-2)" -- --partition=p1
    ```
    
    If you do not pass any resources, they will be detected automatically (same as it works with
    `hq worker start`).

#### Idle timeout
Format[^1]: `--idle-timeout <duration>`

Sets the [idle timeout](worker.md#idle-timeout) for workers started by the allocation queue. We suggest that you do not
use a long duration for this parameter, as it can result in wasting precious allocation time.

#### Worker start command
Format: `--worker-start-cmd <cmd>`

Specifies a shell command that will be executed on each allocated node just before a worker is started
on that node. You can use it e.g. to initialize some shared environment for the node, or to load software modules.

#### Worker stop command
Format: `--worker-stop-cmd <cmd>`

Specifies a shell command that will be executed on each allocated node just after the worker stops on
that node. You can use it e.g. to clean up a previously initialized environment for the node.

!!! Warning

    The execution of this command is best-effort! It is not guaranteed that the command will always be executed. For example,
    PBS/Slurm can kill the allocation without giving HQ a chance to run the command.

#### Worker time limit
Format[^1]: `--worker-time-limit <duration>`

Sets the time limit of workers spawned by the allocation queue. After the time limit expires, the worker will be stopped.
By default, the worker time limit is set to the time limit of the allocation queue. But if you want, you can shorten it
with this flag to make the worker exit sooner, for example to give more time for a [worker stop command](#worker-stop-command)
to execute.

!!! Note

    This command is not designed to stop workers early if they have nothing to do. This functionality is
    provided by [idle timeout](#idle-timeout).

#### Name
Format: `--name <name>`

Name of the allocation queue. It will be used to name allocations submitted to the job manager. Serves for debug purposes
only.

[^1]: You can use various [shortcuts](../cli/shortcuts.md#duration) for the duration value.

## Behavior
The automatic allocator will submit allocations to make sure that there are is a specific number
of allocations waiting to be started by the job manager. This number is called **backlog** and you
can [set it](#parameters) when creating the queue.

For example, if **backlog** was set to `4` and there is currently only one allocation queued into the job manager,
the allocator would queue three more allocations.

The backlog serves to pre-queue allocations, because it can take some time before the job manager
starts them, and also as a load balancing factor, since it will allocate as many resources as the job manager allows.

!!! note

    The **backlog** value does not limit the number of running allocations, only the number of queued allocations.

!!! warning

    Do not set the `backlog` to a large number to avoid overloading the job manager.

When an allocation starts, a HyperQueue [worker](worker.md) will start and connect to the HyperQueue
server that queued the allocation. The worker has the [idle timeout](worker.md#idle-timeout) set to
five minutes, therefore it will terminate if it doesn't receive any new tasks for five minutes.

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

To mitigate the case of incorrectly entered allocation parameters, HQ will also try to submit a test
allocation (do a "dry run") into the target HPC job manager when you add a new allocation queue.
If the test allocation fails, the queue will not be created. You can avoid this behaviour by passing
the `--no-dry-run` flag to `hq alloc add`.

There are also additional safety limits. If `10` allocations in a succession fail to be submitted,
or if `3` allocations that were submitted fail during runtime in a succession, the corresponding
allocation queue will be automatically removed.

### Dry-run command
To test whether PBS/Slurm will accept the submit parameters that you provide to the auto allocator
without creating an allocation queue, you can use the `dry-run` command. It accepts the same
parameters as `hq alloc add`, which it will use to immediately submit an allocation and print
any encountered errors.

```bash
$ hq alloc dry-run pbs --timelimit 2h -- q qexp -A Project1
```

If the allocation was submitted successfully, it will be canceled immediately to avoid wasting resources.

### Finding information about allocations
- **[`Basic queue information`](#display-information-about-an-allocation-queue)** This command will
  show you details about allocations created by the automatic allocator.
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

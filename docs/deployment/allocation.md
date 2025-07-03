**Automatic allocation** is one of the core features of HyperQueue. When you run HyperQueue on an HPC cluster, it allows
you to autonomously ask the allocation manager (PBS/Slurm) for computing resources and spawn HyperQueue [workers](worker.md)
on the provided nodes.

!!! Tip

    You can find CLI reference for HQ autoalloc commands [here](cli:hq.alloc).

Using this mechanism, you can submit computations into HyperQueue without caring about the underlying PBS/Slurm allocations.

!!! Note "Job vs allocation terminology"

    It is common to use the term "job" for allocations created by an HPC allocation manager, such as PBS or Slurm, which are used to
    perform computations on HPC clusters. However, HyperQueue also uses the term "job" for [task graphs](../jobs/jobs.md).

    To differentiate between these two, we will refer to jobs created by PBS or Slurm as `allocations`. We will also refer
    to PBS/Slurm as an `allocation manager`.

## Allocation queue
To enable automatic allocation, you have to create at least one `Allocation queue`. The queue describes a specific configuration that will be used by HyperQueue to request computing resources from the allocation manager on your behalf.

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

You have to specify the `--time-limit` parameter to tell HyperQueue the walltime of the requested allocations (the upper limit on their duration). The automatic allocator will automatically pass this walltime to PBS/Slurm, along with the number of nodes that should be spawned in each allocation. Other parameters, such as project credentials, account ID, queue/partition name or the number of requested CPU or GPU cores per node have to be specified manually by the user using the trailing arguments.

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
In addition to arguments that are passed to `qsub`/`sbatch`, you can also use several other command line options when creating a new allocation queue.

!!! tip
    We recommend you to specify the worker resources that will be available on workers spawned in the given allocation queue using the `--cpus` and `--resource` flags as precisely as possible. It will help the automatic allocator create the first allocation more accurately.

#### Time limit
- Format[^1]: **`--time-limit <duration>`**

Sets the walltime of created allocations.

This parameter is **required**, as HyperQueue must know the duration of the individual allocations.
Make sure that you pass a time limit that does not exceed the limit of the PBS/Slurm queue
that you intend to use, otherwise the allocation submissions will fail. You can use the
[`dry-run` command](#dry-run-command) to debug this.

By default, workers will use a [time limit](./worker.md#time-limit) equal to the time limit of the queue (unless overridden).

!!! Important
    If you specify a [time request](../jobs/jobs.md#time-management)
    for a task, you should be aware that the time limit for the allocation queue **should be larger than the time request**
    if you want to run this task on workers created by this allocations queue, because it will always take some time before
    a worker is fully initialized. For example, if you set `--time-request 1h` when submitting a task, and `--time-limit 1h`
    when creating an allocation queue, this task will never get scheduled on workers from this queue, because the worker will
    get started with an actual time limit of e.g. `59m 58s`.

#### Backlog
- Format: `--backlog <count>`
- Default: `1`

Maximum number of allocations that should be queued (waiting to be started) in PBS/Slurm at any given time. Has to be a positive integer.

!!! note

    The **backlog** value does not limit the number of running allocations, only the number of queued allocations.

!!! warning

    Do not set the `backlog` to a large number to avoid potentially overloading the allocation manager.

#### Maximum number of workers per allocation
- Format: `--max-workers-per-alloc <count>`
- Default: `1`

The maximum number of workers that will be requested in each allocation. Note that if there is not enough computational demand,
the automatic allocator can create allocations with a smaller number of workers.

!!! note

    If you use [multi-node](../jobs/multinode.md) tasks, you will probably want to use this parameter to set the maximum
    number of workers in a queue to the size of your [groups](../jobs/multinode.md#groups).

Note that the number of workers always corresponds to the number of requested nodes, as the allocator
always creates a single worker per node in a single allocation request.

#### Maximum worker count
- Format: `--max-worker-count <count>`
- Default: unlimited

Maximum number of workers that can be queued or running across all allocations managed by the allocation queue. The total amount of workers will be usually limited by the manager (PBS/Slurm), but you can use this parameter to make the limit smaller, for example if you also want to manage allocations outside HyperQueue.

#### Minimal utilization
- Format: `--min-utilization <ratio>`
- Default: `0.0`

Minimal utilization can be used to avoid creating allocations if there is not enough computational demand at the moment. If the scheduler thinks that it can currently make use of `N` of worker resources (on a range of `0.0 - 1.0`) in a single allocation of this queue, `min-utilization` has to be at least `N`, otherwise the allocation will not be created.

It has to be a floating point number between 0.0 and 1.0.

The default minimal utilization is `0`, which means that an allocation will be created if the scheduler thinks that it can use any (non-zero) amount of resources of worker(s) in the allocation.

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

As noted [above](#parameters), we recommend you to set these resources as precisely as possible, if you know them.

#### Idle timeout
- Format[^1]: `--idle-timeout <duration>`
- Default: `5m`

Sets the [idle timeout](worker.md#idle-timeout) for workers started by the allocation queue. We suggest that you do not
use a long duration for this parameter, as it can result in wasting precious allocation time.

#### Worker start command
- Format: `--worker-start-cmd <cmd>`

Specifies a shell command that will be executed on each allocated node just before a worker is started
on that node. You can use it e.g. to initialize some shared environment for the node, or to load software modules.

#### Worker stop command
- Format: `--worker-stop-cmd <cmd>`

Specifies a shell command that will be executed on each allocated node just after the worker stops on
that node. You can use it e.g. to clean up a previously initialized environment for the node.

!!! Warning

    The execution of this command is best-effort! It is not guaranteed that the command will always be executed. For example,
    PBS/Slurm can kill the allocation without giving HQ a chance to run the command.

#### Worker time limit
- Format[^1]: `--worker-time-limit <duration>`

Sets the time limit of workers spawned by the allocation queue. After the time limit expires, the worker will be stopped.
By default, the worker time limit is set to the time limit of the allocation queue. But if you want, you can shorten it
with this flag to make the worker exit sooner, for example to give more time for a [worker stop command](#worker-stop-command)
to execute.

!!! Note

    This command is not designed to stop workers early if they have nothing to do. This functionality is
    provided by [idle timeout](#idle-timeout).

#### Name
- Format: `--name <name>`

Name of the allocation queue. It will be used to name allocations submitted to the allocation manager. Serves for debug purposes
only.

[^1]: You can use various [shortcuts](../cli/shortcuts.md#duration) for the duration value.

### Automatic dry-run submission

Once you create an allocation queue, HyperQueue will first automatically create a test "dry run" allocation, to make sure that PBS/Slurm accepts the arguments that you have passed to the `hq alloc add` command. This allocation will be created in a suspended mode and it will be then immediately cancelled, so that it does not consume any resources.

You can disable this behavior using the `--no-dry-run` flag when running `hq alloc add`.

You can also create a dry-run submission [manually](#dry-run-command).

## Behavior
The (automatic) allocator submits allocations into PBS/Slurm based on current computational demand. When an allocation starts, a HyperQueue [worker](worker.md) will start and connect to the HyperQueue server that queued the allocation. The worker has the [idle timeout](worker.md#idle-timeout) set to five minutes, so it will terminate if it doesn't receive any new tasks for five minutes.

The allocator coordinates with the HyperQueue scheduler to figure out how many workers (and allocations) are needed 
to execute tasks that are currently waiting for computational resources. To determine that, the allocator needs to know the exact resources provided by workers that connect from allocations spawned by a given allocation queue. Because we cannot know these resources exactly until the first worker connects, the allocator can behave slightly differently before the first worker from a given allocation queue connects.

For example, assume that you have specified that workers in a given queue will have `4` CPU cores (using `--cpus 4` when running `hq alloc add`). Here are a few scenarios for which we show how would the allocator behave:

- If we have waiting tasks that require `8` cores, no allocation will be submitted, because workers from this queue definitely cannot compute these tasks.
- If we have waiting tasks that require `2` cores, the allocator knows that workers from this queue will be able to compute them, so it will submit as many allocations are required to compute these tasks (based on [backlog](#backlog) and other limits). 
- If we have waiting tasks that require `2` cores and `1` GPU, the allocator will first submit a single allocation, because it is possible that the spawned worker will have a GPU (which just wasn't explicitly specified by the user in `hq alloc add`). Then:
    - If the connected worker has a GPU available, the allocator will submit further allocations for these waiting tasks.
    - If the connected worker doesn't have a GPU available, the allocator will not submit further allocations to compute tasks that require a GPU.

### Rate limits

The allocator internally uses rate limiting to avoid overloading the PBS/Slurm allocation manager by spawning too many allocations too quickly.

It also uses additional safety limits. If `10` allocations in a succession fail to be submitted or if `3` allocations that were submitted fail to start in a succession, the corresponding allocation queue will be automatically [paused](#pausing-automatic-allocation).

## Pausing automatic allocation

If you want to pause the submission of new allocations from a given allocation queue, without removing the queue completely, you can use the `hq alloc pause <queue-id>` command.

If you later want to resume allocation submission, you can use the `hq alloc resume <queue-id>` command.

## Stopping automatic allocation
If you want to remove an allocation queue, use the following command:

```bash
$ hq alloc remove <queue-id>
```

When an allocation queue is removed, its queued and running allocations will be
canceled immediately.

By default, HQ will not allow you to remove an allocation queue that contains a running allocation.
If you want to force its removal, use the `--force` flag.

When the HQ server stops, it will automatically remove all allocation queues and cleanup all
allocations.

## Debugging automatic allocation
Since the automatic allocator is a "background" process that interacts with an external allocation manager, it can be challenging
to debug its behavior. To aid with this process, HyperQueue provides a dry-run allocation test and also various sources of information that can help you figuring out what is going on.

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

- **Allocation files** Each time the allocator submits an allocation into the allocation manager, it will write the submitted
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

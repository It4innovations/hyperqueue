# CPU resource management

!!! note

    In this text, we use the term **CPU** for a resource that is provided by the operating system
    (e.g. what you get from `/proc/cpuinfo`). In this meaning, it is usually a core of a physical
    CPU. In the text related to NUMA we use the term **socket** to refer to physical CPUs.


## Brief introduction

HyperQueue allows you to select how many CPU cores will be allocated for each task.
By default, each task requires a single CPU of the worker's node. This can be changed by the flag
`--cpus`.

For example, to submit a job with a task that requires 8 CPUs:

```bash
$ hq submit --cpus=8 <program_name> <args...>
```

This ensures that HyperQueue will exclusively reserve 8 CPUs for this task when it is started. This
task would thus never be scheduled on a worker that has less than 8 CPUs.

Note that this reservation exists on a logical level only. To ensure more direct mapping to physical
cores, see [pinning](#pinning) below.


## CPUs are a resource

From version 0.13.0, CPUs are managed as any other resource under name "cpus", with the following additions:

* If a task does not explicitly specify the number of cpus, then it requests 1 CPU as default.

* CPUs request can be specified by ``hq submit --cpus=X ...`` where ``--cpus=X`` is a shortcut for ``--resource cpus=X``, 
  and X can be all valid requests for a resource, including values like ``all`` or ``8 compact!``.
  (More in [Resource Management](resources.md)).

* A task may be automatically pinned to a given CPUs (see [pinning](#pinning)).

* There are some extra environmental variables for CPUs (see below).

* CPUs are automatically detected. See below for information about NUMA or Hyper Threading.

* CPUs provided by a worker can be explicitly specified via ``--cpus``, see below.


## CPU related environment variables

The following variables are created when a task is executed:

* ``HQ_CPUS`` - List of cores assigned to a task. (this is an alias for ``HQ_RESOURCE_VALUES_cpus``).
* ``HQ_PIN`` - Is set to `taskset` or `omp` (depending on the used pin mode) if the task was pinned
by HyperQueue (see below).
* ``NUM_OMP_THREADS`` -- Set to number of cores assigned for task. (For compatibility with OpenMP).
                         This option is not set when you ask for a non-integer number of CPUs.


## Pinning

By default, HQ internally allocates CPUs on a logical level.
In other words, HQ ensures that the sum of requests of concurrently running tasks does not exceed
the number of CPUs of the worker, but process assignment to cores is left to the system scheduler,
which may move processes across CPUs as it wants.

If this is not desired, especially in the case of NUMA, processes could be pinned, either manually
or automatically.

### Automatic pinning

HyperQueue can pin threads using two ways: with `taskset` or by setting `OpenMP` environment variables.
You can use the `--pin` flag to choose between these two modes.

=== "taskset"

    ```bash
    $ hq submit --pin taskset --cpus=8 <your-program> <args>
    ```

    will cause HyperQueue to execute your program like this:
    ```bash
    taskset -c "<allocated-cores>" <your-program> <args>`
    ```

=== "OpenMP"

    ```bash
    $ hq submit --pin omp --cpus=8 <your-program> <args>
    ```

    will cause HyperQueue to execute your program like this:
    ```bash
    OMP_PROC_BIND=close OMP_PLACES="{<allocated-cores>}" <your-program> <args>
    ```

If any automatic pinning mode is enabled, the environment variable `HQ_PIN` will be set.

### Manual pinning

If you want to gain full control over core pinning, you may pin the process by yourself.

The assigned CPUs are stored in the environment variable `HQ_CPUS` as a comma-delimited list
of CPU IDs. You can use utilities such as `taskset` or `numactl` and pass them `HQ_CPUS` to
pin a process to these CPUs.

!!! warning

    If you manually pin your processes, do not also use the `--pin` flag of the `submit` command.
    It may have some unwanted interferences.

Below you can find an example of a script file that pins the executed process manually using
`taskset` and `numactl`:

=== "taskset"

    ```bash
    #!/bin/bash

    taskset -c $HQ_CPUS <your-program> <args...>
    ```

=== "numactl"

    ```bash
    #!/bin/bash

    numactl -C $HQ_CPUS <your-program> <args...>
    ```

If you submit this script with `hq submit --cpus=4 script.sh`, it will pin your program to 4 CPUs
allocated by HQ.

## NUMA allocation strategy

Workers automatically detect the number of CPUs and on Linux systems they also detect their partitioning into sockets.
When a NUMA architecture is automatically detected, indexed resource with groups is used for resource "cpus".

You can then use allocation strategies for groups to specify how sockets are
allocated. They follow the same rules as normal allocation strategies;
for clarity we are rephrasing the group allocation strategies in terms of cores and sockets: 

* Compact (``compact``) - Tries to allocate cores on as few sockets as possible in the current worker state.

    ```bash
    $ hq submit --cpus="8 compact" ...
    ```

* Strict Compact (`compact!`) - Always allocates cores on as few sockets as possible for a target node.
  The task will not be executed until the requirement could be fully fulfilled.
  For example, if your worker has 4 cores per socket, and you ask for 4 CPUs, it will always be
  executed on a single socket. If you ask for 8 CPUs, it will always be executed on two sockets.

    ```bash
    $ hq submit --cpus="8 compact!" ...
    ```

    !!! tip

        You might encounter a problem in your shell when you try to specify the strict compact policy,
        because the definition contains an exclamation mark (`!`). In that case, try to wrap the policy
        in single quotes, like this:

        ```bash
        $ hq submit --cpus='8 compact!' ...
        ```

* Scatter (`scatter`) - Allocate cores across as many sockets possible, based on the currently available
  cores of a worker. If your worker has 4 sockets with 8 cores per socket, and you ask for 8 CPUs,
  then HQ will try to run the process with 2 CPUs on each socket, if possible given the currently available
  worker cores.

    ```bash
    $ hq submit --cpus="8 scatter" ...
    ```

The default policy is the `compact` policy, i.e. `--cpus=<X>` is equivalent to `--cpus="<X> compact"`.

!!! note

    Specifying a policy only has an effect if you have more than one socket (physical CPUs).
    In case of a single socket, policies are indistinguishable.


## CPU configuration

Each worker will automatically detect the number of CPUs available. On Linux systems, it will also
detect the partitioning into sockets (NUMA configuration). In most cases, it should work out of the box.
If you want to see how will a HQ worker see your CPU configuration without actually starting the worker,
you can use the [`hq worker hwdetect`](cli:hq.worker.hwdetect) command, which will print the detected CPU configuration.


### Manual specification of CPU configuration

If the automatic detection fails for some reason, or you want to manually configure the CPU 
configuration, you can use the `--cpus` flag when starting a worker. 
It is an alias for ``--resource cpus=...`` (More in [Resource Management](resources.md)), except it also allow to define ``--cpus=N`` where ``N`` is an integer; it is then interpreted as ``1xN`` in the resource definition.

Below there are some examples  of configuration that you can specify:

- Worker with 8 CPUs and a single socket.
  ```bash
  $ hq worker start --cpus=8
  ```

- Worker with 2 sockets with 12 cores per socket.
  ```bash
  $ hq worker start --cpus=2x12
  ```

- Manually specify that the worker should use the following core ids and how they are organized
  into sockets. In this example, two sockets are defined, one with 3 cores and one with 2 cores.
  ```bash
  $ hq worker start --cpus=[[2, 3, 4], [10, 14]]
  ```


### Disable Hyper Threading

If you want to detect CPUs but ignore HyperThreading then ``--no-hyper-threading`` flag can be used.
It will detect only the first virtual core of each physical core.

Example:

  ```bash
  $ hq worker start --no-hyper-threading
  ```

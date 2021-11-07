**Note:** In this text we use term **CPU** as a resource that is provided by operating system (e.g. what you get from /proc/cpuinfo). In this meaning, it is usually a core of a physical CPU. In the text related to NUMA we use term **socket** to refer to a physical CPU.


## Requesting more CPUs

By default, each task allocates a single CPU on worker's node. This can be changed by argument ``--cpus=...``.

Example: Request for a job with a task that needs 8 cpus:

```
$ hq submit --cpus=8 <program_name> <args...>
```

This ensures that 8 cpus will be exclusively reserved when this task is started. This task will never be scheduled on a worker that has less then 8 cpus.


## Requesting all CPUs

Setting ``--cpus=all`` ensures that will request all CPUs of the worker and ensures an exclusive run of the task.

```
$ hq submit --cpus=all <program_name> <args...>
```


## Pinning

By default, HQ internally allocates CPUs on logical level without pinning.
In other words, HQ ensures that the sum of requests of concurrently running tasks does not exceed the number of CPUs in the worker, but the the process placement is left on the system scheduler that may move processes across
CPUs as it wants.

If this is not desired, especially in case of NUMA, processes could be pinned, either manually or automatically.


### Automatic pinning

If you just want to pin your processes to allocated CPUs, use ``--pin`` flag.

```
$ hq submit --pin --cpus=8 <your-program> <args>
```

When an automatic pinning is enabled then the environment variable ``HQ_PIN`` is set to ``1`` in the task process.


### Manual pinning

If you want to gain a full controll over pinning processes, you may pin the process by yourself.

The assigned CPUs is stored in environment ``HQ_CPUS`` as a comma-delimited list
of CPU ids. You can use utilities as ``taskset`` or ``numactl`` and pass there  ``HQ_CPUS`` to pin a process to these CPUs.

**Warning** If you manually pin your processes, do not use ``--pin`` flag in submit command. It may have some unwanted interferences.


For example you can create a following ``script.sh`` (with executable permission)

```bash
#!/bin/bash

taskset -c $HQ_CPUS <your-program> <args...>
```

If it is submitted as ``$ hq submit --cpus=4 script.sh``
It will pin your program to 4 CPUs allocated by HQ.

In case of ``numactl``, the equivalent script is:

```bash
#!/bin/bash

numactl -C $HQ_CPUS <your-program> <args...>
```


## NUMA allocation policy

HQ currently ofsers the following allocation strategies how CPUs are allocated.
It can be specified by ``--cpus`` argument in form ``"<#cpus> <policy>"``.

Note: Specifying policy has effect only if you have more then one sockets(physical CPUs). In case of a single socket, policies are indistinguishable.

* Compact (``compact``) - Tries to allocate cores on as few sockets as possible in the current worker state.

  Example: ``hq --cpus="8 compact" ...``

* Strict Compact (``compact!``) - Always allocate cores on as few sockets as possible for a target node. The task is not executed until the requirement could not be fully fullfiled. E.g. If your worker has 4 cores per socket and you ask for 4 cpus, it will be always executed on a single socket. If you ask for 8 cpus, it will be always executed on two sockets.

  Example: ``hq --cpus="8 compact!" ...``

* Scatter (``scatter``) - Allocate cores across as many sockets possible in the current worker state. If your worker has 4 sockets with 8 cores per socket and you ask for 8 cpus than if possible in the current situation, HQ tries to run process with 2 cpus on each socket.

  Example: ``hq --cpus="8 scatter" ...``


The default policy is the compact policy, i.e. ``--cpus=XX`` is equivalent to ``--cpus="XX compact"``


## CPU requests and job arrays

Resource requests are applied to each task of job. For example, if you submit the following: ``hq --cpus=2 --array=1-10`` it will create 10 tasks where each task needs two CPUs.


## CPUs configuration

Worker automatically detect number of CPUs and on Linux system it also detects partitioning into sockets. In most cases,
it should work without need of any touch. If you want to see how is your seen by a worker without actually starting it,
you can start ``$ hq worker hwdetect`` that only prints CPUs layout.

### Manual specification of CPU configration

If automatic detection fails, or you want to manually configure set CPU configuration, you can use
``--cpus`` parameter; for example as follows:

- 8 CPUs for worker
  ``$ hq worker start --cpus=8``

- 2 sockets with 12 cores per socket
  ``$ hq worker --cpus=2x12``

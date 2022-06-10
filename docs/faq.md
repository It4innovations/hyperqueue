# FAQ
## HQ fundamentals

??? question "How does HQ work?"

    You start a HQ server somewhere (e.g. a login node or a cloud partition of a cluster). Then you
    can submit your jobs containing tasks to the server. You may have hundreds of thousands of tasks;
    they may have various CPUs and other resource requirements.
  
    Then you can connect any number of HQ workers to the server (either manually or via SLURM/PBS).
    The server will then immediately start to assign tasks to them.
  
    Workers are fully and dynamically controlled by server; you do not need to specify what tasks are
    executed on a particular worker or preconfigure it in any way.
  
    HQ provides a command line tool for submitting and controlling jobs.
  
    <p align="center">
      <img width="600" src="../imgs/schema.png">
    </p>

??? question "What is a task in HQ?"

    Task is a unit of computation. Currently, it is either the execution of an arbitrary external
    program (specified via CLI) or the execution of a single Python function (specified via our Python
    API).

??? question "What is a job in HQ?"

    Job is a collection of tasks (a task graph). You can display and manage jobs using the CLI.

??? question "How to deploy HQ?"

    HQ is distributed as a single, self-contained and statically linked binary. It allows you to start
    the server, the workers, and it also serves as CLI for submitting and controlling jobs. No other
    services are needed.

??? question "How many jobs/tasks may I submit into HQ?"

    Our preliminary benchmarks show that the overhead of HQ is around 0.1 ms per task. It should be
    thus possible to submit a job with tens or hundreds of thousands tasks into HQ.

    Note that HQ is designed for a large number of tasks, not jobs. If you want to perform a lot of
    computations, use [task arrays](jobs/arrays.md), i.e. create a job with many tasks, not many jobs
    each with a single task.

    HQ also supports [streaming](jobs/streaming.md) of task outputs into a single file.
    This avoids creating many small files for each task on a distributed file system, which improves
    scaling.

??? question "Does HQ support multi-CPU tasks?"

    Yes. You can define an arbitrary [amount of CPUs](jobs/cresources.md) for each task.
    HQ is also NUMA aware and you can select the NUMA allocation strategy.

??? question "Does HQ support job/task arrays?"

    Yes, see [task arrays](jobs/arrays.md).

??? question "Does HQ support tasks with dependencies?"

    Yes, although it is currently only implemented in the Python API, which is experimental. It is
    currently not possible to specify dependencies using the CLI.

??? question "How is HQ implemented?"

    HQ is implemented in Rust and uses Tokio ecosystem. The scheduler is work-stealing scheduler implemented in
    our project [Tako](https://github.com/spirali/tako/),
    that is derived from our previous work [RSDS](https://github.com/It4innovations/rsds).
    Integration tests are written in Python, but HQ itself does not depend on Python.

## Relation to HPC technologies

??? question "Do I need to SLURM or PBS to run HQ?"

    No. Even though HQ is designed to work smoothly on systems using SLURM/PBS, they are not required
    in order for HQ to work.

??? question "Is HQ a replacement for SLURM or PBS?"

    Definitely not. Multi-tenancy is out of the scope of HQ, i.e. HQ does not provide user isolation.
    HQ is light-weight and easy to deploy; on an HPC system each user (or a group of users that trust
    each other) may run their own instance of HQ.

??? question "Do I need an HPC cluster to run HQ?"

    No. None of functionality is bound to any HPC technology. Communication between all components
    is performed using TCP/IP. You can also run HQ locally on your personal computer.

??? question "Is it safe to run HQ on a login node shared by other users?"

    Yes. All communication is secured and encrypted. The server generates a secret file and only
    those users that have access to that file may submit jobs and connect workers. Users without
    access to the secret file will only see that the service is running.

## Relation to other task runtimes

??? question "What is the difference between HQ and Snakemake?"

    In cluster mode, Snakemake submits each Snakemake job as one HPC job into SLURM/PBS. If your jobs
    are too small, you will have to manually aggregate them to avoid exhausting SLURM/PBS resources.
    Manual job aggregation is often quite arduous and since the aggregation is static, it might also
    waste resources because of missing load balancing.
  
    In the case of HQ, you do not have to aggregate tasks. You can submit millions of small tasks to
    HQ and it will take care of assigning them dynamically to individual workers (and SLURM/PBS jobs,
    if [automatic allocation](deployment/allocation.md) is used).

<p align="center">
<img src="docs/imgs/hq.png">
</p>

![Tests](https://github.com/it4innovations/hyperqueue/actions/workflows/test.yml/badge.svg) [![DOI](https://zenodo.org/badge/349152473.svg)](https://zenodo.org/badge/latestdoi/349152473)

**HyperQueue** is a tool designed to simplify execution of large workflows (task graphs) on HPC clusters. It allows you to execute a large number of tasks in a simple way, without having to manually submit jobs into batch schedulers like Slurm or PBS. You just specify what you want to compute – HyperQueue will automatically ask for computational resources and dynamically load-balance tasks across all allocated nodes and cores. HyperQueue can also work without Slurm/PBS as a general task executor.

[Documentation](https://it4innovations.github.io/hyperqueue/)

If you find a bug or a problem with HyperQueue, please create an [issue](https://github.com/It4innovations/hyperqueue/issues).
For more general discussion or feature requests, please use our
[discussion forum](https://github.com/It4innovations/hyperqueue/discussions). If you want to chat
with the HyperQueue developers, you can use our [Zulip](https://hyperqueue.zulipchat.com/) server.

If you use HyperQueue in your research, please consider [citing it](#publications).

This image shows how HyperQueue can work on a distributed cluster that uses Slurm or PBS:

<img src="docs/imgs/architecture-bg.png" width="500" alt="Architecture of HyperQueue deployed on a Slurm/PBS cluster" />

## Features

- **Resource management**
    - Load balancing of tasks across all available (HPC) resources
    - Automatic submission of Slurm/PBS jobs on behalf of the user
    - Complex and arbitrary task resource requirements (# of cores, GPUs, memory, FPGAs, ...)
      - Fractional resources (task needs `0.5` of a GPU)
      - Resource variants (task needs `1 GPU and 4 CPU cores` or `16 CPU cores`)

- **Performance**
    - Scheduler can scale to hundreds of nodes and workers
    - Overhead per one task is below `0.1ms`
    - Allows streaming of stdout/stderr from tasks to avoid creating many small files on distributed filesystems

- **Easy deployment**
    - Provided as a single, statically linked binary without any dependencies apart from `libc`
    - No admin access to a cluster is needed for its usage

# Getting started

## Installation

* Download the latest binary distribution from this [link](https://github.com/It4innovations/hyperqueue/releases/latest).
* Unpack the downloaded archive:

  ```bash
  $ tar -xvzf hq-<version>-linux-x64.tar.gz
  ```

> If you want to try the newest features, you can also download a nightly
> [build](https://github.com/It4innovations/hyperqueue/releases/nightly).

## Submitting a simple task

* Start a server (e.g. on a login node or in a cluster partition)

  ```bash
  $ hq server start &
  ```
* Submit a job (command ``echo 'Hello world'`` in this case)

  ```bash
  $ hq submit echo 'Hello world'
  ```
* Ask for computing resources

    * Start worker manually

      ```bash
      $ hq worker start &
      ```

    * Automatic submission of workers into PBS/SLURM

      - PBS:

        ```bash
        $ hq alloc add pbs --time-limit 1h -- -q <queue>
        ```
      - Slurm:

        ```bash
        $ hq alloc add slurm --time-limit 1h -- -p <partition>
        ```

    * Manual request in PBS

      - Start worker on the first node of a PBS job

        ```bash
        $ qsub <your-params-of-qsub> -- hq worker start
        ```
      - Start worker on all nodes of a PBS job

        ```bash
        $ qsub <your-params-of-qsub> -- `which pbsdsh` hq worker start
        ```
    * Manual request in SLURM

      - Start worker on the first node of a Slurm job

        ```bash
        $ sbatch <your-params-of-sbatch> --wrap "hq worker start"
        ```
      - Start worker on all nodes of a Slurm job

        ```bash
        $ sbatch <your-params-of-sbatch> --wrap "srun --overlap hq worker start"
        ```

* Monitor the state of jobs

  ```bash
  $ hq job list --all
  ```

## What's next?

Check out the [documentation](https://it4innovations.github.io/hyperqueue/).

# FAQ

* **How HQ works?**

  You start a HQ server somewhere (e.g. login node, cloud partition of a cluster). Then you can submit your jobs to
  the server. You may have hundreds of thousands of jobs; they may have various CPUs and other resource requirements.

  Then you can connect any number of HQ workers to the server (either manually or via SLURM/PBS). The server will then
  immediately start to assign jobs to them.

  Workers are fully and dynamically controlled by server; you do not need to specify what jobs are executed on a
  particular worker or preconfigure it in any way.

  HQ provides a command line tool for submitting and controlling jobs.

    <p align="center">
    <img width="600" src="docs/imgs/schema.png">
    </p>

* **What is a task in HQ?**

  Task is a unit of computation. Currently, it is either the execution of an arbitrary external
  program (specified via CLI) or the execution of a single Python function (specified via our Python
  API).

* **What is a job in HQ?**

  Job is a collection of tasks (a task graph). You can display and manage jobs using the CLI.

* **Do I need to use SLURM or PBS to run HQ?**

  No. Even though HQ is designed to smoothly work on systems using SLURM/PBS, they are not required for HQ to work.

* **Is HQ a replacement for SLURM or PBS?**

  Definitely not. Multi-tenancy is out of the scope of HQ, i.e. HQ does not provide user isolation. HQ is
  light-weight and easy to deploy; on an HPC system each user (or a group of users that trust each other)
  may run her own instance of HQ.

* **Do I need an HPC cluster to run HQ?**

  No. None of functionality is bound to any HPC technology. Communication between all components is performed using
  TCP/IP. You can also run HQ locally.

* **What operating systems does HQ support?**

  HyperQueue currently only officially supports Linux (Ubuntu, Debian, CentOS, etc.). It might be possible to
  compile it for other operating systems, however we do not provide any support nor promise to fix any bugs for other
  operating systems.

* **Is it safe to run HQ on a login node shared by other users?**

  Yes. All communication is secured and encrypted. The server generates a secret file and only those users that have
  access to it file may submit jobs and connect workers. Users without access to the secret file will only see that the
  service is running.

* **How many jobs/tasks may I submit into HQ?**

  Our preliminary benchmarks show that the overhead of HQ is around 0.1 ms per task. It should be
  thus possible to submit a job with tens or hundreds of thousands tasks into HQ.

  Note that HQ is designed for a large number of tasks, not jobs. If you want to perform a lot of
  computations, use [task arrays](jobs/arrays.md), i.e. create a job with many tasks, not many jobs
  each with a single task.

  HQ also supports [streaming](jobs/streaming.md) of task outputs into a single file.
  This avoids creating many small files for each task on a distributed file system, which improves
  scaling.

* **Does HQ support multi-CPU jobs?**

  Yes. You can define an arbitrary amount of cores for each task. HQ is also NUMA aware
  and you can select the allocation strategy.

* **Does HQ support job arrays?**

  Yes, see [task arrays](https://it4innovations.github.io/hyperqueue/stable/jobs/arrays).

* **Does HQ support dependencies between tasks?**

  Yes.

* **How is HQ implemented?**

  HQ is implemented in Rust and the Tokio async ecosystem. The scheduler is a work-stealing scheduler
  implemented in our project [Tako](https://github.com/spirali/tako/),
  which is derived from our previous work [RSDS](https://github.com/It4innovations/rsds).
  Integration tests are written in Python, but HQ itself does not depend on Python.

You can find more frequently asked questions [here](https://it4innovations.github.io/hyperqueue/stable/faq).

# HyperQueue team

We are a group of researchers working at [IT4Innovations](https://www.it4i.cz/), the Czech National
Supercomputing Center. We welcome any outside contributions.

# Publications

- [HyperQueue: Efficient and ergonomic task graphs on HPC clusters](https://www.sciencedirect.com/science/article/pii/S2352711024001857)
  (paper @ SoftwareX journal)
- [HyperQueue: Overcoming Limitations of HPC Job Managers](https://sc21.supercomputing.org/proceedings/tech_poster/tech_poster_pages/rpost104.html)
  (poster @ SuperComputing'21)

If you want to cite HyperQueue, you can use the following BibTex entry:

```bibtex
@article{hyperqueue,
  title = {HyperQueue: Efficient and ergonomic task graphs on HPC clusters},
  journal = {SoftwareX},
  volume = {27},
  pages = {101814},
  year = {2024},
  issn = {2352-7110},
  doi = {https://doi.org/10.1016/j.softx.2024.101814},
  url = {https://www.sciencedirect.com/science/article/pii/S2352711024001857},
  author = {Jakub Beránek and Ada Böhm and Gianluca Palermo and Jan Martinovič and Branislav Jansík},
  keywords = {Distributed computing, Task scheduling, High performance computing, Job manager}}
}
```

# Acknowledgement

* This work was supported by the LIGATE project. This project has received funding from the European High-Performance Computing Joint Undertaking (JU) under grant agreement No 956137. The JU receives support from the European Union’s Horizon 2020 research and innovation programme and Italy, Sweden, Austria, the Czech Republic, Switzerland.

* This work was supported by the Ministry of Education, Youth and Sports of the Czech Republic through the e-INFRA CZ (ID:90140).

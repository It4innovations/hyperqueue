??? question "How does HQ work?"

    You start a HQ server somewhere (e.g. a login node or a cloud partition of a cluster). Then you can submit your jobs to
    the server. You may have hundreds of thousands of jobs; they may have various CPUs and other resource requirements.
  
    Then you can connect any number of HQ workers to the server (either manually or via SLURM/PBS). The server will then
    immediately start to assign jobs to them.
  
    Workers are fully and dynamically controlled by server; you do not need to specify what jobs are executed on a
    particular worker or preconfigure it in any way.
  
    HQ provides a command line tool for submitting and controlling jobs.
  
    <p align="center">
      <img width="600" src="../imgs/schema.png">
    </p>

??? question "What is a job in HQ"

    Right now, we support running arbitrary external programs or bash scripts. We plan to support Python defined
    workflows (with a Dask-like API).

??? question "How to deploy HQ?"

    HQ is distributed as a single, self-contained and statically linked binary. It allows you to start the server, the
    workers, and it also serves as CLI for submitting and controlling jobs. No other services are needed.
    (See example below).

??? question "Do I need to SLURM or PBS to run HQ?"

    No. Even though HQ is designed to smoothly work on systems using SLURM/PBS, they are not required for HQ to work.

??? question "Is HQ a replacement for SLURM or PBS?"

    Definitely no. Multi-tenancy is out of the scope of HQ, i.e. HQ does not provide user isolation. HQ is
    light-weight and easy to deploy; on a HPC system each user (or a group of users that trust each other) may run her own
    instance of HQ.

??? question "Do I need an HPC cluster to run HQ?"

    No. None of functionality is bound to any HPC technology. Communication between all components is performed using
    TCP/IP. You can also run HQ locally.

??? question "Is it safe to run HQ on a login node shared by other users?"

    Yes. All communication is secured and encrypted. The server generates a secret file and only those users that have
    access to it file may submit jobs and connect workers. Users without access to the secret file will only see that the
    service is running.

??? question "What is the difference between HQ and Snakemake?"

    In cluster mode, Snakemake submits each Snakemake job as one job into SLURM/PBS. If your jobs are too small, you will
    have to manually aggregate them to avoid exhausting SLURM/PBS resources. Manual job aggregation is often quite arduous
    and since the aggregation is static, it might also waste resources because of poor load balancing.
  
    In the case of HQ, you do not have to aggregate jobs. You can submit millions of small jobs to HQ
    and it will take care of assigning them dynamically to individual SLURM/PBS jobs and workers.

??? question "How many jobs may I submit into HQ?"

    Our preliminary benchmarks show that overhead of HQ is around 0.1 ms per task.
    We also support streaming of task outputs into a single file (this file contains metadata, hence outputs for each task can be filtered or ordered).
    This avoids creating many small files for each task on a distributed file system that may have a large impact on scaling.

??? question "Does HQ support multi-CPU jobs?"

    Yes. You can define number of CPUs for each job. HQ is NUMA aware and you can choose the allocation strategy.

??? question "Does HQ support job arrays?"

    Yes.

??? question "Does HQ support jobs with dependencies?"

    Not yet. It is actually implemented in the scheduling core, but it has
    no user interface yet.
    But we consider it as a crucial must-have feature.

??? question "How is HQ implemented?"

    HQ is implemented in Rust and uses Tokio ecosystem. The scheduler is work-stealing scheduler implemented in
    our project [Tako](https://github.com/spirali/tako/),
    that is derived from our previous work [RSDS](https://github.com/It4innovations/rsds).
    Integration tests are written in Python, but HQ itself does not depend on Python.

??? question "Who stands behind HyperQueue?"

    We are a group of researchers interested in distributed computing and machine learning. We operate at
    [IT4Innovations](https://www.it4i.cz/), the Czech National Supercomputing Center. We welcome any contribution from
    outside.

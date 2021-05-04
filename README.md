<p align="center">
<img width="300" src="docs/imgs/hq.png">
</p>

**Warning** this project is under heavy development; it works on a basic level, but we are still working on some
important features.

[[Documentation]](https://spirali.github.io/hyperqueue/)

**HyperQueue** (HQ) lets you build a computation plan consisting of a large amount of tasks (so-called qjobs) and then
execute it transparently over a system like SLURM/PBS. It dynamically groups qjobs into SLURM/PBS jobs and distributes
them to fully utilize allocated notes. You thus don't have to care about qjob granularity and crucially, you don't have
to manually aggregate qjobs into SLURM/PBS jobs.

## Features

- **Performant**
    - The inner scheduler can scale to hundreds of nodes
    - Qjob granularities can range from milliseconds to hours
- **Easy deployment**
    - HQ is distributed as a single, statically linked binary without any dependencies
    - No admin access to a cluster is needed

## FAQ

* **How HQ works?**

  You start a HQ server somewhere (e.g. login node, cloud partition of a cluster). Then you can submit your qjobs to
  the server. You may have hundreds of thousands of qjobs; they may have various CPUs and other resource requirements.

  Then you can connect any number of HQ workers to the server (either manually or via SLURM/PBS). The server will then
  immediately start to assign qjobs to them.

  Workers are fully and dynamically controlled by server; you do not need to specify what qjobs are executed on a
  particular worker or configure it in any way.

  HQ provides a command line tool for submitting and controlling qjobs.

    <p align="center">
    <img width="600" src="docs/imgs/schema.png">
    </p>

* **What is a qjob in HQ?**

  Right now, we support running arbitrary external programs or bash scripts. We plan to support Python defined
  workflows (with a Dask-like API).

* **How to deploy HQ?**

  HQ is distributed as a single, self-contained and statically linked binary. It allows you to start the server, the
  workers, and it also serves as CLI for submitting and controlling qjobs. No other services are needed.
  (See example below).

* **Do I need to SLURM or PBS to run HQ?**

  No. Even though HQ is designed to smoothly work on systems using SLURM/PBS, they are not required for HQ to work.

* **Is HQ a replacement for SLURM or PBS?**

  Definitely no. Multi-tenancy is out of the scope of HQ, i.e. HQ does not provide user isolation or fairness. HQ is
  light-weight and easy to deploy; on a HPC system each user (or a group of users that trust each other) may run her own
  instance of HQ.

* **Do I need an HPC cluster to run HQ?**

  No. None of functionality is bound to any HPC technology. Communication between all components is performed using
  TCP/IP. You can also run HQ locally.

* **Is it safe to run HQ on a login node shared by other users?**

  Yes. All communication is secured and encrypted. The server generates a secret file and only those users that have
  access to it file may submit jobs and connect workers. Users without access to the secret file will only see that the
  service is running.

* **What is the difference between HQ and Snakemake?**

  In cluster mode, Snakemake submits each Snakemake job as one job into SLURM/PBS. If your jobs are too small, you will
  have to manually aggregate them to avoid exhausting SLURM/PBS resources. Manual job aggregation is often quite arduous
  and since the aggregation is static, it might also waste resources because of poor load balancing.
  
  If you use HQ, you will not have to aggregate jobs. You can submit millions of (almost) arbitrarily small qjobs to HQ
    and it will take care of assigning them dynamically to individual SLURM/PBS jobs and workers.

* **How many qjobs may I submit into HQ?**

  We did not benchmark HQ itself yet, but our backend library used in HQ handles hundreds of thousands qjob with `1ms`
  overhead per qjob with dependencies and around `0.1ms` per qjob without dependencies.

* **Does HQ support qjobs with dependencies?**

  Not yet, but we consider it as a first class feature. It is actually implemented in the scheduling core, but it has
  no user interface yet.

# Getting started

## Installation

* Download latest binary distribution from this [link](https://github.com/spirali/hyperqueue/releases/latest).
* Unpack the downloaded archive:

  ``$ tar -xvzf hq-<version>-linux-x64.tar.gz``

## Simple usage

* Start server (e.g. on a login node or in a cluster partition)

  ``$ hq server start``

* Submit a job (command ``echo 'Hello world'`` in this case)

  ``$ hq submit echo 'Hello world'``

* Ask for computing resource

    * Start worker manually

      ``$ hq worker start``

    * Automatic resource request
      [Not implemented yet]

    * Manual request in PBS

      ``$ qsub <your-params-of-qsub> -- hq worker start``

    * Manual request in SLURM

      ``sbatch <your-params-of-sbatch> -- hq worker start``

* Monitor the state of qjobs

  ``$ hq jobs``

# What next?

[Documentation](https://spirali.github.io/hyperqueue/)

# Roadmap

## 0.1

* [x] Basic worker management
* [x] Submitting, observing and canceling simple qjobs
* [x] Encryption and authentication

## 0.2

* [ ] Qjobs that take more than 1 cpu
* [ ] Job arrays

## 0.3

* [ ] Time constraints
* [ ] Priorities

## Next releases (unordered)

* [ ] API for dependencies
* [ ] Generic resource management
* [ ] Python API
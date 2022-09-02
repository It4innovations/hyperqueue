# Comparison with other task runtimes
There is a large number of task runtimes, so we cannot list all of them here. Below you can find
a selection of other task runtimes that we have experience with and/or that are somehow relevant for
HyperQueue.

## Dask
[Dask](https://www.dask.org/) is a task runtime that is very popular within the Python community,
which allows executing arbitrary task graphs composed of Python functions on a distributed cluster.
It also supports distributing code using `numpy` or `pandas` compatible API.

While Dask by itself does not interact with PBS or Slurm, you can use
[Dask-JobQueue](https://jobqueue.dask.org/en/latest/) to make it operate in a similar fashion as
HyperQueue - with the centralized server running on a login node and the workers running on compute
nodes. Dask does not support arbitrary [resource requirements](jobs/resources.md) and since it is
written in Python, it can [have problems](https://arxiv.org/abs/2010.11105) with scaling to very large
task graphs.

If your use-case is primarily Python-based, you should definitely give Dask a try, it's a great tool.

## SnakeMake
[SnakeMake](https://snakemake.readthedocs.io/en/stable/) is a workflow execution system that focuses
on scientific reproducibility. It lets users specify computational workflows using a DSL that combined
configuration files and Python. It can operate both as a meta-scheduler (outside of PBS/Slurm) and
also as a classical task runtime within a PBS/Slurm job.

With SnakeMake, you can submit a workflow either using a task-per-job model (which has high overhead)
or you can partition the workflow into several jobs, but in that case SnakeMake will not provide load
balancing across these partitions. HyperQueue allows you to submit large workflows without partitioning
them manually in any way, as the server will dynamically load balance the tasks onto workers from different
PBS/Slurm allocations.

Since SnakeMake workflows are defined in configuration files, it's a bit more involved to run
computations in SnakeMake than in HyperQueue. On the other hand, SnakeMake lets you define more
complex workflows with improved traceability and reproducibility.

## Merlin
[Merlin](https://github.com/LLNL/merlin) is a workflow execution system focused on HPC-scale
machine learning workflows. It has a relatively similar architecture to HyperQueue, although it
uses configuration files rather than CLI for specifying jobs.

<div style="display: flex; justify-content: center;">
  <img src="imgs/hq.png">
</div>

**HyperQueue** is a tool designed to simplify execution of large workflows (task graphs) on HPC clusters. It allows you to execute a large number of tasks in a simple way, without having to manually submit jobs into batch schedulers like Slurm or PBS. You just specify what you want to compute – HyperQueue will automatically ask for computational resources and dynamically load-balance tasks across all allocated nodes and cores. HyperQueue can also work without Slurm/PBS as a general task executor.

If you use HyperQueue in your research, please
consider [citing it](https://www.sciencedirect.com/science/article/pii/S2352711024001857).

## Useful links
- [Installation](installation.md)
- [Quick start](quickstart.md)
- [Python API](python/index.md)
- [Command-line interface reference](cli-reference)
- [Repository](https://github.com/It4innovations/hyperqueue)
- [Discussion forum](https://github.com/It4innovations/hyperqueue/discussions)
- [Zulip (chat platform)](https://hyperqueue.zulipchat.com/)

## Features
**Resource management**

- Batch jobs are submitted and managed [automatically](deployment/allocation.md)
- Computation is distributed amongst all allocated nodes and cores
- Tasks can specify complex [resource requirements](jobs/cresources.md)
    - Non-fungible resources (tasks are assigned specific resources, e.g. a GPU with ID `1`)
    - Fractional resources (tasks can require e.g. `0.5` of a GPU)
    - Resource variants (tasks can require e.g. `1 GPU and 4 CPU cores` OR `16 CPU cores`)
    - Related resources (tasks can require e.g. `4 CPU cores in the same NUMA node`)

**Performance**

- Scales to millions of tasks and hundreds of nodes
- Overhead per task is around 0.1 ms
- Task output can be [streamed](jobs/streaming.md) to a single file to avoid overloading distributed filesystems

**Simple user interface**

- Task graphs can be defined via a CLI, TOML workflow files or a Python API
- Cluster utilization can be monitored with a real-time dashboard

**Easy deployment**

- Provided as a single, statically linked [binary](installation.md) without any runtime dependencies
- No admin access to a cluster is needed for its usage

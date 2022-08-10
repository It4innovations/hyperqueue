<div style="display: flex; justify-content: center;">
  <img src="imgs/hq.png">
</div>

**HyperQueue** is a tool designed to simplify execution of large workflows on HPC clusters. It allows you to execute a
large number of tasks in a simple way, without having to manually submit jobs into batch schedulers like PBS or Slurm.
You just specify what you want to compute – HyperQueue will automatically ask for computational resources and dynamically
load-balance tasks across all allocated nodes and cores.

## Useful links
- [Installation](installation.md)
- [Quick start](quickstart.md)
- [Python API](python)
- [Repository](https://github.com/It4innovations/hyperqueue)
- [Discussion forum](https://github.com/It4innovations/hyperqueue/discussions)
- [Zulip (chat platform)](https://hyperqueue.zulipchat.com/)

## Features
**Resource management**

- Batch jobs are submitted and managed [automatically](deployment/allocation.md)
- Computation is distributed amongst all allocated nodes and cores
- Tasks can specify [resource requirements](jobs/cresources.md) (# of cores, GPUs, memory, ...)

**Performance**

- Scales to millions of tasks and hundreds of nodes
- Overhead per task is around 0.1 ms
- Task output can be [streamed](jobs/streaming.md) to a single file to avoid overloading distributed filesystems

**Simple deployment**

- *HQ* is provided as a single, statically linked [binary](installation.md) without any dependencies
- No admin access to a cluster is needed

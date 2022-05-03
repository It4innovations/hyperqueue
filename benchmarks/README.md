# Benchmark suite
This directory contains a framework for running various benchmarks.

It has support for spawning a distributed cluster for various tools (HQ, SnakeMake, ...), along with
node monitoring and profiling. Some features are only available for HyperQueue clusters.

The results of benchmarks are stored into JSON files, which can be used to generate HTML dashboards.

## Quick start
The benchmarks are meant to be launched from Python code. You can find some examples in `main.py`.
To compare HyperQueue with zero-worker and with normal worker, you can run:
```bash
$ python main.py compare-zw
```
The results will be stored into `benchmarks/zw`.

## Available profilers
You can attach various profilers to the HyperQueue server or the workers. Use the `server_profilers`
and/or `worker_profilers` attribute of `HqClusterInfo`.

### Flamegraph (`FlamegraphProfiler`)
Uses `perf` for stack sampling, results are rendered as a flamegraph.

### Perf events (`PerfEventsProfiler`)
Uses `perf stat` to gather various CPU performance events.

### Cachegrind (`CachegrindProfiler`)
Uses Cachegrind to instrument the profiled binary. The results can be visualized e.g. using KCacheGrind.
Note that using Cachegrind can slow down the execution by orders of magnitude.

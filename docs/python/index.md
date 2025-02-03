# Python API
To provide greater flexibility and support use-cases that are difficult to express using the CLI
such as dynamically submitting tasks when some part is finished.
Python API covers all task definition including all options available through Job Definition File
(dependencies between tasks, resource variants, etc)

You can find the HyperQueue Python API reference [here](apidoc).

## Requirements
To use the Python API, you will need at least Python 3.9 and some dependencies that will be installed
automatically using pip.

## Installation
You can install the HyperQueue Python API from `PyPi` with the following command:

```bash
$ python3 -m pip install hyperqueue
```

The Python package contains a pre-compiled version of HyperQueue, so you do not have to download `hq`
manually if you just want to use the Python API.

!!! warning

    The Python API is currently distributed only for the `x86-x64` architecture. If you need a build
    for another architecture, please
    [contact us](https://github.com/It4innovations/hyperqueue/issues/new) on GitHub.

    You can also build the Python package manually from our GitHub repository, but you will need to
    install a Rust toolchain for that.

## Quick start
Here is a minimal code example that spawns a local HyperQueue cluster and uses it to submit
a simple job:

```python
from hyperqueue import Job, LocalCluster

# Spawn a HQ server
with LocalCluster() as cluster:
    # Add a single HyperQueue worker to the server
    cluster.start_worker()

    # Create a client and a job
    client = cluster.client()
    job = Job()

    # Add a task that executes `ls` to the job
    job.program(["ls"])

    # Submit the job
    submitted = client.submit(job)

    # Wait until the job completes
    client.wait_for_jobs([submitted])
```

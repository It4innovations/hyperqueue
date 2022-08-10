# Client
To submit [jobs](../jobs/jobs.md) using the Python API, you first need to create
a [`Client`](hyperqueue.client.Client) that connects to a running HyperQueue cluster. You have two
options of deploying the cluster. Once you have an instance of a `Client`, you can use it to
[submit](submit.md) a job.

## Using external deployment
If you want to run the HyperQueue infrastructure on a distributed cluster or you want to use
[automatic allocation](../deployment/allocation.md), then [deploy](../deployment/index.md)
HyperQueue in any of the supported ways and then pass
the [server directory](../deployment/server.md#server-directory) to the `Client`:

```python
from hyperqueue import Client

client = Client("/home/user/.hq-server/hq-current")
```

If you have used the default server directory and the server is deployed on a file-system shared by
the node that executes the Python code, you can simply create an instance of a `Client` without
passing any parameters.

## Using a local cluster
You can use the [`LocalCluster`](hyperqueue.cluster.LocalCluster)
class to spawn a HyperQueue server and a set of workers directly on your local machine.
This functionality is primarily intended for local prototyping and debugging, but it can also be
used for actual computations for simple use-cases that do not require a distributed deployment of
HyperQueue.

When you create the cluster, it will initially only start the HyperQueue server. To connect workers
to it, use the [`start_worker`](hyperqueue.cluster.LocalCluster#f_start_worker) method.

```python
from hyperqueue import LocalCluster
from hyperqueue.cluster import WorkerConfig

with LocalCluster() as cluster:
    # Add a worker with 4 cores to the cluster
    cluster.start_worker(WorkerConfig(cores=4))

    # Create a client connected to the cluster
    client = cluster.client()
```

!!! tip

    You can use `LocalCluster` instances as context managers to make sure that the
    cluster is properly cleaned up at the end of the `with` block.

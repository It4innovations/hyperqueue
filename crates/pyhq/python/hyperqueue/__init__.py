"""
This is the Python API of HyperQueue.

Important classes:

* [`Client`](`hyperqueue.client.Client`) serves for connecting to a HyperQueue server.
* [`LocalCluster`](`hyperqueue.cluster.LocalCluster`) can be used to spawn a local HyperQueue
cluster.
* [`Job`](`hyperqueue.job.Job`) describes a job containing a directed acyclic graph of tasks.
It can be submitted using a client.
"""

# Re-exports
from .client import Client  # noqa: F401
from .cluster import LocalCluster  # noqa: F401
from .job import Job  # noqa: F401
from .ffi import get_version  # noqa: F401

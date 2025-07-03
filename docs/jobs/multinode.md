

!!! warning

    Multi-node support is now in the experimental stage.
    The core functionality is working, but some features
    may be limited and quality of scheduling may vary.


Multi-node tasks are tasks that spreads across multiple nodes.
Each node reserved for such task is exclusively reserved, i.e. no other 
tasks may run on such nodes.

A job with multi-node task can be specified by ``--nodes=X`` option.

An example of a job with multi-node task asking for 4 nodes:

```bash
$ hq submit --nodes 4 test.sh
```

When the task is started, four nodes are assigned to this task.
One of them is chosen as "root" node where ``test.sh`` is started.

Node names of all assigned nodes can be found in file which path is in
environmental variable ``HQ_NODE_FILE``. Each line is a node name.
The first line is always the root node.
The node is a short hostname, i.e. hostname stripped by a suffix after first "."
(e.g. if a hostname of worker is "cn690.karolina.it4i.cz" then node name is "cn690").
Many HPC applications use only short hostnames, hence we provide them as default.

If you need a full hostnames, there is file which name is written in ``HQ_HOST_FILE`` and it has the same meaning
as ``NQ_NODE_FILE`` but contains the full node hostnames without stripping.

Note: Both files are placed in task directory; therefore, a multi-node tasks always enables task directory (``--task-dir``).

If a multinode task is started, HQ also creates variable `HQ_NUM_NODES` that
holds the number of nodes assigned to a task (i.e. the number of lines of the node file)


## Groups

A multi-node task is started only on workers that belong to the same group.
By default, workers are grouped by PBS/Slurm allocations and workers outside any allocation 
are put in "default" group.

A group of a worker can be specified at the start of the worker and it may be any string. Example:

```bash
$ hq worker start --group my_group
```

## Running MPI tasks

A script that starts an MPI program in multi-node task may look like as follows:

```bash
#!/bin/sh

mpirun --node-list=$HQ_NODE_FILE ./a-program
```

If you are running SLURM you should start the MPI program as follows:

```
#!/bin/sh

srun --nodefile=$HQ_NODE_FILE --nodes=$HQ_NUM_NODES mpirun ...
```

Note: It is important to set `--nodes` otherwise the node file will not be respected.

## Automatic allocation

If you combine multi-node tasks with [automatic allocation](../deployment/allocation.md), you should
configure the maximum [number of tasks per allocation](../deployment/allocation.md#maximum-number-of-workers-per-allocation).

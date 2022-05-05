

!!! warning

    Multi-node support is now in the experimental stage.
    The core functionality is working, but some features
    may be limited and quality of scheduling may vary.
    Also auto allocation feature is not yet fully prepared for 
    multi-node tasks.


Multi-node tasks are tasks that spreads across multiple nodes.
Each node reserved for such task is exclusively reserved, i.e. no other 
tasks may run on such nodes.

A job with multi-node task can be specified by ``--nodes=X`` option.

An example of a job with multi-node task asking for 4 nodes:

```commandline
$ hq submit --nodes 4 test.sh
```

When the task is started, four nodes are assigned to this task.
One of them is chosen as "root" node where ``test.sh`` is started.

Hostnames of all assigned nodes can be found in file which path is in 
environmental variable ``HQ_NODE_FILE``. Each line is now host name.
The first line is always the root node.


Note: Multi-node tasks always enables task directory (``--task-dir``).

## Running MPI tasks

A script that starts an MPI program in multi-node task may look like as follows:

```bash
#!/bin/sh

mpirun --node-list=$HQ_NODE_FILE ./a-program
```
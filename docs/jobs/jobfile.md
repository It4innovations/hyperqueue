## Job Definition File

Job Definition File (JDF) a way how to submit a complex pipeline into a HyperQueue.
It is a [TOML](https://toml.io/) file that describes tasks of a job.
JDF provides all functionalities as command line interface of HyperQueue and also adds access to additional features: 

* *Heterogeneous tasks* -- Job may be composed of different tasks
* *Dependencies* --  Tasks may have dependencies
* *Resource request alternatives* -- Task may have alternative resource requests, e.g.: 4 cpus OR 1 cpus and 1 gpu

Note that these features are also available through Python interface.


## Minimal example

First, we create file with the following content:

```toml
[[task]]
command = ["sleep", "1"]
```

Let us assume that we have named this file as ``myjob``,
then we can run the following command to submit a job:

```commandline
$ hq job submit-file myfile
```

The effect will be same as running:

```commandline
$ hq submit sleep 1
```

## Task configuration

The following shows how job and task may be configured in more detail.
All options except `command` are optional.
If not said otherwise, an option in format `xxx = ...`
is an equivalent of `--xxx = ... ` in `hq submit` command.
The default are the same as CLI interface.

```toml

name = "test-job"
stream_log = "output.log"  # Stdout/Stderr streaming (see --log)
max_fails = 11

[[task]]
stdout = "testout-%{TASK_ID}
stderr = "testerr-%{TASK_ID}"
task_dir = true
time_limit = "1m 10s"
priority = -1
crash_limit = 12
command = ["/bin/bash", "-c", "echo $ABC"]

# Environment variables
env = {"ABC" = "123", "XYZ" = "aaaa"}

# Content that will be written on stdin
stdin = "Hello world!"

[[task.request]]
resources = { "cpus" = "4 compact!", "gpus" = 2 }
time_request = "10s"
```

## More tasks

More tasks with different configuration may be defined as follows:

```toml
[[task]]
command = ["sleep", "1"]

[[task]]
command = ["sleep", "2"]

[[task]]
command = ["sleep", "3"]
```

In the case above, tasks are given automatic task ids from id 0.
You can also specify IDs manually:

```toml
[[task]]
id = 10
command = ["sleep", "1"]

[[task]]
id = 11
command = ["sleep", "2"]

[[task]]
id = 2
command = ["sleep", "3"]
```

## Task arrays

If you want to create uniform tasks you can define task array (similar to ``--array`):

```toml
[[array]]
ids = "1,2,50-100"
command = ["sleep", "1"]
```

You can also specify array with content of ``HQ_ENTRIES``:

```toml
[[array]]
entries = ["One", "Two", "Three"]
command = ["sleep", "1"]
```

!!! note

    Options `entries` and `ids` can be used together.

## Task dependencies

TODO

## Resource variants

More resource configurations may be defined for a task.
In this case, HyperQueue will takes into account all these configurations during scheduling.
When a task is started exactly one configuration is chosen.
If in a given moment more configuration are possible for a given task,
the configuration first defined has a higher priority.

The following configuration defines that a task may be executed on
1 cpus and 1 gpu OR on 4 cpus.

```toml
[[task]]
command = [...]
[[task.request]]
resources = { "cpus" = 1, "gpus" = 1 }
[[task.request]]
resources = { "cpus" = 4 }
```

In the case that many tasks with such a configuration are submitted to a worker with
16 cpus and 4 gpus then HyperQueue will run simultaneously 4 tasks in the first configuration
and 3 tasks in the second one.

For a task with resource variants, HyperQueue sets variable `HQ_RESOURCE_VARIANT`
to an index of chosen variant (counted from 0) when a task is started.
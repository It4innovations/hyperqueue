# Task dependencies
One of the most useful features of the HyperQueue Python API is that it allows you to define
dependencies between individual tasks of a job.

If a task `B` **depends** on task `A`, then `B` will not be executed until `A` has (successfully)
finished. Using dependencies, you can describe arbitrarily complex DAG (directed acyclic graph)
workflows.

!!! notice

    HyperQueue jobs are independent of each other, so dependencies can only be specified between tasks
    within a single job.

## Defining dependencies
To define a dependency between tasks, you will first need to store the
[`Task`](hyperqueue.task.task.Task) instances that you get when you create a [task](submit.md#tasks).
You can then use the `deps` parameter when creating a new task and pass an existing task instance
to define a dependency:

```python
from hyperqueue import Job

job = Job()

# Create a first task that generates data
task_a = job.program(["generate-data", "--file", "out.txt"])

# Create a dependent task that consumes the data
job.program(["consume-data", "--file", "out.txt"], deps=[task_a])
```

The second task will not be started until the first one successfully finishes.

You can also depend on multiple tasks at once:
```python
# Create several tasks that generate data
tasks = [job.program([
    "generate-data",
    "--file",
    f"out-{i}.txt"
]) for i in range(5)]

# Create a dependent task that consumes the data
job.program(["consume-data", "--file", "out-%d.txt"], deps=[tasks])
```

Dependencies are transitive, so you can build an arbitrary graph:
```python
task_a = job.program(["generate", "1"])
task_b = job.program(["generate", "2"])

task_c = job.program(["compute"], deps=[task_a, task_b])

task_d = job.program(["postprocess"], deps=[task_c])
```
In this case, task `D` will not start until all the three previous tasks are successfully finished.

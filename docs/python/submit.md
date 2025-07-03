# Submitting jobs
You can use the Python API to submit [jobs](../jobs/jobs.md) (directed acyclic graphs of tasks)
through a [`Client`](pyapi:hyperqueue.client.Client). In addition to the functionality offered by the
HyperQueue CLI, you can use the Python API to add [dependencies](dependencies.md) between jobs,
configure each task [individually](#parametrizing-tasks) and create tasks out of
[Python functions](#python-functions).

## Job
To build a job, you first have to create an instance of the [`Job`](pyapi:hyperqueue.job.Job) class.

```python
from hyperqueue import Job

job = Job()
```

## Tasks
Once you have created a job, you can add tasks to it. Currently, each task can represent either
the execution of an [external program](#external-programs) or the execution of a
[Python function](#python-functions).

To create complex workflows, you can also specify [dependencies](dependencies.md) between tasks.

### External programs
To create a task that will execute an external program, you can use the
[`program`](pyapi:hyperqueue.job.Job#f_program) method of a `Job`:

```python
job.program(["/bin/my-program", "foo", "bar", "--arg", "42"])
```

You can pass the program arguments or various other [parameters](#parametrizing-tasks) to the task.
The `program` method will return a [`Task`](pyapi:hyperqueue.task.task.Task) object that represents the
created task. This object can be used further e.g. for defining [dependencies](dependencies.md).

### Python functions
If you want to execute a Python function as a task, you can use the
[`function`](pyapi:hyperqueue.job.Job#f_function) method of a `Job`:

```python
def preprocess_data(fast, path):
    with open(path) as f:
        data = f.read()
    if fast:
        preprocess_fast(data)
    else:
        preprocess(data)

job.function(preprocess_data, args=(True, "/data/a.txt"))
job.function(preprocess_data, args=(False, "/data/b.txt"))
```

You can pass both positional and keyword arguments to the function. The arguments will be serialized
using [cloudpickle](https://github.com/cloudpipe/cloudpickle).

Python tasks can be useful to perform e.g. various data preprocessing and organization tasks. You can
co-locate the logic of Python tasks together with the code that defines the submitted workflow (job),
without the need to write an additional external script.

Same as with the `program` method, `function` will return a [`Task`](pyapi:hyperqueue.task.task.Task)
that can used to define [dependencies](dependencies.md).

!!! note

    Currently, a new Python interpreter will be started for each Python task.

#### Python environment
When you use a Python function as a task, the task will attempt to import the `hyperqueue` package
when it executes (to perform some bookkeeping on the background). This function will be executed on
a worker - this means that it needs to have access to the correct Python version (and virtual environment)
that contains the `hyperqueue` package!

To make sure that the function will be executed in the correct Python environment, you can use
[`PythonEnv`](pyapi:hyperqueue.task.function.PythonEnv) and its `prologue` argument. It lets you specify
a (shell) command that will be executed before the Python interpreter that executes your
function is spawned.

```python
from hyperqueue.task.function import PythonEnv
from hyperqueue import Client

env = PythonEnv(
    prologue="ml Python/XYZ && source /<my-path-to-venv>/bin/activate"
)
client = Client(python_env=env)
```

If you use Python functions as tasks, it is pretty much required to use `PythonEnv`, unless your
workers are already spawned in an environment that has the correct Python loaded (e.g. using `.bashrc`
or a similar mechanism).

### Parametrizing tasks
You can parametrize both external or Python tasks by setting their working directory, standard
output paths, environment variables or HyperQueue specific parameters like
[resources](../jobs/resources.md) or [time limits](../jobs/jobs.md#time-management). In contrast to
the CLI, where you can only use a single set of parameters for all tasks of a job, with the Python
API you can specify these parameters individually for each task.

You can find more details in the documentation of the [`program`](pyapi:hyperqueue.job.Job#f_program) or
[`function`](pyapi:hyperqueue.job.Job#f_function) methods.

## Submitting a job
Once you have added some tasks to the job, you can submit it using the `Client`'s
[`submit`](pyapi:hyperqueue.client.Client#f_submit) method:

```python
client = Client()
submitted = client.submit(job)
```

To wait until the job has finished executing, use the
[`wait_for_jobs`](pyapi:hyperqueue.client.Client#f_wait_for_jobs) method:

```python
client.wait_for_jobs([submitted])
```

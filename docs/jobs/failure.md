In distributed systems, failure is inevitable. This sections describes how HyperQueue handles various types of failures
and how can you affect its behavior.

## Resubmitting array jobs
When a job fails or is canceled, you can submit it again. 
However, in case of [task arrays](arrays.md), different tasks may end in different states, and often we want to 
recompute only tasks with a specific status (e.g. failed tasks).

By following combination of commands you may recompute only failed tasks. Let us assume that we want to recompute
all failed tasks in job 5:

```commandline
$ hq submit --array=`hq job task-ids 5 --filter=failed` ./my-computation
```
It works as follows: Command `hq job task-ids 5 --filter=failed` returns IDs of failed jobs of job `5`, and we set
it to `--array` parameter that starts only tasks for given IDs.

If we want to recompute all failed tasks and all canceled tasks we can do it as follows:

```commandline
$ hq submit --array=`hq job task-ids 5 --filter=failed,canceled` ./my-computation
```

Note that it also works with `--each-line` or `--from-json`, i.e.:

```commandline
# Original computation
$ hq submit --each-line=input.txt ./my-computation


# Resubmitting failed jobs
$ hq submit --each-line=input.txt --array=`hq job task-ids last --filter=failed` ./my-computation
```


## Task restart

Sometimes a worker might crash while it is executing some task. In that case the server will automatically
reschedule that task to a different worker and the task will begin executing from the beginning.

In order to let the executed application know that the same task is being executed repeatedly, HyperQueue assigns each
execution a separate **Instance ID**. It is a 32b non-negative number that identifies each (re-)execution of a task.

It is guaranteed that a newer execution of a task will have a larger instance ID, however HyperQueue explicitly
**does not** guarantee any specific values or differences between two IDs. Each instance ID is valid only for a particular
task. Two different tasks may have the same instance ID.

Instance IDs can be useful e.g. when a task is restarted, and you want to distinguish the output of the first execution
and the restarted execution (by default, HQ will overwrite the standard output/error file of the first execution). You
can instead create a separate stdout/stderr file for each task execution using the [instance ID placeholder](jobs.md#placeholders).

## Task array failures
By default, when a single task of a [task array](arrays.md) fails, the computation of the job will continue.

You can change this behavior with the `--max-fails=<X>` option of the `submit` command, where `X` is non-negative integer.
If specified, once more tasks than `X` tasks fail, the rest of the job's tasks that were not completed yet will be canceled.

For example:
```commandline
$ hq submit --array 1-1000 --max-fails 5 ...
```
This will create a task array with `1000` tasks. Once `5` or more tasks fail, the remaining uncompleted tasks of the job
will be canceled.

In distributed systems, failure is inevitable. This sections describes how HyperQueue handles various types of failures
and how can you affect its behaviour.

## Resubmitting jobs
When a job fails or is canceled, you might want to submit it again, without the need to pass all the original
parameters. You can achieve this using **resubmit**:

```bash
$ hq job resubmit <job-id>
```

It wil create a new job that has the same configuration as the job with the entered job id. 

This is especially useful for [task arrays](arrays.md). By default, `resubmit` will submit all tasks of the original job;
however, you can specify only a subset of tasks based on their [state](jobs.md#task-state):

```bash
$ hq job resubmit <job-id> --status=failed,canceled
```

Using this command you can resubmit e.g. only the tasks that have failed, without the need to recompute all tasks of
a large task array.

## Task restart
Sometimes a worker might crash while it is executing some task. In that case the server will reschedule that task to a
different worker and the task will begin executing from the beginning.

In order to let the executed application know that the same task is being executed repeatedly, HyperQueue assigns each
execution a separate **Instance id**. It is a 32b non-negative number that identifies each (re-)execution of a task.

It is guaranteed that a newer execution of a task will have a larger instance id, however HyperQueue explicitly
**does not** guarantee any specific values or differences between two ids. Each instance id is valid only for a particular
task. Two different tasks may have the same instance id.

## Task array failures
By default, when a single task of a [task array](arrays.md) fails, the computation of the job will continue.

You can change this behaviour with the `--max-fails=<X>` option of the `submit` command, where `X` is non-negative integer.
If specified, once more tasks than `X` tasks fail, the rest of the job's tasks that were not completed yet will be canceled.

For example:
```bash
$ hq submit --array 1-1000 --max-fails 5 ...
```
This will create a task array with `1000` tasks. Once `5` or more tasks fail, the remaining uncompleted tasks of the job
will be canceled.

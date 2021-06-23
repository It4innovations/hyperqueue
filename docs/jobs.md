# Jobs

A *job* is a portion of work that can be submitted into a HyperQueue.

Each job may have one or *tasks*. In the current version, a is a single execution of a program.

**In this section, we are introducing simple jobs, where each job has exactly one tasks.**
See section about Job arrays for submitting more tasks in one job.

# Identification numbers

Each job is identified by a positive integer that is assigned by HyperQueue server when the job is submitted, we refer to it as *job id*.

Each task is identified by an unsigned 32b integer called *task id*. Task id can be assigned by a user and same task id may be used in two different jobs.

In simple jobs, task id is set to 0.


## Submitting jobs

``hq submit <program> <args1> ...``

HyperQueue assigns a unique job id when a job is submitted.

When your command contains its own switches, you need to use ``--`` after submit:

``hq submit -- /bin/bash -c 'echo $PPID'``


### Name of a job

Each job has an assigned name. It has only an informative character for the user. By default, the name is extracted from
the job's program name. You can set a job name explicitly by:

``hq submit --name=<NAME> ...``

### Working directory of a job

You can change the working directory of a job using the ``--cwd`` parameter. By default it is set to the directory
from which was the job submitted.

!!! Hint

    You can use [placeholders](#placeholders) in the working directory path.

### Output of the job

By default, each job will produce two files containing the standard output and standard error output, respectively.

The paths where these files will be created can be changed via the parameters ``--stdout=<path>`` and ``--stderr=<path>``.
You can also disable creating stdout/stderr completely by setting value ``none``.

```bash
$ hq submit --stdout=none ...
```

The default values for these paths are ``stdout.%{JOB_ID}.%{TASK_ID}`` and ``stderr.%{JOB_ID}.%{TASK_ID}``. You can read
about the `%{JOB_ID}` and `%{TASK_ID}` placeholders [below](#placeholders).

!!! Hint

    You can use [placeholders](#placeholders) in the `stdout` and `stderr` paths.

## Placeholders
You can use special variables in working directory, `stdout` and `stderr` paths, which will be interpolated with
job/task-specific information before the job is executed. Placeholders are enclosed in curly braces and prepended with
a percent sign.

Currently, you can use the following placeholders:

| Placeholder | Value                          |
| ----------- | ------------------------------------ |
| `%{JOB_ID}`     | Job ID. |
| `%{TASK_ID}`    | Task ID. |
| `%{SUBMIT_DIR}` | Directory from which the job was submitted. |
| `%{CWD}`        | Working directory of the job.<br/><br/>This placeholder is only available for `stdout` and `stderr` paths. |
| `%{DATE}`       | Current date when the job was executed in the RFC3339 format. |

## Information about jobs

List of all jobs:

``hq jobs``


List of jobs in a defined state:

``hq jobs <filters...>``

Where filters can be: ``waiting``, ``running``, ``finished``, ``failed``, or ``canceled``.

Detailed information about a job:

``hq job <job-id>``

!!! Hint

    You can also use `hq job last` to get information about the most recently submitted job.

## Task states

```
Submitted
   |
   |
   v
Waiting-----------------\
   | ^                  |
   | |                  |
   v |                  |
Running-----------------|
   | |                  |
   | \--------\         |
   |          |         |
   v          v         v
Finished    Failed   Canceled
```

* *Submitted* - Only an informative state that a submission was successful; it is only shown immediately after a submit.
* *Waiting* - The task is waiting to be executed.
* *Running* - The task is running in a worker. It may become "waiting" again when a worker (where the task is running) is lost.
* *Finished* - The task has successfully finished.
* *Failed* - The task has failed. The error can be shown by ``hq job <job-id>``.
* *Canceled* -  The task has been canceled by a user.


## Job states

In simple jobs, job state corresponds directly to the state of its single task. In the case of task arrays, see the chapter
about [task arrays](arrays.md).

## Canceling jobs

``hq cancel <job-id>``

A job cannot be canceled if it is already finished, failed, or canceled.


## Priorities

Not released yet, **scheduled for release v0.3**

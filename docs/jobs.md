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

!!! Warning

    If you want to avoid creating many files, see the section about streaming


By default, each job will produce two files containing the standard output and standard error output, respectively.

The paths where these files will be created can be changed via the parameters ``--stdout=<path>`` and ``--stderr=<path>``.
You can also disable creating stdout/stderr completely by setting value ``none``.

```bash
$ hq submit --stdout=none ...
```

The default values for these paths are ``job-%{JOB_ID}/stdout.%{TASK_ID}`` and ``job-%{JOB_ID}/stderr.%{TASK_ID}``. You can read
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
| `%{INSTANCE_ID}` | Instance ID (see below)  |
| `%{SUBMIT_DIR}` | Directory from which the job was submitted. |
| `%{CWD}`        | Working directory of the job.<br/><br/>This placeholder is only available for `stdout` and `stderr` paths. |
| `%{DATE}`       | Current date when the job was executed in the RFC3339 format. |


## Setting env variables

In a submit of a task, you can set an environment variable named `KEY` with the value `VAL` by:

``--env KEY=VAL``

You can pass the following flag multiple times to pass multiple variables.

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

## Time management

Time management is split into two settings:

* **time limit** - It is the maximal running time of a task.
               If it is reached then task is terminated and set into FAILED state.
               This setting does not have any impact on scheduling.
* **time request** - The minimal remaining lifetime of a worker needed to start the task.
                     Workers that do not have enough remaining lifetime will not be scheduled to run this task.
                     Once the task is scheduled and starts executing, it does not matter w.r.t this setting for how long it will actually run.
                     Workers without known remaining lifetime is always assumed that it may execute any time request. 

We have separated these settings to solve the following scenario. Lets us assume that we have a collection of task
where the vast majority of tasks usually finish within 10 minutes but some of them run 30 minutes. We do not know in advance which tasks are slow.
In this situation, we want to set the time limit to 35 minutes
to protect us against an error. However, we want to schedule the task to worker as long as it has at least 10 minutes of lifetime despite
we are risking of rare losing some computation time when worker dies before the computation is finished. 
(Note that in this situation, a task is rescheduled to another worker; we have only lost some computational power and not the whole task.)


### Time limit

Time limit is set as follows:

``hq submit --time-limit=TIME ...``

Where ``TIME`` is a number followed by units (e.g. ``10 min``)

### Time request

Time request is set as follows:

``hq submit --time-request=TIME``

### Time Units

You can use the following units:

* msec, ms -- milliseconds
* seconds, second, sec, s
* minutes, minute, min, m
* hours, hour, hr, h
* days, day, d
* weeks, week, w
* months, month, M -- defined as 30.44 days
* years, year, y -- defined as 365.25 days


Time can be also a combination of more units:

``hq submit --time-limit="1h 30min" ...``


## Task instance

It may happen that a task is started more than once when a worker crashes during execution of a task and the task is rescheduled to another worker. Instance IDs exist to distinguish each run when necessary. Instance ID is 32b non-negative number and it is guarantted that the newer execution has a bigger value. HyperQueue explicitly does *not* guarantee any specific value or differences between two ids. Instance ID is valid only for a particular task. Two different tasks may have the same instance ID.


## Job states

In simple jobs, job state corresponds directly to the state of its single task. In the case of task arrays, see the chapter
about [task arrays](arrays.md).

## Canceling jobs

A job cannot be canceled if it is already finished, failed, or canceled.


* Cancel specific job:

  ``hq cancel <job-id>``

* Cancel all jobs:

  ``hq cancel all``

* Cancel last submitted job:

  ``hq cancel last``


## Waiting for jobs

You can submit a job with flag ``--wait`` and HQ will wait until the submitted job is not terminated (until all tasks are either finished, failed or canceled).

You can also use ``hq wait <job_id>`` to wait for a specific job or ``hq wait last`` to wait for the last submitted job or ``hq wait all`` to wait for all jobs.


## Priorities

Priorities affect the order in which the "waiting" tasks are executed. Priority can be any 32b *signed* integer. A lowest number marks the lowest priority, e.g. when task A with priority 5 and task B with priority 3 are scheduled to the same worker, and only one of them may be executed, then A will be executed first.

In a submit of a task, you can set priority as follows

``hq submit --priority=<PRIORITY>``

If no priority is specified, then task has priority 0.


## Resubmit

If you want to recompute a previous job, jou can use:

``hq resubmit <job-id>``

that creates a new job with the exact the same configuration as the source job.

By default, it submits all tasks of the original job; however, you can specify only a subset of tasks, e.g.

``hq resubmit <job-id> --status=failed,canceled``

Resubmits only tasks that failed or were canceled.

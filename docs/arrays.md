
# Task arrays

[Added in 0.2]

Task arrays is a mechanism for submitting many tasks at once.
It allows to create a job with many tasks in a single submit and monitor and manage them as a single group.

Note: From the user perspective, it is functionally equivalent for "job arrays" in SLURM/PBS; however, HQ does not use SLURM/PBS job arrays for providing this feature.
HyperQueue's task arrays are handled as any other tasks, e.g. it may happen that two tasks from one array may run simultaneously in the same worker if there are enough resources.


## Submitting task array

The following submits 100 tasks in a single job with task ids 1-100:

``$ hq submit --array 1-100 <program> <args1> ...``

Generally, task ids may be specified with the following syntax (X and Y are unsigned integers):

* X-Y - Include range from X to Y
* X - An array with a single element X

## Env variables

When a task is started then the following environment variables are created:

* HQ_JOB_ID - Job id
* HQ_TASK_ID - Task id


## Task states

Each task has its own individual state as defined in the [previous chapter](jobs.md).
The number of tasks with each state can be displayed by the following command:

``$ hq job <job_id>``

Detailed state of each task will also be included if you pass the `--tasks` flag:

``$ hq job <job_id> --tasks``

A global job state for summary outputs is derived from the state of its tasks by the first rule that matches from the
following list of rules:


1. If at least one task is in state "Running", then job state is "Running".
2. If at least one task has not been computed yet, then job state is "Waiting".
   A task has been computed once it has reached the `canceled`, `failed` or `finished` state.
3. If at least one task is in state "Canceled" then job state is "Canceled".
4. If at least one task is in state "Failed" then job state is "Failed".
5. All tasks have to be in state "Finished", therefore the job state will also be "Finished".


## Task fail in array job

By default, when a task fails the computation of job continues.

You can change it by ``--max-fails=X`` where ``X`` is non-negative integer.
If more tasks then ``X`` fails, then the rest of non-finished tasks are canceled.

## Time limit

Time limit (``--time-limit``) is counted for each task separatatelly.

## Job canceling

When a job with more tasks is canceled then all non-finished tasks is canceled.


## Task for each line of a file

The switch ``--each-line=<FILE>`` is an alternative way to create an array job. It creates a job for each line of the file. The content of a line is stored in variable ``HQ_ENTRY``.

Example:

``$ hq submit --each-line /path/to/file my-program.sh``




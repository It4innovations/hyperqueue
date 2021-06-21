
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


## Tasks states

Each task has its own individual state as defined in the previous chapter.
The number of tasks with each state can be seen by:

``$ hq job <job_id>``

State of each task can be seen:

``$ hq job <job_id> --tasks``

A global job state for summary outputs is derived as following (when a rule is matched, the rest is ignored):

* If at least one task is in state "Running" then job state is "Running".
* If at least one task is in state "Canceled" then job state is "Canceled".
* If at least one task is in state "Failed" then job state is "Failed".
* If at least all tasks are in state "Finished" then job state is "Finished".
* Otherwise the job state is "Waiting".


## Task fail in array job

By default, when a task fails the computation of job continues.

You can change it by ``--max-fails=X`` where ``X`` is non-negative integer.
If more tasks then ``X`` fails, then the rest of non-finished tasks are canceled.

## Job canceling

When a job with more tasks is canceled then all non-finished tasks is canceled.
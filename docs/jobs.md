
# Jobs

In the current version, a job is a single execution of a program.

## Submiting jobs

``hq submit <program> <args1> ...``

HyperQueue assigns a unique job id when a job is submitted.


## Name of a job

Each job has assigned a name. It has only an informative character for the user. By default, the name is extracted from the job's program name. You can set a job name explicitly by:

``hq submit --name=<NAME> ...``


## Information about jobs

List of all jobs:

``hq jobs``


Detailed information about a job:

``hq job <job-id>``


## Output of the job

By default, job produces two files named ``stdout.<job-id>`` and ``stderr.<job-id>`` that contains standard output and standard error output in the. The files are by default placed in the directory where the job was submitted.


## Job states

```
Submitted
   |
   |
   v
Waiting-----------------\
   |                    |
   |                    |
   v                    |
Running-----------------|
   | |                  |
   | \--------\         |
   |          |         |
   v          v         v
Finished    Failed   Canceled
```

* *Submitted* - Only an informative state that submit was successfull, immediately changed into "Waiting" state.
* *Waiting* - The job is waiting for start
* *Running* - The job is running in a worker
* *Finished* - The job was sucessfully finished
* *Failed* - The job failed. The error can be shown by ``hq job <job-id>``.
* *Canceled* -  The job was canceled by a user.

## Canceling jobs

``hq cancel <job-id>``

A job cannot be canceled if it is already finished, failed, or canceled.



## Priorities

Not released yet, **scheduled for release v0.3**


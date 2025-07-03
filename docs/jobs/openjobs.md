# Open jobs

By default, a job is a set of tasks that are created atomically during a submit, and no other task can be added to the
job.
We call this job *closed*. In contrast, HQ allows you to create an *open* job that allows new tasks to be submitted as
long as it is open.

## Opening a job

A job can be opened with the [`hq job open`](cli:hq.job.open) command:

```bash
$ hq job open
```

If opening was successful, this will be printed:

```
Job <ID> is open.
```

If you want to get just ID without any additional text, you can open job as follows:

```bash
$ hq --output-mode=quiet job open
```

Note: In the list of jobs, an open job is marked with "*" before the id.

## Submitting tasks into open jobs

A submit to an open job is the same as a normal submit, except that you must specify the job you are submitting to with
the `--job` argument. You may submit multiple times into the same job. Tasks are scheduled to the workers immediately
when they are received by the server.

```
$ hq submit --job <JOB_ID> ... other submit args ...
$ hq submit --job <JOB_ID> ... other submit args ...
$ hq submit --job <JOB_ID> ... other submit args ...
```

## Task Ids

All tasks in one job share the task ID space. When you do not specify task ids, HQ automatically assigns a smallest ID
that is bigger then any existing task id.

```bash
$ hq job open
$ hq submit --job <JOB_ID> -- hostname # Task ID is 0 
$ hq submit --job <JOB_ID> -- hostname # Task ID is 1

# Task IDs are 2, 3, 4 ...
$ hq submit --job <JOB_ID> --each-line='test.txt' -- do-something
```

If you are explicitly specifying task IDs, it is an error if task ID is reused:

```bash
$ hq submit --job <JOB_ID> -- hostname # Task ID is 0

# This is Ok 
$ hq submit --job <JOB_ID> --array 10-20 -- hostname

# This fails: Task ID 0 and 10, 11, 12 already exist
$ hq submit --job <JOB_ID> --array 0-12 -- hostname
```

## Job name and `--max-fails`

Job's name and configuration open `--max-fails` are the property of the job. They can be set when job is opened and they
cannot be later changed. Submit options `--name` and `--max-fails` cannot be used if you are submitting into an open
job.

```bash
# Configuring jobs's name and max fails
$ hq job open --name=MyOpenJob --max-fails=10

# Submit fails becase --max-fails cannot be used together with --job
$ hq submit --job <JOB_ID> --max-fails=5 ...
```

## Submit file into open job

Submitting job definition file into an open job works in the similar way as a normal submit, you just need to
add `--job` parameter.

```bash
$ hq job submit-file --job <JOB_ID> job-definition.toml
```

## Closing job

You can close a job with the [`hq job close`](cli:hq.job.close) command:

```bash
$ hq job close <JOB_SELECTOR>
```

When a job is closed, you are not allowed to submit any more tasks to the job.
It has no effect on tasks already submitted to the job; they continue to be processed as usual.

Closing of already closed job throws an error.

## Completion semantics

Leaving open jobs has no overhead, but it does affect the semantics of job completion.
A job is considered completed when all tasks have been completed and the job is *closed*.
Therefore, [`hq job wait`](cli:hq.job.wait) will wait until all tasks of the selected jobs are complete and the jobs are closed.

If you want to wait only for completion of tasks and ignoring if job is open or closed then there
is `hq job wait --without-close ...`.

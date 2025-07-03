Sometimes, you may have connected workers and submitted tasks, but tasks are not running, and it is not clear why. For
this purpose, there is a [`hq task explain`](cli:hq.task.explain) command in HQ.

It can be used as follows:

`hq task explain <JOB_ID> <TASK_ID>`

This command provides information about whether the given task can run on workers.
If it cannot run, it returns explanation why. The explanation considers the following areas:

* Resources
* Time request and remaining worker time
* Size of the worker group for multi-node tasks
* Task dependencies
* If a task has multiple resource variants, the explanation is provided for each variant.

Note: `explain` considers runtime information; therefore, it works only for waiting/running tasks and live workers.

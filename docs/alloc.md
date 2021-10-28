
# Automatic Allocation

The goal of automatic allocation is to autonomously allocate resources for workers in HPC scheduler (PBS/SLURM). In other words, it submits SLURM/PBS jobs as needed.


**Note:** This is feature is in a preview mode. It works but semantics, default configuration, or API could be changed in future versions.

To solve the terminology clash between HQ jobs and PBS/SLURM jobs, we will call the latter as "mjobs" (= manager jobs, where we use manager as an umbrella term for PBS or SLURM).


## Behavior

The current version works as follows: If there are waiting tasks in HQ, then HQ server tries to submit and maintain a given amount of *WAITING* mjobs in manager. This number can be configured and default value is 4. Note that this does not limit the total number of running workers, only waiting mjobs in queue. If a cluster is empty and HPC scheduler gives starts our mjobs then it will eventually allocate the whole cluster.

If there no waiting tasks then the submission of mjobs are stopped.

Each worker submited through auto allocation is configured that it will terminates after 5 minutes without tasks.


## Example

Start auto allocation over PBS in queue "qexp":

``$ hq alloc add pbs --queue qexp``

Start auto allocation over SLURM in queue "qexp":

``$ hq alloc add slurm --partition qexp``

## Passing additional arguments
You can also add arbitrary arguments to the `hq alloc add` command, which will be forwarded to `qsub` or `sbatch`:

```bash
$ hq alloc add pbs --queue qprod -- -A PROJ-1
```

## Information about allocations


List of allocations:

``$ hq alloc list``

Allocation details:

``$ hq alloc info <ID>``


Events for a queue:

``$ hq alloc events <ID>``


## Removing allocation

``$ hq alloc remove <ID>``

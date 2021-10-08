
# Automatic Allocation

The goal of automatic allocation is to autonomously allocate resources for workers in HPC scheduler (PBS/SLURM). In other words, it submits SLURM/PBS jobs as needed.


**Note:** This is feature is in a preview mode. It works but semantics, default configuration, or API could changed in future versions.

To solve the terminology clash between HQ jobs and PBS/SLURM jobs, we will call the latter as "mjobs" (= manager jobs, where we use manager as an umbrella term for PBS or SLURM).


## Behavior

The current version works as follows: If there are waiting tasks in HQ, then HQ server tries to submit and maintain a given amount of *WAITING* mjobs in manager. This number can be configured and default value is 4. Note that this does not limit the total number of running workers, only waiting mjobs in queue. If a cluster is empty and HPC scheduler gives starts our mjobs then it will eventually allocate the whole cluster.

If there no waiting tasks then the submission of mjobs are stopped.

Each worker submited through auto allocation is configured that it will terminates after 5 minutes without tasks.


## Example

Start auto allocation over PBS in queue "qexp":

$ hq alloc add pbs --queue qexp


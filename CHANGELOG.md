# DEV

## New features

  * You can now test auto allocation parameters using a dry-run command:
    ```bash
    $ hq alloc dry-run pbs --time-limit 2h -- -q qexp -A Project1
    ```
    Using this command you can quickly test if PBS/Slurm will accept allocations created with
    the provided parameters.
  * Worker can be started with --cpus="no-ht" that detects CPUs but ignores HyperThreading
    (for each physical core it ignores all except the first HT virtual core)
  * You can now specify the timelimit of PBS/Slurm allocations using the `HH:MM:SS` format:
    `hq alloc add pbs --time-limit 01:10:30`.
  * Improve error messages printed when an invalid CLI parameter is entered.

## Changes

  * `hq alloc remove` will no longer let you remove an allocation queue that contains running
    allocations by default. If you want to force its removal and cancel the running allocations
    immediately, use the `--force` flag. 

# v0.6.1

## Fixes

* Fixed computation of worker load in scheduler
* Fixed performance problem when canceling more than 100k tasks

## Changes

* When a job is submitted, it does not show full details in response
   but only a short message. Details can be still shown by `hq job <id>`.


# v0.6.0

## New features

  * Generic resource management has been added. You can find out more in the [documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/gresources/).
    * HyperQueue can now automatically detect how many Nvidia GPUs are present on a worker node.
  * You can now submit a task array where each task will receive one element of a JSON array using
    `hq submit --from-json`. You can find out more in the [documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/arrays/#json-array).

## Changes

  * There have been a few slight CLI changes:
    * `hq worker list` no longer has `--offline` and `--online` flags. It will now display only running
      workers by default. If you want to show also offline workers, use the `--all` flag.
    * `hq alloc add` no longer has a required `--queue/--partition` option. The PBS queue/Slurm partition
      should now be passed as a trailing argument after `--`: `hq alloc add pbs -- -qqprod`.
  * Server subdirectories generated for each run of the HyperQueue server are now named with a numeric ID instead of
  a date.
  * The documentation has been [rewritten](https://it4innovations.github.io/hyperqueue).


# v0.5.0

## New features

  * Time limit and Time request for tasks (options ``--time-limit`` and ``--time-request``)
  * Time limit for workers
  * Job and task times are shown in job information tables
  * Integers in command line options can be now written with an underscore separator (e.g. ``--array=1-1_000``)
  * Placeholders in log file paths
  * Preview version of PBS and SLURM auto allocation
  * HyperQueue can be now compiled without `jemalloc` (this enables PowerPC builds).
    To remove dependency on `jemalloc`, build HyperQueue with `--no-default-features`.

## Changes

  * `hq submit --wait` and `hq wait` will no longer display a progress bar while waiting for the job(s) to finish.
  The progress bar was moved to `hq submit --progress` and `hq progress`.
  * The default path of job stdout and stderr has been changed to ``job-%{JOB_ID}/%{TASK_ID}.[stdout/stderr]``
  * Normalization of stream's end behavior when job is canceled
  * Job id is now represented as u32


# v0.4.0

## New features

  * Streaming - streaming stdout/stderr of all tasks in a job into one file
    to avoid creating many files.
  * Better reporting where job is running.
  * Setting a priority via ``hq submit --priority <P>``
  * Option ``hq submit --wait ...`` to wait until the submitted job finishes
  * Command ``hq wait <id> / all / last`` to wait for a given job(s)
  * Command ``hq resubmit <job-id>`` to resubmit a previous job
  * Command ``hq cancel all`` / ``hq cancel last`` to cancel all jobs / last job
  * Command ``hq worker stop all`` to cancel all workers
  * Command ``hq server info`` to get an information about server


# v0.3.0

## New features

  * Option for automatic closing workers without tasks (Idle timeout)
  * Submit option ``--max-fails X`` to cancel an job when more than X tasks fails
  * Submit option ``--each-line FILE`` to create a task per a line in a file.
  * Submit option ``--env VAR=VALUE`` to specify env variable in a task
  * Submit option ``--cwd DIR`` to specify a working dir of a task
  * New placeholders in paths: ``%{CWD}``, ``%{DATE}``, and ``%{SUBMIT_DIR}``
  * Added a progressbar in a job array detail.
  * ``hq server start --host=xxx`` allows to specify hostname/address under which the server is visible


# v0.2.1

## New features

  * Filters for command ``hq jobs <filter>``
    (e.g. ``hq jobs running``)

## Fixes

  * NUMA detection on some architectures


# v0.2.0

## New features

  * Job arrays
  * Cpu management
  * --stdout/--stderr configuration in submit

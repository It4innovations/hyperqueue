# DEV

## Fixes

### Automatic allocation
* [Issue #294](https://github.com/It4innovations/hyperqueue/issues/294): The automatic allocator
  leaves behind directories of inactive (failed or finished) allocations on the filesystem. Although
  these directories contain useful debugging information, creating too many of them can needlessly
  waste disk space. To alleviate this, HyperQueue will now keep only the last `20` directories of
  inactive allocations per each allocation queue and remove the older directories to save space.
  
  You can change this parameter by using the `--max-kept-directories` flag when creating an allocation
  queue:

  ```bash
  $ hq alloc add pbs --time-limit 1h --max-kept-directories 100
  ```

## New features


### Job submission
   * `#HQ` directives - You can now specify job parameters using a shell script passed to `hq submit`
     by using HQ directives such as `#HQ --cpus=4`. This feature was inspired by similar functionality
     that is present in e.g. PBS or Slurm. You can find more information in the
     [documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/directives/).

   * HyperQueue will now attempt to parse shebang (like `#!/bin/bash`) if you provide a path to a
     shell script (`.sh`) as the first command in `hq submit`. If the parsing is successful, HyperQueue
     will use the parsed interpreter path to execute the shell script. In practice, this means that
     you can now submit scripts beginning with a shebang like this:

        ```bash
        $ hq submit script.sh
        ```

        This previously failed, unless you provided an interpreter, or provided a path starting with
        `.` or an absolute path to the script.


### Worker configuration
  * You can now select what should happen when a worker loses its connection to the server using the
    new `--on-worker-lost` flag available for `worker start` and `hq alloc add` commands. You can find
    more information in the [documentation](https://it4innovations.github.io/hyperqueue/stable/deployment/worker/#lost-connection-to-the-server).


### CLI
* You can now force HyperQueue commands to output machine-readable data using the `--output-mode` flag
available to all HyperQueue commands. Notably, you can output data of the commands as JSON. You can
find more information in the [documentation](https://it4innovations.github.io/hyperqueue/stable/cli/output-mode/).

* You can now generate shell completion using the `hq generate-completion <shell>` command.

## Changes
### CLI
* The command line interface for jobs has been changed to be more consistent with the interface for
  workers. Commands that have been formerly standalone (like `hq jobs`, `hq resubmit`, `hq wait`) are
  not accessed through `hq job`. The only previous job-related command that remained on the top level
  is `hq submit`, which is now a shortcut for `hq job submit`. Here is a table of changed commands:

  | **Previous command** | **New command**    |
      |------------------|--------------------|
  | `hq jobs`           | `hq job list`    |
  | `hq job`            | `hq job info`    |
  | `hq resubmit`       | `hq job resubmit` |
  | `hq cancel`         | `hq job cancel`  |
  | `hq wait`           | `hq job wait`    |
  | `hq progress`       | `hq job progress` |
  | `hq submit`         | `hq submit` or `hq job submit` |

* The `--tasks` flag of the `hq job info <job-id>` command has been removed. If you want to display the
individual tasks of a job, please use the new `hq job tasks <job-id>` command.

* The command line parsing of `hq submit` has been changed slightly. All flags and arguments that appear
  after the first positional argument will now be considered to belong to the executed program, not to
  the submit command. This mimics the behaviour of e.g. `docker run`. For example:
    ```bash
    $ hq submit foo --array 1-4
    # Before: submits a task array with 4 tasks that execute the program `foo`
    # Now: submits a single task that executes `foo --array 1-4`
    ```

* Tables outputted by various informational commands (like `hq job info` or `hq worker list`)
are now more densely packed and should thus better fit on terminal screens.

# v0.7.0

## Fixes

  * Fixes an invalid behavior of the scheduler when resources are defined

  * The automatic allocator will no longer keep submitting allocations in situations where the created
    workers would not be able to execute currently waiting tasks. Currently, this situation is detected
    only for the case when a task has a time request higher than the time limit of the allocation
    queue.

## New features

### Automatic allocation
* You can now specify CPU and generic resources for workers created by the automatic allocator: 
    ```bash
    $ hq alloc add pbs --time-limit 2h --cpus 4x4 --resource "gpu=indices(1-2)" -- -q qexp -A Project1
    ```
* You can now test auto allocation parameters using a dry-run command:
    ```bash
    $ hq alloc dry-run pbs --time-limit 2h -- -q qexp -A Project1
    ```
    Using this command you can quickly test if PBS/Slurm will accept allocations created with
    the provided parameters.
* You can now specify a limit for the number of workers spawned inside a single allocation queue.
  You can use the parameter `--max-worker-count` when creating a queue to make sure that the queue
  will not create too many workers.
    ```bash
    $ hq alloc add pbs --time-limit 00:10:00 --max-worker-count 10 -- -q qprod -A Project1
    ```
* You can now specify the timelimit of PBS/Slurm allocations using the `HH:MM:SS` format:
`hq alloc add pbs --time-limit 01:10:30`.

### Resource management
* Workers can be now started with the parameter `--cpus="no-ht"`. When detecting CPUs in this mode,
  HyperThreading will be ignored (for each physical core only the first HT virtual core will be chosen).
* The user may explicitly specify what CPU IDs should be used by a worker
  (including arrangement of IDs into sockets).
  (E.g. ``hq worker start --cpus=[[0, 1], [6, 8]]``)

### CLI
* Improve error messages printed when an invalid CLI parameter is entered.

## Changes

  * The `--time-limit` parameter of `hq alloc add` command is now required.
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

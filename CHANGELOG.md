## Unreleased

### New features

* Server scheduler more respects priorities

## v0.24.0

### Breaking changes

* The `--no-detect-resources` flag of the `hq worker start` command has been removed.
  You can now configure automatically detected resources in a granular way using the new
  `--detect-resources` flag (see below). `--no-detect-resources` corresponds to `--detect-resources=none`.

### New features

* New policy `tight` (and `tight!`) that is the original implementation of `compact`.
  It selects minimal number of resources groups and then tries to get maximum resources from
  a biggest group and then maximum resources from the second biggest group, etc.
  The policy `compact` now behaves as is described in the section "Changes".
* Resource policy `compact!` is now allowed to take fractional resource request.
* New command `hq alloc cat <alloc-id> <stdout/stderr>`, which can be used
  to debug the output of allocations submitted by the automatic allocator.
* New command `hq server wait` that repeatedly tries to connect to a server with a configurable timeout.
  This is useful for deployment scripts that need to wait for server availability.
* New `hq alloc add` parameter called `--wrap-worker-cmd`. It can be used to start
  workers on allocated nodes using some wrapping mechanism (e.g. Podman).
* New flag `--detect-resources` for `hq worker start`. It can be used to configure which worker resources
  will be automatically detected. You can e.g. say `--detect-resources=cpus,gpus/nvidia`.
  See [documentation](https://it4innovations.github.io/hyperqueue/latest/jobs/resources) for more information.
* The scheduler has better compacting behavior when there are small number of tasks
  and workers appearing/disappering
* Autoallocator keeps log file when probing allocation fails.
* Unstable: Resource "coupling".
  You may specify that some resources are coupled, e.g. cpus and gpus.
  That means that cpus are gpus are organized in numa nodes, and allocation strategy
  will respect that, i.e., it tries to find cpus and gpus from the same numa nodes.
  Note: The current implementation does not detect coupling automatically,
  you have to specify it manually.

### Changes

* Allocation policy `compact` was updated.
  It still tries to find the minimal number of the resource groups,
  but when they are found, resources are evenly taken from the selected groups.
  In rare cases when you need original behavior, use new policy `tight`.
  It is not a breaking change, because the `compact` previously did not specified how
  exactly will be resources taken from groups.
* Worker process terminated because of idle timeout now returns zero exit code.

### Fixes

* Fixed the issue of possible ignoring idle timeout when time request is used.
* Fixes broken streaming when job file is used.
* Fixed missing fields in export of journal into JSON

## 0.23.0

### Breaking change

* In `--crash-limit` value 0 is no longer allowed, use `--crash-limit=unlimited`.
* The `--workers-per-alloc` flag of the `hq alloc add` command has been **replaced** with `--max-workers-per-alloc`,
  which determines the maximum number of workers to spawn in each allocation. Previously, the flag caused the
  allocator to (almost) always spawn the determined number of workers per allocation, regardless of actual
  computational load.

### Changes

The automatic allocator has been finally reimplemented, and is now much better:

* It now uses information from the scheduler to determine how many allocations to spawn, and thus it can react to the
  current computational load much more accurately. It should also be less "eager".
* It properly supports multi-node tasks.
* It considers computational load across all allocation queues (before, each queue was treated separately, which led to
  creating too many submissions).
* It now exposes a `min-utilization` parameter, which can be used to avoid spawning an allocation that couldn't be
  utilized
  enough.

As this is a large behavioral change, we would be happy to hear your feedback!

### New features

* New command `hq task explain <job_id> <task_id>` explains why a task cannot be run on a given worker.
* The server scheduler now slightly prioritizes tasks from older jobs and finishing partially-computed task graphs
* New values for `--crash-limit`:
    * `never-restart` - task is never restarted, even if it "crashes" on a worker that was explicitly terminated.
    * `unlimited` - unlimited crash limit
* `hq worker info` contains more information
* `hq job forget` tries to free more memory
* You can now configure Job name in the Python API.
* `hq job progress` now displays all jobs and tasks that you wait for, rather than those that were unfinished at the
  time when the command was executed.

### Fixes

* Fixed a problem with journal loading when task dependencies are used
* Fixed restoring crash counters and instance ids from journal
* Fixed some corner cases of load balancing in server scheduler

### Docs

* CLI documentation (when `--help` is used) was cleaned up and improved
* Our documentation now contains an automatically generated reference of all available HQ CLI commands and options
* The `hq doc` and `hq generate-completion` commands have been documented

### Experimental

* Added direct data transfers between tasks. User API not stabilized

## 0.22.0

### New features

* Added `hq worker deploy-ssh` to deploy workers to a set of nodes using SSH.
* Added `hq doc` command for accessing documentation about various HQ features from the command-line.
* `hq journal replay` added. It similar to `hq journal stream` but it will not wait for new events.
* More robust initialization of dashboard
* Authentication and encryption of client/worker connection can be disabled. It is mostly for testing
  and benchmarking purpose. Do not use if you are not in 100% safe environment.

### Breaking change

* The Python API now requires Python 3.9, up from Python 3.6.

### Fixes

* Fixes #848, inefficient scheduling of tasks with priorities
* HyperQueue will no longer allocate extreme amounts of memory when loading a corrupted journal

## v0.21.1

### Fixes

* Fixes random task crashes. Details in #823.

## v0.21.0

### Breaking change

* Pre-built HyperQueue releases available from our GitHub repository are now built with GLIBC `2.28`, instead of `2.17`.
  If you need to run HyperQueue on a system with an older GLIBC version, you might need to recompile it from source on
  your system. If you encounter any issues, please let us know.

### Changes

* `hq event-log` command renamed to `hq journal`
* `hq dashboard` has been re-enabled by default.

### New features

* Added `hq journal prune` for pruning journal file.
* Added `hq journal flush` for forcing server to flush the journal.

## v0.20.0

### New features

* It is now possible to dynamically submit new tasks into an existing job (we call this concept "Open jobs").
  See [Open jobs documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/openjobs/)

* Worker streaming. Before, you could stream task stderr/stdout to the server over the network using the `--log`
  parameter of `hq submit`.
  This approach had various issues and was not scalable. Therefore, we have replaced this functionality with worker
  streaming,
  where the streaming of task output to a set of files on disk is performed by workers instead.
  This new streaming approach creates more files than original solution (where it was always one file per job),
  but the number of files stays small and independent on the number of executed tasks.
  The new architecture also allows parallel I/O writing and storing of multiple job streams in one stream handle.
  You can use worker streaming using the `--stream` parameter of `hq submit`. Check out the documentation for more
  information.

* Optimization of journal size

* Tasks' crash counters are not increased when worker is stopped by `hq worker stop` or by time limit.

### Removed

* Because worker streaming fully replaces original streaming, the original server streaming was removed.
  For most cases, you can rename `--log` to `--stream` and `hq log` to `hq output-log`. See the docs for more details.

### Fixes

* HQ should no longer crash while printing job info when a failed task does not have any workers
  attached (https://github.com/It4innovations/hyperqueue/issues/731).

### Note

* Dashboard still not enabled in this version

## v0.19.0

### New features

* Server resilience. Server state can be loaded back from a journal when it crashes. This will restore the state of
  submitted jobs and also autoallocator queues. Find out
  more [here](https://it4innovations.github.io/hyperqueue/stable/deployment/server/#resuming-stoppedcrashed-server).

* `HQ_NUM_NODES` for multi-node tasks introduced. It contains the number of nodes assigned to task.
  You do not need to manually count lines in `HQ_NODE_FILE` anymore.

### Changes

* Dashboard is disabled in this version. We expect to reneeble it in 1-2 release cycles

* Node file generated for multi-node tasks now contains only short hostnames
  (e.g. if hostname is "cn690.karolina.it4i.cz", only "cn690" is written into node list)
  You can read ``HQ_HOST_FILE`` if you need to get full hostnames without stripping.

### Fixes

* Enable passing of empty `stdout`/`stderr` to Python function tasks in the Python
  API (https://github.com/It4innovations/hyperqueue/issues/691).
* `hq alloc add --name <name>` will now correctly use the passed `<name>` to name allocations submitted to Slurm/PBS.

## v0.18.0

### Breaking change

* Mechanism for resubmitting tasks was changed. Command `resubmit` was removed,
  see https://it4innovations.github.io/hyperqueue/latest/jobs/failure/ for replacement.

* The output format of the `job info` command with JSON output mode has been changed. Note that
  the JSON output mode is still unstable.

### New features

* Combination of --time-request and --nodes is now allowed

* Allow setting a time request for a task (`min_time` resource value) using the Python API.

* Optimizations related to job submit & long term memory saving

* The CLI dashboard is now enabled by default. You can try it with the `hq dashboard` command. Note that it is still
  very experimental and a lot of useful features are missing.

## v0.17.0

### Breaking change

#### Memory resource in megabytes

* Automatically detected resource "mem" that is the size of RAM of a worker is now using megabytes as a unit.
  i.e. `--resource mem=100` asks now for 100 MiB (previously 100 bytes).

### New features

#### Non-integer resource requests

* You may now ask of non-integer amount of a resource. e.g. for 0.5 of GPU.
  This enables resource sharing on the logical level of HyperQueue scheduler and allows to utilize remaining part the
  resource
  by another tasks.

#### Job submission

* You can now specify `cleanup modes` when passing `stdout`/`stderr` paths to tasks. Cleanup mode decides what should
  happen with the file once the task has finished executing. Currently, a single cleanup mode is implemented, which
  removes
  the file if the task has finished successfully:

```bash
$ hq submit --stdout=out.txt:rm-if-finished /my-program
```

### Fixes

* Fixed crash when task fails during its initialization

## v0.16.0

### New features

#### Pregenerating access files

* Via command `hq server generate-access` you can precreate an access file that can be later used for staring server,
  and connecting workers, and clients. This is useful in cloud environments.

#### Job submission

* A new command `hq job forget <job-selector>` has been introduced. It can be used to completely forget a job, and thus
  reduce the memory usage of the HQ server. It is useful especially if you submit a large amount of jobs and keep the
  server running for a long time.

#### Automatic allocation

* Autoalloc can now execute a custom shell command/script on each worker node before the worker starts and after the
  worker stops. You can use this feature e.g. to initialize some data or load software modules for each worker node.

  ```console
  $ hq alloc add pbs --time-limit 30m \
    --worker-start-cmd "/project/xxx/init-node.sh" \
    --worker-stop-cmd "/project/xxx/cleanup-node.sh"
  ```
* You can now set a time limit for workers spawned in allocations with the `--worker-time-limit` flag. You can use this
  command to make workers stop sooner, so that you e.g. give more headroom for a `--worker-stop-cmd` command to execute
  before the allocation is terminated. If you do not use this parameter, worker time limit will be set to the time limit
  of the allocation.

  Example:
  ```console
  $ hq alloc add pbs --time-limit 1h --worker-time-limit 58m --worker-stop-cmd "/project/xxxx/slow-command.sh"
  ```
  In this case, the allocation will run for one hour, but the HQ worker will be stopped after 58 minutes (unless it is
  stopped sooner because of idle timeout). The worker stop command will thus have at least two minutes to execute.

### Changes

#### Access file

The format of the access file is changed. It is mostly internal change but you can experience parsing error when
connecting
an old client/worker to a new server (Connecting a new client/worker to an old server will given you a proper message).

## v0.15.0

### Breaking changes

- **NVIDIA GPUs are now automatically detected under the resource name `gpus/nvidia`, instead of
  just `gpus`!** If you have been using the `gpus` resource name, you should update your scripts.
  See more details below.

### New features

#### Resource management

* You can now specify more resources for one task, e.g.: 1 cpu and 1 gpu OR 4 cpus. The scheduler considers both
  configurations in task planning.
  For example let us assume that we have many tasks with the mentioned configuration and worker with 16 cpus and 4 gpus.
  The tasks will fully utilize the node, 4 tasks will run in the configuration with gpu and 3 tasks will run in the cpu
  only mode.

* Job Definition File is a TOML file that can define a job.
  It allows to submit complex jobs without using Python API (dependencies, resource variants, ...).

  ```console
  $ hq job submit-file myfile.toml
  ```

* You can now specify (indexed) resource values provided by workers as strings (previously only
  integers were allowed). Notably, automatic detection of Nvidia GPUs specified with string UUIDs
  now works.

  ```console
  $ hq worker start --resource="res1=[foo, bar]"
  ```

* HyperQueue now provides built-in support for AMD GPUs. For this reason, the default name of GPU
  resources that are automatically detected on a worker has been changed from `gpus` to `gpus/nvidia`
  for NVIDIA GPUs. AMD GPUs are now autodetected as `gpus/amd`. In the future, we intend to create a way
  to ask for any GPU resource (e.g. `--resource=gpus=2`), regardless of its type.

* AMD GPUs are now automatically detected in workers from the environment variable `ROCR_VISIBLE_DEVICES`.

* Allowed characters for resource names has been changed. The name now has to begin with an ASCII letter,
  and it can only contain ASCII letters, ASCII digits and the slash (`/`) symbol. This restriction is
  introduced for better alignment with shells, which typically do not support complicated variable names.
  HQ passes the resource names to executed tasks through environment variables, so it has to take this
  into account. Note that the `/` symbol in resource name will be normalized to `_` when being passed
  to a task.

* `hq task info` now shows more information

### Changes

#### Job submission

* The default path for `stdout` and `stderr` files has been changed
  from `%{SUBMIT_DIR}/job-%{JOB_ID}/%{TASK_ID}.[stdout/stderr]`
  to `%{CWD}/job-%{JOB_ID}/%{TASK_ID}.[stdout/stderr]`. Note that the default value for the working
  directory (`%{CWD}`) is set to the submission directory, so if you have used the defaults before,
  nothing will change for you. Stdout and stderr paths are now also resolved relative to the working
  directory of the given task, not to the submit directory.

## v0.14.0

### New features

#### CLI

* [#545](https://github.com/It4innovations/hyperqueue/issues/545) Add a new command `hq job summary`,
  which displays the amount of jobs per each job state.

### Platforms

* HQ can be now compiled for Raspberry Pi

### Fixes

#### Worker

* [#539](https://github.com/It4innovations/hyperqueue/issues/539) Fix connection of worker to server
  in the presence of both IPv4 and IPv6 addresses.

#### Job submission

* [#540](https://github.com/It4innovations/hyperqueue/issues/540) Parse all arguments from shebang
  in a directives file (e.g. `#!/bin/bash -l`).

#### Streaming

* Fixed a bug in closing streaming when tasks are very short and synchronized.

## v0.13.0

### New features

#### Resource management

* Almost complete rewrite of resource management.
  CPU and other resources were unified: the most visible change is that you can define "cpus" and other resource;
  and other resources can now be defined in groups (NUMA-like resources).

* Many improvements in scheduler: Improved schedules for multi-resource requests;
  better behavior on non-heterogeneous clusters;
  better interaction between resources and priorities.

#### Automatic allocation

* [#467](https://github.com/It4innovations/hyperqueue/issues/467) You can now pause (and resume)
  autoalloc queues using `hq alloc pause` and `hq alloc resume`.
  Paused queues will not submit new allocations into the selected job manager. They can be later resumed.
  When an autoalloc queue hits too many submission or worker execution errors, it will now be paused
  instead of removed.

#### Tasks

* HQ allows to limit how many times a task may be in a running state while worker is lost
  (such a task may be a potential source of worker's crash).
  If the limit is reached, the task is marked as failed.
  The limit can be configured by `--crash-limit` in submit.

* Groups of workers are introduced. A multi-node task is now started only on workers from the same group.
  By default, workers are grouped by PBS/Slurm allocations, but it can be configured manually.

### Changes

#### Resource management

* ``--cpus=no-ht`` is now changed to a flag ``--no-hyper-threading``.
* Explicit list definition of a resource was changed from ``--resource xxx=list(1,2,3)`` to ``--resource xxx=[1,2,3]``.
  (this is the result of unification of CPUs with other resources).
* Python API: Attribute `generic` in `ResourceRequest` is renamed to `resources`

#### Tasks

* [#461](https://github.com/It4innovations/hyperqueue/issues/461) When a task is cancelled, times out
  or its worker is killed, HyperQueue now tries to make sure that both the tasks and any processes that
  it has spawned will be also terminated.
* [#480](https://github.com/It4innovations/hyperqueue/issues/480) You can now select multiple tasks in `hq task info`.

## v0.12.0

### New features

#### Automatic allocation

* [#457](https://github.com/It4innovations/hyperqueue/pull/457) You can now specify the idle timeout
  for workers started by the automatic allocator using the `--idle-timeout` flag of the `hq alloc add` command.

#### Resiliency

* [#449](https://github.com/It4innovations/hyperqueue/pull/449) Tasks that were present during multiple
  crashes of the workers will be canceled.

#### CLI

* [#463](https://github.com/It4innovations/hyperqueue/pull/463) You can now wait until `N` workers
  are connected to the clusters with `hq worker wait N`.

#### Python API

* Resource requests improvements in Python API.

### Changes

#### CLI

* [#477](https://github.com/It4innovations/hyperqueue/pull/477) Requested resources are now shown while
  submitting an `array` and while viewing information about task `TASK_ID` of specified
  job `JOB_ID` using `hq task info JOB_ID TASK_ID`

* [#444](https://github.com/It4innovations/hyperqueue/pull/444) The `hq task list` command will now
  hide some details by default, to conserve space in terminal output. To show all details, use the
  `-v` flag to enable verbose output.

* [#455](https://github.com/It4innovations/hyperqueue/pull/445) Improve the quality of error messages
  produced when parsing various CLI parameters, like resources.

#### Automatic allocation

* [#448](https://github.com/It4innovations/hyperqueue/pull/448) The automatic allocator will now start
  workers in multi-node Slurm allocations using `srun --overlap`. This should avoid taking up Slurm
  task resources by the started workers (if possible). If you run into any issues with using `srun`
  inside HyperQueue tasks, please let us know.

#### Jobs

* [#483](https://github.com/It4innovations/hyperqueue/pull/483) There is no longer a length limit
  for job names.

### Fixes

#### Job submission

* [#450](https://github.com/It4innovations/hyperqueue/pull/450) Attempts to resubmit a job with zero
  tasks will now result in an explicit error, rather than a crash of the client.

#### Automatic allocation

* [#494](https://github.com/It4innovations/hyperqueue/pull/494) Fixed a specific issue where the
  auto allocator could submit more allocations than intended.

## v0.11.0

### New features

#### CLI

* [#464](https://github.com/It4innovations/hyperqueue/pull/464) New command was added that allows users
  to see more detailed info about selected task `TASK_ID` from a concrete job `JOB_ID`.
    ```bash
    $ hq task info JOB_ID TASK_ID
    ```

* [#423](https://github.com/It4innovations/hyperqueue/pull/423) You can now specify the server
  directory using the `HQ_SERVER_DIR` environment variable.

#### Resource management

* [#427](https://github.com/It4innovations/hyperqueue/pull/427) A new specifier has been added to
  specify **indexed pool** resources for workers as a set of individual resource indices.
    ```bash
    $ hq worker start --resource "gpus=list(1,3,8)"
    ```
* [#428](https://github.com/It4innovations/hyperqueue/pull/427) Workers will now attempt to automatically
  detect available GPU resources from the `CUDA_VISIBLE_DEVICES` environment variable.

#### Stream log

* Basic export of stream log into JSON (`hq output-log <log_file> export`)

#### Server

* Improved scheduling of multi-node tasks.

* Server now generates a random unique ID (UID) string every time a new server is started (`hq server start`).
  It can be used as a placeholder `%{SERVER_ID}`.

### Changes

#### CLI

* [#464](https://github.com/It4innovations/hyperqueue/pull/464) More detailed task information (Time, Paths)
  were moved from `hq task list` into `hq task info`.
* [#433](https://github.com/It4innovations/hyperqueue/pull/433) (**Backwards incompatible change**)
  The CLI command `hq job tasks` has been removed and its functionality has been incorporated into the
  `hq task list` command instead.
  [resource requests](https://it4innovations.github.io/hyperqueue/stable/jobs/gresources/#resource-request),
* [#420](https://github.com/It4innovations/hyperqueue/pull/420) Shebang (e.g. `#!/bin/bash`) will
  now be read from submitted program based on the provided
  [directives mode](https://it4innovations.github.io/hyperqueue/stable/jobs/directives/). If a shebang
  is found, HQ will execute the program located at the shebang path and pass it the rest of the
  submitted arguments.

  By default, directives and shebang will be read from the submitted program only if its filename ends
  with `.sh`. If you want to explicitly enable reading the shebang, pass `--directives=file` to
  `hq submit`.

  Another change is that the shebang is now read by the client (i.e. it will be read on the node that
  submits the job), not on worker nodes as previously. This means that the submitted file has to be
  accessible on the client node.

#### Resource management

* [#427](https://github.com/It4innovations/hyperqueue/pull/427) (**Backwards incompatible change**)
  The environment variable `HQ_RESOURCE_INDICES_<resource-name>`, which is passed to tasks with
  [resource requests](https://it4innovations.github.io/hyperqueue/stable/jobs/gresources/#resource-request),
  has been renamed to `HQ_RESOURCE_VALUES_<resource-name>`.
* [#427](https://github.com/It4innovations/hyperqueue/pull/427) (**Backwards incompatible change**)
  The specifier for specifying **indexed pool** resources for workers as a range has been renamed from
  `indices` to `range`.

    ```bash
    # before
    $ hq worker start --resource "gpus=indices(1-3)"
    # now
    $ hq worker start --resource "gpus=range(1-3)"
    ```
* [#427](https://github.com/It4innovations/hyperqueue/pull/427) The
  [generic resource](https://it4innovations.github.io/hyperqueue/stable/jobs/gresources/)
  documentation has been rewritten and improved.

## v0.10.0

### New features

#### Running tasks

* HQ will now set the OpenMP `OMP_NUM_THREADS` environment variable for each task. The amount of threads
  will be set according to the number of requested cores. For example, this job submission:

```
$ hq submit --cpus=4 -- <program>
```

would pass `OMP_NUM_THREADS=4` to the executed `<program>`.

* New task OpenMP pinning mode was added. You can now use `--pin=omp` when submitting jobs. This
  CPU pin mode will generate the corresponding `OMP_PLACES` and `OMP_PROC_BIND` environment variables
  to make sure that OpenMP pins its threads to the exact cores allocated by HyperQueue.

* Preview version of multi-node tasks. You may submit multi-node task by ``hq submit --nodes=X ...``

#### CLI

* Less verbose log output by default. You can use "--debug" to turn on the old behavior.

### Changes

#### Scheduler

* When there is only a few tasks, scheduler tries to fit tasks on fewer workers.
  Goal is to enable earlier stopping of workers because of idle timeout.

#### CLI

* The `--pin` boolean option for submitting jobs has been changed to take a value. You can get the
  original behaviour by specifying `--pin=taskset`.

### Fixes

#### Automatic allocation

- PBS/Slurm allocations using multiple workers will now correctly spawn a HyperQueue worker on all
  allocated nodes.

## v0.9.0

### New features

#### Tasks

* Task may be started with a temporary directory that is automatically deleted when the task is finished.
  (flag `--task-dir`).

* Task may provide its own error message by creating a file with name passed by environment variable
  `HQ_ERROR_FILENAME`.

#### CLI

* You can now use the `hq task list <job-selector>` command to display a list of tasks across multiple jobs.
* Add `--filter` flag to `worker list` to allow filtering workers by their status.

### Changes

#### Automatic allocation

* Automatic allocation has been rewritten from scratch. It will no longer query PBS/Slurm allocation
  statuses periodically, instead it will try to derive allocation state from workers that connect
  to it from allocations.
* When adding a new allocation queue, HyperQueue will now try to immediately submit a job into the queue
  to quickly test whether the entered configuration is correct. If you want to avoid this behaviour, you
  can use the `--no-dry-run` flag for `hq alloc add <pbs/slurm>`.
* If too many submissions (10) or running allocations (3) fail in a succession, the corresponding
  allocation queue will be automatically removed to avoid error loops.
* `hq alloc events` command has been removed.
* The `--max-kept-directories` parameter for allocation queues has been removed. HyperQueue will now keep
  `20` last allocation directories amongst all allocation queues.

### Fixes

* HQ will no longer warn that `stdout`/`stderr` path does not contain the `%{TASK_ID}` placeholder
  when submitting array jobs if the placeholder is contained within the working directory path and
  `stdout`/`stderr` contains the `%{CWD}` placeholder.

## v0.8.0

### Fixes

#### Automatic allocation

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

### New features

#### Jobs

* Added new command for outputting `stdout`/`stderr` of jobs.

  ```bash
  # Print stdout of all tasks of job 1
  $ hq job cat 1 stdout

  # Print stderr of tasks 1, 2, 3 of job 5
  $ hq job cat 5 stderr --tasks 1-3
  ```

  You can find more information in
  the [documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/jobs/#display-job-stdoutstderr)
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

* Capturing stdio and attaching it to each task of a job. This can be used to submitting scripts
  without creating file. The following command will capture stdin and executes it in Bash

  ```bash
  $ hq submit --stdin bash
  ```

#### Worker configuration

* You can now select what should happen when a worker loses its connection to the server using the
  new `--on-worker-lost` flag available for `worker start` and `hq alloc add` commands. You can find
  more information in
  the [documentation](https://it4innovations.github.io/hyperqueue/stable/deployment/worker/#lost-connection-to-the-server).

#### CLI

* You can now force HyperQueue commands to output machine-readable data using the `--output-mode` flag
  available to all HyperQueue commands. Notably, you can output data of the commands as JSON. You can
  find more information in the [documentation](https://it4innovations.github.io/hyperqueue/stable/cli/output-mode/).

* You can now generate shell completion using the `hq generate-completion <shell>` command.

### Changes

#### CLI

* The command line interface for jobs has been changed to be more consistent with the interface for
  workers. Commands that have been formerly standalone (like `hq jobs`, `hq resubmit`, `hq wait`) are
  not accessed through `hq job`. The only previous job-related command that remained on the top level
  is `hq submit`, which is now a shortcut for `hq job submit`. Here is a table of changed commands:

  | **Previous command** | **New command**    |
  |----------------------|--------------------|
  | `hq jobs`            | `hq job list`    |
  | `hq job`             | `hq job info`    |
  | `hq resubmit`        | `hq job resubmit` |
  | `hq cancel`          | `hq job cancel`  |
  | `hq wait`            | `hq job wait`    |
  | `hq progress`        | `hq job progress` |
  | `hq submit`          | `hq submit` or `hq job submit` |

* The `--tasks` flag of the `hq job info <job-id>` command has been removed. If you want to display the
  individual tasks of a job, please use the `hq task list <job-id>` command.

* The command line parsing of `hq submit` has been changed slightly. All flags and arguments that appear
  after the first positional argument will now be considered to belong to the executed program, not to
  the submit command. This mimics the behaviour of e.g. `docker run`. For example:
    ```bash
    $ hq submit foo --array 1-4
    # Before: submits a task array with 4 tasks that execute the program `foo`
    # Now: submits a single task that executes `foo --array 1-4`
    ```

* `hq job list` will now only show queued and running jobs by default. You can use the `--all` flag
  to display all jobs or the `--filter` flag to filter jobs that are in specified states.

* The `--status` flag of `hq job resubmit` has been renamed to `--filter`.

* Tables outputted by various informational commands (like `hq job info` or `hq worker list`)
  are now more densely packed and should thus better fit on terminal screens.

### Preview features

* You can now store HyperQueue events into a log file and later export them to JSON for further
  processing. You can find more information in the
  [documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/directives/).

  *Note that this functionality is quite low-level, and it's designed primarily for
  tool builders that use HyperQueue programmatically, not regular users. It is also currently
  unstable.*

* You can now try the preview version of HQ dashboard. It can be started via:

  ```bash
  $ hq dashboard
  ```

## v0.7.0

### Fixes

* Fixes an invalid behavior of the scheduler when resources are defined

* The automatic allocator will no longer keep submitting allocations in situations where the created
  workers would not be able to execute currently waiting tasks. Currently, this situation is detected
  only for the case when a task has a time request higher than the time limit of the allocation
  queue.

### New features

#### Automatic allocation

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

#### Resource management

* Workers can be now started with the parameter `--cpus="no-ht"`. When detecting CPUs in this mode,
  HyperThreading will be ignored (for each physical core only the first HT virtual core will be chosen).
* The user may explicitly specify what CPU IDs should be used by a worker
  (including arrangement of IDs into sockets).
  (E.g. ``hq worker start --cpus=[[0, 1], [6, 8]]``)

#### CLI

* Improve error messages printed when an invalid CLI parameter is entered.

### Changes

* The `--time-limit` parameter of `hq alloc add` command is now required.
* `hq alloc remove` will no longer let you remove an allocation queue that contains running
  allocations by default. If you want to force its removal and cancel the running allocations
  immediately, use the `--force` flag.

## v0.6.1

### Fixes

* Fixed computation of worker load in scheduler
* Fixed performance problem when canceling more than 100k tasks

### Changes

* When a job is submitted, it does not show full details in response
  but only a short message. Details can be still shown by `hq job <id>`.

## v0.6.0

### New features

* Generic resource management has been added. You can find out more in
  the [documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/gresources/).
    * HyperQueue can now automatically detect how many Nvidia GPUs are present on a worker node.
* You can now submit a task array where each task will receive one element of a JSON array using
  `hq submit --from-json`. You can find out more in
  the [documentation](https://it4innovations.github.io/hyperqueue/stable/jobs/arrays/#json-array).

### Changes

* There have been a few slight CLI changes:
    * `hq worker list` no longer has `--offline` and `--online` flags. It will now display only running
      workers by default. If you want to show also offline workers, use the `--all` flag.
    * `hq alloc add` no longer has a required `--queue/--partition` option. The PBS queue/Slurm partition
      should now be passed as a trailing argument after `--`: `hq alloc add pbs -- -qqprod`.
* Server subdirectories generated for each run of the HyperQueue server are now named with a numeric ID instead of
  a date.
* The documentation has been [rewritten](https://it4innovations.github.io/hyperqueue).

## v0.5.0

### New features

* Time limit and Time request for tasks (options ``--time-limit`` and ``--time-request``)
* Time limit for workers
* Job and task times are shown in job information tables
* Integers in command line options can be now written with an underscore separator (e.g. ``--array=1-1_000``)
* Placeholders in log file paths
* Preview version of PBS and SLURM auto allocation
* HyperQueue can be now compiled without `jemalloc` (this enables PowerPC builds).
  To remove dependency on `jemalloc`, build HyperQueue with `--no-default-features`.

### Changes

* `hq submit --wait` and `hq wait` will no longer display a progress bar while waiting for the job(s) to finish.
  The progress bar was moved to `hq submit --progress` and `hq progress`.
* The default path of job stdout and stderr has been changed to ``job-%{JOB_ID}/%{TASK_ID}.[stdout/stderr]``
* Normalization of stream's end behavior when job is canceled
* Job id is now represented as u32

## v0.4.0

### New features

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

## v0.3.0

### New features

* Option for automatic closing workers without tasks (Idle timeout)
* Submit option ``--max-fails X`` to cancel an job when more than X tasks fails
* Submit option ``--each-line FILE`` to create a task per a line in a file.
* Submit option ``--env VAR=VALUE`` to specify env variable in a task
* Submit option ``--cwd DIR`` to specify a working dir of a task
* New placeholders in paths: ``%{CWD}``, ``%{DATE}``, and ``%{SUBMIT_DIR}``
* Added a progressbar in a job array detail.
* ``hq server start --host=xxx`` allows to specify hostname/address under which the server is visible

## v0.2.1

### New features

* Filters for command ``hq jobs <filter>``
  (e.g. ``hq jobs running``)

### Fixes

* NUMA detection on some architectures

## v0.2.0

### New features

* Job arrays
* Cpu management
* --stdout/--stderr configuration in submit

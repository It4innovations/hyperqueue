It is a common use case to execute the same command for multiple input parameters, for example:

- Perform a simulation for each input file in a directory or for each line in a CSV file.
- Train many machine learning models using *hyperparameter search* for each model configuration.

HyperQueue allows you to do this using a [job](jobs.md) that contains many tasks. We call such jobs
**Task arrays**. You can create a task array with a single `submit` command and then manage all created
tasks as a single group using its containing job.

!!! note

    Task arrays are somewhat similar to "job arrays" used by PBS and Slurm. However, *HQ* **does not** use PBS/Slurm job
    arrays for implementing this feature. Therefore, the limits that are commonly enforced on job arrays on HPC clusters
    do not apply to HyperQueue task arrays.

## Creating task arrays
To create a task array, you must provide some **source** that will determine how many tasks should be created and what
inputs (environment variables) should be passed to each task so that you can differentiate them.

Currently, you can create a task array from a [range of integers](#integer-range), from [each line](#lines-of-a-file)
of a text file or from each item of a [JSON array](#json-array). You cannot combine these sources, as they are mutually
exclusive.

!!! tip "Handling many output files"

    By default, each task in a task array will create two output files (containing `stdout` and `stderr` output). Creating
    large task arrays will thus generate a lot of files, which can be problematic especially on network-based shared
    filesystems, such as Lustre. To avoid this, you can either [disable](jobs.md#output) the output or use
    [**Output streaming**](streaming.md).

### Integer range
The simplest way of creating a task array is to specify an integer range. A task will be started for each integer in the
range. You can then differentiate between the individual tasks using [task id](jobs.md#identification-numbers)
that can be accessed through the `HQ_TASK_ID` [environment variable](jobs.md#environment-variables).

You can enter the range as two unsigned numbers separated by a dash[^2], where the first number should be smaller than
the second one. The range is inclusive.

[^2]: The full syntax can be seen in the second selector of the [ID selector shortcut](../cli/shortcuts.md).

The range is entered using the `--array` option:

```bash
# Task array with 3 tasks, with ids 1, 2, 3
$ hq submit --array 1-3 ...

# Task array with 6 tasks, with ids 0, 2, 4, 6, 8, 10
$ hq submit --array 0-10:2 ...
```

### Lines of a file
Another way of creating a task array is to provide a text file with multiple lines. Each line from the file will be
passed to a separate task, which can access the value of the line using the environment variable `HQ_ENTRY`.

This is useful if you want to e.g. process each file inside some directory. You can generate a text file that will
contain each filepath on a separate line and then pass it to the submit command using the `--each-line` option:

```bash
$ hq submit --each-line entries.txt ...
```

!!! tip
    To directly use an environment variable in the submitted command, you have to make sure that it will be expanded
    when the command is executed, not when the command is submitted. You should also execute the command in a bash script
    if you want to specify it directly and not via a script file.
    
    For example, the following command is **incorrect**, as it will expand `HQ_ENTRY` during submission (probably to an
    empty string) and submit a command `ls `:
    ```bash
    $ hq submit --each-line files.txt ls $HQ_ENTRY
    ```
    To actually submit the command `ls $HQ_ENTRY`, you can e.g. wrap the command in apostrophes and run it in a shell:
    ```bash
    $ hq submit --each-line files.txt bash -c 'ls $HQ_ENTRY'
    ```

### JSON array
You can also specify the source using a JSON array stored inside a file. HyperQueue will then create a task for each
item in the array and pass the item as a JSON string to the corresponding task using the environment variable `HQ_ENTRY`.

!!! note

    The root JSON value stored inside the file must be an array.

You can create a task array in this way using the `--from-json` option:

```bash
$ hq submit --from-json items.json ...
```

If `items.json` contained this content:
```json
[{
  "batch_size": 4,
  "learning_rate": 0.01
}, {
  "batch_size": 8,
  "learning_rate": 0.001
}]
```
then HyperQueue would create two tasks, one with `HQ_ENTRY` set to `{"batch_size": 4, "learning_rate": 0.01}`
and the other with `HQ_ENTRY` set to `{"batch_size": 8, "learning_rate": 0.001}`.

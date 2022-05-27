Jobs containing [many tasks](arrays.md) will generate a large amount of `stdout` and `stderr` files, which can be
problematic, especially on network-based shared filesystems, such as Lustre. For example, when you submit the following
task array:

```bash
$ hq submit --array=1-10000 my-computation.sh
```

`20000` files (`10000` for stdout and `10000` for stderr) will be created on the disk.

To avoid this situation, HyperQueue can optionally stream the `stdout` and `stderr` output of all tasks of a job over a
network to the server, which will continuously append it to a single file called the **Log**.

!!! note

    In this section, we refer to `stdout` and `stderr` as **channels**.

## Redirecting output to the log
You can redirect the output of `stdout` and `stderr` to a log file and thus enable output streaming by passing a path
to a filename where the log will be stored with the `--log` option:

```
$ hq submit --log=<log-path> --array=1-10000 ...
```

This command would cause the `stdout` and `stderr` of all `10000` tasks to be streamed into the server, which will write
them to a single file specified in `<log-path>`. The streamed data is stored with additional metadata, which allows the
resulting file to be filtered/sorted by tasks or output type (`stdout`/`stderr`).

!!! tip

    You can use selected [placeholders](jobs.md#placeholders) inside the log path.

### Partial redirection
By default, both `stdout` and `stderr` will be streamed if you specify `--log` and do not specify an explicit path for
`stdout` and `stderr`. To stream only one of the channels, you can use the `--stdout`/`--stderr` options to redirect
one of them to a file or to disable it completely.

For example:

```bash
# Redirecting stdout into a file, streaming stderr into `my-log`
$ hq submit --log=my-log --stdout="stdout-%{TASK_ID}" ...

# Streaming stdout into `my-log`, disabling stderr
$ hq submit --log=my-log --stderr=none ...
```

## Guarantees
HyperQueue provides the following guarantees regarding output streaming:

- When a task is `Finished` or `Failed`, then it is guaranteed\* that its streamed output is fully flushed into the
log file.
- When a task is `Canceled`, then its stream is not necessarily fully written into the log file at the moment
it becomes canceled. Some parts of its output may be written later, but the stream will be eventually closed.
- When a task is `Canceled` or its [time limit](jobs.md#time-management) is reached, then the part of its stream
that was buffered in the worker is dropped to avoid spending additional resources for this task.

    In practice, only output produced immediately before a task is canceled could be dropped, since output data is streamed
    to the server as soon as possible.

\* If the streaming itself failed (e.g. because there was insufficient disk space for the log file), then the task will
fail with an error prefixed with `"Streamer:"` and no further streaming guarantees will be upheld.

### Superseded streams
When a worker crashes while executing a task, the task will be [restarted](failure.md#task-restart). If output streaming
is enabled and the task has already streamed some output data before it was restarted, invalid or duplicate output
could appear in the log.

To avoid mixing outputs from different executions of the same task, when a task is restarted, HyperQueue automatically
marks all output streamed from previous runs of the task as superseded and ignores this output by default.

### Current limitations
The current version does not support streaming the output of multiple jobs into the same file. In other words, if you
submit multiple jobs with the same log filename, like this:

```bash
$ hq submit --log=my-log ...
$ hq submit --log=my-log ...
```

Then the log will contain data from a single job only, the other data will be overwritten.

## Inspecting the log file
HyperQueue lets you inspect the data stored inside the log file using various subcommands. All `log` subcommands have the
following structure:

```bash
$ hq log <log-file-path> <subcommand> <subcommand-args>
```

### Log summary
You can display a summary of a log file using the `summary` subcommand:
```bash
$ hq log <log-file-path> summary
```

### Printing log content
If you want to simply print the (textual) content of the log file, without any associating metadata, you can use the
`cat` subcommand:

```bash
$ hq log <log-file-path> cat <stdout/stderr>
```

It will print the raw content of either `stdout` or `stderr`, ordered by task id. All outputs will be concatenated one
after another. You can use this to process the streamed data e.g. by a postprocessing script.

By default, this command will fail if there is an unfinished stream (i.e. when some task is still running and streaming
data into the log). If you want to use `cat` even when the log is not finished yet, use the `--allow-unfinished` option.

If you want to see the output of a specific task, you can use the `--task=<task-id>` option.

!!! note

    Superseded streams are completely ignored by the `cat` subcommand.

### Log metadata
If you want to inspect the contents of the log, along with its inner metadata that shows which task and which channel
has produced which part of the data, you can use the `show` subcommand:

```bash
$ hq log <log-file-path> show
```

The output will have the form `X:Y> DATA` where `X` is task id and `Y` is `0` for `stdout` channel and `1` for `stderr`
channel.

You can filter a specific channel with the `--channel=stdout/stderr` flag.

By default, HQ does not show stream close metadata from streams that are empty (e.g. when a task did not produce any
output on some channel). You can change that with the flag `--show-empty`.

!!! note

    Superseded streams are completely ignored by the `show` subcommand.

### Exporting log

Log can be exported into JSON by the following command:

```commandline
$ hq log <log-file-path> export
```

This prints the log file into a JSON format on standard output.
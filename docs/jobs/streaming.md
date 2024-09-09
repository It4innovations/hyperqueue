Jobs containing [many tasks](arrays.md) will generate a large amount of `stdout` and `stderr` files, which can be
problematic, especially on network-based shared filesystems, such as Lustre. For example, when you submit the following
task array:

```bash
$ hq submit --array=1-10000 my-computation.sh
```

`20000` files (`10000` for stdout and `10000` for stderr) will be created on the disk.

To avoid this situation, HyperQueue can optionally stream the `stdout` and `stderr` output of
tasks into a compact format that do not create a file per task.

!!! note

    In this section, we refer to `stdout` and `stderr` as **channels**.

<p align="center">
<img width="600" src="../../imgs/streaming.png">
</p>

## Redirecting output to the stream

You can redirect the output of `stdout` and `stderr` to a log file and thus enable output streaming by passing a path
to a filename where the log will be stored with the `--stream` option:

```
$ hq submit --log=<stream-path> --array=1-10_000 ...
```

Stream path has to be a directory and it the user responsibility to ensure existence of the directory
and visibility of each worker.

This command would cause the `stdout` and `stderr` of all `10_000` tasks to be streamed into the server, which will
write them to files in `<stream-path>`. The streamed data is written in a compact way independently on the number of
tasks. The format also contains additional metadata,
which allows the resulting file to be filtered/sorted by tasks or channel.

!!! tip

    You can use selected [placeholders](jobs.md#placeholders) inside the stream path.

### Partial redirection

By default, both `stdout` and `stderr` will be streamed if you specify `--stream` and do not specify an explicit path
for
`stdout` and `stderr`. To stream only one of the channels, you can use the `--stdout`/`--stderr` options to redirect
one of them to a file or to disable it completely.

For example:

```bash
# Redirecting stdout into a file, streaming stderr into `my-log`
$ hq submit --stream=my-log --stdout="stdout-%{TASK_ID}" ...

# Streaming stdout into `my-log`, disabling stderr
$ hq submit --stream=my-log --stderr=none ...
```

## Guarantees

HyperQueue provides the following guarantees regarding output streaming:

When a task is `Finished` or `Failed` it is guaranteed that all data produced by the task is flushed into the
streaming file. With the following two exceptions:

- If the streaming itself fails (e.g. because there was insufficient disk space for the
  stream file), then the task will fail with an error prefixed with `"Streamer:"` and no streaming guarantees
  will be upheld.

- When a task is `Canceled` or task fails because of [time limit](jobs.md#time-management) is reached, then the part of
  its stream that was buffered in the worker is dropped to avoid spending additional resources for this task.

## Inspecting the stream files

HyperQueue lets you inspect the data stored inside the stream file using various subcommands. All these commands have
the following structure:

```bash
$ hq log <stream-path> <subcommand> <subcommand-args>
```

### Stream summary

You can display a summary of a log file using the `summary` subcommand:

```bash
$ hq log <stream-path> summary
```

### Stream jobs

To print all job IDs that streaming in the stream path, you can run the following command:

```bash
$ hq log <stream-path> jobs
```

### Printing stream content

If you want to simply print the (textual) content of the log file, without any associating metadata, you can use the
`cat` subcommand:

```bash
$ hq read <stream-path> cat <job-id> <stdout/stderr>
```

It will print the raw content of either `stdout` or `stderr`, ordered by task id. All outputs will be concatenated one
after another. You can use this to process the streamed data e.g. by a postprocessing script.

By default, this command will fail if there is an unfinished stream (i.e. when some task is still running and streaming
data into the log). If you want to use `cat` even when the log is not finished yet, use the `--allow-unfinished` option.

If you want to see the output of a specific task, you can use the `--task=<task-id>` option.

### Stream metadata

If you want to inspect the contents of the log, along with its inner metadata that shows which task and which channel
has produced which part of the data, you can use the `show` subcommand:

```commandline
$ hq read <log-file-path> show
```

The output will have the form `J.T:C> DATA` where `J` is a job id, `T` is a task id and `C` is `0` for `stdout` channel
and `1` for `stderr` channel.

You can filter a specific channel with the `--channel=stdout/stderr` flag.

### Exporting log

Log can be exported into JSON by the following command:

```commandline
$ hq read <log-file-path> export
```

This prints the log file into a JSON format on standard output.

### Superseded streams

When a worker crashes while executing a task, the task will be [restarted](failure.md#task-restart).
HyperQueue gives each run of task a difference INSTANCE_ID, and it is a part of stream metadata,
hence HyperQueue streaming is able to avoid mixing
outputs from different executions of the same task, when a task is restarted.

HyperQueue automatically marks all output from previous instance of a task except the last instance as *superseded*.
You can see statistics about superseded data via `hq read <stream-path> summary` command.
In the current version, superseded data is ignored by all other commands.

## More server instances

HyperQueue supports writing streams from the different server instances into the same directory.
If you run `hq read` commands over such directory then it will detect the situation and prints all server uids
that writes into the directory. You have to specify the server instance via `hq read --server-uid=<SERVER_UID> ...`
when working with such a stream directory.

!!! note

    When a server is restored from a journal file, it will maintain the same server UID. When a server is 
    started "from a scratch" a new server uid is generated.

## Working with non-shared file system

You do not need to have a shared file system when working with streaming path. It is just your responsibility to
collect all generated files into one directory before using `hq read` commands.
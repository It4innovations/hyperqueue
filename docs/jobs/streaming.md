Jobs containing [many tasks](arrays.md) will generate a large amount of `stdout` and `stderr` files, which can cause performance issues, especially on network-based shared filesystems, such as Lustre. For example, when you submit the following task array:

```bash
$ hq submit --array=1-10000 my-computation.sh
```

`20000` files (`10000` for stdout and `10000` for stderr) will be created on the disk.

To avoid this issue, HyperQueue can optionally stream the `stdout` and `stderr` output of
tasks into a smaller number of files stored in a compact binary format.

!!! note

    In this section, we refer to `stdout` and `stderr` as **channels**.

<p align="center">
<img width="600" src="../../imgs/streaming.png">
</p>

## Redirecting task output

You can enable output streaming by using the `--stream` option of the `submit` command. You should pass it a path to a directory on disk where the streamed `stdout` and `stderr` output will be stored.

```
$ hq submit --stream=<stream-dir> --array=1-10_000 ...
```

!!! warning

    It is the user's responsibility to ensure that the `<stream-dir>` path is accessible and writable by each worker that might execute tasks of the submitted job. See also [Working with a non-shared filesystem](#working-with-a-non-shared-file-system).

The command above will cause the `stdout` and `stderr` of all `10_000` tasks to be streamed in a compact way into a small number of files located in `<stream-dir>`. Note that the number of files created in the directory will be independent of the number of tasks of the job, thus alleviating the performance issue on networked filesystems. The created binary files will also contain additional metadata, which allows the resulting files to be filtered/sorted by tasks or channel.

!!! tip

    You can use selected [placeholders](jobs.md#placeholders) inside the stream directory path.

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

## Inspecting the stream data

HyperQueue lets you inspect the data stored inside the stream directory using various subcommands. All these commands have the following structure:

```bash
$ hq output-log <stream-dir> <subcommand> <subcommand-args>
```

### Stream summary

You can display a summary of a stream directory using the `summary` subcommand:

```bash
$ hq output-log <stream-dir> summary
```

### Stream jobs

To print all job IDs that streaming in the stream directory, you can run the following command:

```bash
$ hq output-log <stream-dir> jobs
```

### Printing stream content

If you want to simply print the (textual) content of the stream directory contents, without any associating metadata, you can use the `cat` subcommand:

```bash
$ hq output-log <stream-dir> cat <job-id> <stdout/stderr>
```

It will print the raw content of either `stdout` or `stderr`, ordered by task id. All outputs will be concatenated one
after another. You can use this to process the streamed data e.g. by a postprocessing script.

By default, this command will fail if there is an unfinished stream (i.e. when some task is still running and streaming
data into the streaming directory). If you want to use `cat` even when streaming has not finished yet, use the `--allow-unfinished` option.

If you want to see the output of a specific task, you can use the `--task=<task-id>` option.

### Stream metadata

If you want to inspect the contents of the stream directory along with its inner metadata that shows which task and which channel
has produced which part of the data, you can use the `show` subcommand:

```bash
$ hq output-log <stream-directory> show
```

The output will have the form `J.T:C> DATA` where `J` is a job id, `T` is a task id and `C` is `0` for `stdout` channel
and `1` for `stderr` channel.

You can filter a specific channel with the `--channel=stdout/stderr` flag.

### Exporting the stream data

The contents of the stream directory can be exported into JSON by the following command:

```bash
$ hq output-log <stream-dir> export
```

This prints the streamed data into a JSON format to standard output.

### Superseded streams

When a worker crashes while executing a task, the task will be [restarted](failure.md#task-restart).
HyperQueue gives each run of task a difference INSTANCE_ID, and it is a part of stream metadata,
hence HyperQueue streaming is able to avoid mixing
outputs from different executions of the same task, when a task is restarted.

HyperQueue automatically marks all output from previous instance of a task except the last instance as *superseded*.
You can see statistics about superseded data via `hq output-log <stream-dir> summary` command.
In the current version, superseded data is ignored by all other commands.

## Multiple server instances

HyperQueue supports writing streams from the different server instances into the same directory.
If you run `hq output-log` commands over such directory then it will detect the situation and print all server UIDs
that write into the directory. You have to specify the server instance
via `hq output-log --server-uid=<SERVER_UID> ...`
when working with such a streaming directory.

!!! note

    When a server is restored from a journal file, it will maintain the same server UID. When a server is started "from a scratch" a new server UID is generated.

## Working with a non-shared filesystem

You do not need to have a shared filesystem when working with streaming. You just have to collect all generated files from the streaming directories in the different filesystems into a single directory before using the `hq output-log` commands.

For example, you could use `/tmp/hq-stream` as a stream directory, which can be a local disk path on each worker, and then merge the contents of all such directories and use `hq output-log` on the resulting merged directory.

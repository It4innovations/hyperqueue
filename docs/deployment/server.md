The server is a crucial component of HyperQueue which manages [workers](worker.md) and [jobs](../jobs/jobs.md). Before
running
any computations or deploying workers, you must first start the server.

## Starting the server

The server can be started by running the following command:

```bash
$ hq server start
```

You can change the hostname under which the server is visible to workers with the --host option:

```bash
$ hq server start --host=HOST
```

## Server directory

When the server is started, it creates a **server directory** where it stores information needed for
submitting [jobs](../jobs/jobs.md)
and connecting [workers](worker.md). This directory is then used to select a running HyperQueue instance.

By default, the server directory will be stored in `$HOME/.hq-server`. This location may be changed with the option
`--server-dir=<PATH>`, which is available for all HyperQueue CLI commands. You can run more instances of HyperQueue
under
the same Unix user, by making them use different server directories.

If you use a non-default server directory, make sure to pass the same `--server-dir` to all HyperQueue commands that
should use the selected HyperQueue server:

```bash
$ hq --server-dir=foo server start &
$ hq --server-dir=foo worker start
```

!!! tip

    To avoid having to pass the `--server-dir` parameter to all `hq` commands separately, you can also pass it through the`HQ_SERVER_DIR` environment variable, and export it to share it for all commands in the same terminal session:
    ```bash
    $ export HQ_SERVER_DIR=bar
    $ hq server start &
    $ hq worker start &
    ```

!!! important

    When you start the server, it will create a new subdirectory in the server directory, which will store the data of the current running instance. It will also create a symlink `hq-current` which will point to the currently active
    subdirectory.
    Using this approach, you can start a server using the same server directory multiple times without overwriting data
    of the previous runs.

!!! danger "Server directory access"

    Encryption keys are stored in the server directory. Whoever has access to the server directory may submit jobs,
    connect workers to the server and decrypt communication between HyperQueue components. By default, the directory is
    only accessible by the user who started the server.

## Keeping the server alive

The server is supposed to be a long-lived component. If you shut it down, all workers will disconnect and all
computations
will be stopped. Therefore, it is important to make sure that the server will stay running e.g. even after you
disconnect from a cluster where the server is deployed.

For example, if you SSH into a login node of an HPC cluster and then run the server like this:

```bash
$ hq server start
```

The server will quit when your SSH session ends, because it will receive a SIGHUP signal. You can use established Unix
approaches to avoid this behavior, for example prepending the command with [nohup](https://en.wikipedia.org/wiki/Nohup)
or using a terminal multiplexer like [tmux](https://en.wikipedia.org/wiki/Tmux).

## Resuming stopped/crashed server

The server supports resilience, which allows it to restore its state after it is stopped or if it crashes. To enable
resilience, you can tell the server to log events into a *journal* file, using the `--journal` flag:

```bash
$ hq server start --journal /path/to/journal
```

If the server is stopped or it crashes, and you use the same command to start the server (using the same journal file
path), it will continue from the last point:

```bash
$ hq server start --journal /path/to/journal
```

This functionality restores the state of jobs and automatic allocation queues.
However, it does not restore worker connections; in the current version, new workers
have to be connected to the server after it restarts.

!!! warning

    If the server crashes, the last few seconds of progress may be lost. For example,
    when a task is finished and the server crashes before the journal is written, then
    after resuming the server, the task will be not be computed after a server restart.

### Exporting journal events

If you'd like to programmatically analyze events that are stored in the journal file, you can
export them to JSON using the following command:

```bash
$ hq journal export <journal-path>
```

The events will be read from the provided journal and printed to `stdout` encoded in JSON, one
event per line (this corresponds to line-delimited JSON, i.e. [NDJSON](http://ndjson.org/)).

You can also directly stream events in real-time from the server using the following command:

```bash
$ hq journal stream
```

!!! warning

    The JSON format of the journal events and their definition is currently unstable and can change
    with a new HyperQueue version.

### Pruning journal

Command `hq journal prune` removes all completed jobs and disconnected workers from the journal file.

### Flushing journal

Command `hq journal flush` will force the server to flush the journal.
It is mainly for the testing purpose or if you are going to `hq journal export` on
a live journal (however, it is usually better to use `hq journal stream`).

## Stopping server

You can stop a running server with the following command:

```bash
$ hq server stop
```

When a server is stopped, all running jobs and connected workers will be immediately stopped.
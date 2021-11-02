The server is a crucial component of HyperQueue which manages [workers](worker.md) and [jobs](../jobs/jobs.md). Before running
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
When the server is started, it creates a **server directory** where it stores information needed for submitting [jobs](../jobs/jobs.md)
and connecting [workers](worker.md). This directory is then used to select a running HyperQueue instance.

By default, the server directory will be stored in `$HOME/.hq-server`. This location may be changed with the option
`--server-dir=<PATH>`, which is available for all HyperQueue CLI commands. You can run more instances of HyperQueue under
the same Unix user, by making them use different server directories.

If you use a non-default server directory, make sure to pass the same `--server-dir` to all HyperQueue commands that
should use the selected HyperQueue server:

```bash
$ hq --server-dir=foo server start
$ hq --server-dir=foo worker start
```

!!! important
    When you start the server, it will create a new subdirectory in the server directory, which will store the data of the
    current running instance. It will also create a symlink `hq-current` which will point to the currently active subdirectory.
    Using this approach, you can start a server using the same server directory multiple times without overwriting data
    of the previous runs.

!!! danger "Server directory access"

    Encryption keys are stored in the server directory. Whoever has access to the server directory may submit jobs,
    connect workers to the server and decrypt communication between HyperQueue components. By default, the directory is
    only accessible by the user who started the server.

## Keeping the server alive
The server is supposed to be a long-lived component. If you shut it down, all workers will disconnect and all computations
will be stopped. Therefore, it is important to make sure that the server will stay running e.g. even after you disconnect
from a cluster where the server is deployed.

For example, if you SSH into a login node of an HPC cluster and then run the server like this:

```bash
$ hq server start
```

The server will quit when your SSH session ends, because it will receive a SIGHUP signal. You can use established Unix
approaches to avoid this behavior, for example prepending the command with [nohup](https://en.wikipedia.org/wiki/Nohup)
or using a terminal multiplexer like [tmux](https://en.wikipedia.org/wiki/Tmux).

## Stopping server
You can stop a running server with the following command:

```bash
$ hq server stop
```

When a server is stopped, all running jobs and connected workers will be immediately stopped.

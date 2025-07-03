By default, HyperQueue CLI commands output information in a human-readable way, usually in the form
of a table. If you want to use the CLI commands programmatically, HyperQueue offers two additional
output modes that are designed to be machine-readable.

You can change the output type of any HyperQueue CLI command either by using the `--output-mode` flag
or by setting the `HQ_OUTPUT_MODE` environment variable.

=== "Flag"

    ```bash
    $ hq --output-mode=json job list
    ```

=== "Environment variable"

    ``` bash
    $ HQ_OUTPUT_MODE=json hq job list
    ```

Currently, there are three output modes available. The default, human-readable `cli` mode, and then
two machine-readable modes, [JSON](#json) and [Quiet](#quiet).

!!! important
    Each machine-readable mode supports a set of commands. You can also use commands that are not
    listed here, but their output might be unstable, or they might not output anything for a given
    output mode.

## JSON
The `json` output mode is intended to provide very detailed information in the form of a JSON value.
With this mode, HyperQueue will always output exactly one JSON value, either an array or an object.

!!! warning

    The JSON output is currently unstable and can change with every new HyperQueue version. It can also
    be incomplete and might not contain all information outputted by the human-readable `cli` mode.
    The documentation below can be inaccurate, please examine [source code](https://github.com/It4innovations/hyperqueue/blob/main/crates/hyperqueue/src/client/output/json.rs) for more details.

### Error handling
When an error occurs during the execution of a command, the program will exit with exit code `1`
and the program will output a JSON object with a single `error` key containing a human-readable
description of the error.

### Date formatting
Time-based items are formatted in the following way:

- **Duration** - formatted as a floating point number of seconds.
- **Datetime (timestamp)** - formatted as a `ISO8601` date in UTC

### Supported commands
- Server info: [`hq server info`](cli:hq.server.info)

    ??? Example
        ```json
        {
          "host": "my-machine",
          "hq_port": 42189,
          "pid": 32586,
          "server_dir": "/foo/bar/.hq-server",
          "start_date": "2021-12-20T08:45:41.775753188Z",
          "version": "0.7.0",
          "worker_port": 38627
        }
        ```

- Worker list: [`hq worker list`](cli:hq.worker.list)

    ??? Example
        ```json
        [{
          "configuration": {
            "heartbeat_interval": 8.0,
            "hostname": "my-machine",
            "idle_timeout": null,
            "listen_address": "my-machine:45611",
            "log_dir": "...",
            "resources": {
              "cpus": [[0, 1, 2, 3]],
              "generic": [{
                "kind": "sum",
                "name": "resource1",
                "params": {
                  "size": 1000
                }
              }]
            },
            "time_limit": null,
            "work_dir": "..."
          },
          "ended": null,
          "id": 1
        }]
        ```

- Worker info: [`hq worker info`](cli:hq.worker.info)

    ??? Example
        ```json
        {
          "configuration": {
            "heartbeat_interval": 8.0,
            "hostname": "my-machine",
            "idle_timeout": null,
            "listen_address": "my-machine:45611",
            "log_dir": "...",
            "resources": {
              "cpus": [[0, 1, 2, 3]],
              "generic": [{
                "kind": "sum",
                "name": "resource1",
                "params": {
                  "size": 1000
                }
              }]
            },
            "time_limit": null,
            "work_dir": "..."
          },
          "ended": null,
          "id": 1
        }
        ```

- Submit a job: [`hq submit`](cli:hq.submit)

    ??? Example
        ```json
        {
          "id": 1
        }
        ```

- Job list: [`hq job list`](cli:hq.job.list)

    ??? Example
        ```json
        [{
          "id": 1,
          "name": "ls",
          "resources": {
            "cpus": {
              "cpus": 1,
              "type": "compact"
            },
            "generic": [],
              "min_time": 0.0
            },
          "task_count": 1,
          "task_stats": {
            "canceled": 0,
            "failed": 0,
            "finished": 1,
            "running": 0,
            "waiting": 0
          }
        }]
        ```

- Job info: [`hq job info`](cli:hq.job.info)

    ??? Example
        ```json
        {
          "finished_at": "2021-12-20T08:56:16.438062340Z",
          "info": {
            "id": 1,
            "name": "ls",
            "resources": {
              "cpus": {
                "cpus": 1,
                "type": "compact"
              },
              "generic": [],
                "min_time": 0.0
              },
            "task_count": 1,
            "task_stats": {
              "canceled": 0,
              "failed": 0,
              "finished": 1,
              "running": 0,
              "waiting": 0
            }
          },
          "max_fails": null,
          "pin": null,
          "priority": 0,
          "program": {
            "args": [
              "ls"
            ],
            "cwd": "%{SUBMIT_DIR}",
            "env": {
              "FOO": "BAR"
            },
            "stderr": {
              "File": "job-%{JOB_ID}/%{TASK_ID}.stderr"
            },
            "stdout": {
              "File": "job-%{JOB_ID}/%{TASK_ID}.stdout"
            }
          },
          "started_at": "2021-12-20T08:45:53.458919345Z",
          "tasks": [{
            "finished_at": "2021-12-20T08:56:16.438062340Z",
            "id": 0,
            "started_at": "2021-12-20T08:56:16.437123396Z",
            "state": "finished",
            "worker": 1,
            "cwd": "/tmp/foo",
            "stderr": {
              "File": "job-1/0.stderr"
            },
            "stdout": {
              "File": "job-1/0.stdout"
            }
          }],
          "time_limit": null,
          "submit_dir": "/foo/bar/submit"
        }
        ```

- Automatic allocation queue list: [`hq alloc list`](cli:hq.alloc.list)

    ??? Example
        ```json
        [{
          "additional_args": [],
          "backlog": 4,
          "id": 1,
          "manager": "PBS",
          "max_worker_count": null,
          "name": null,
          "timelimit": 1800.0,
          "worker_cpu_args": null,
          "worker_resource_args": [],
          "workers_per_alloc": 1
        }]
        ```

- Automatic allocation queue info: [`hq alloc info`](cli:hq.alloc.info)

    ??? Example
        ```json
        [{
          "id": "pbs-1",
          "worker_count": 4,
          "queue_at": "2021-12-20T08:56:16.437123396Z",
          "started_at": "2021-12-20T08:58:25.538001256Z",
          "ended_at": null,
          "status": "running",
          "workdir": "/foo/bar"
        }]
        ```

## Quiet
The `quiet` output mode will cause HyperQueue to output only the most important information that
should be parseable without any complex parsing logic, e.g. using only Bash scripts.

### Error handling
When an error occurs during the execution of a command, the program will exit with exit code `1`
and the error will be printed to the standard error output.

### Supported commands
- Submit a job: [`hq submit`](cli:hq.submit)

    ??? Schema
        Outputs a single line containing the ID of the created job.

    ??? Example
        ```bash
        $ hq --output-mode=quiet submit ls
        1
        ```

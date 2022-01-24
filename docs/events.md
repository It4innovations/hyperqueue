# Events
HyperQueue internally records various events that describe what has happened during the lifetime of
the HQ cluster (worker has connected, task was finished, an allocation was submitted to PBS, etc.).
These events might be useful for some power-users, for example to analyze task execution statistics
or allocation durations.

To access these events, you have to start the HyperQueue [server](deployment/server.md) with the
`--event-log-path` option:

```bash
$ hq server start --event-log-path=events.bin
```

If you use this flag, HQ will continuously stream its events into a log file at the provided path.
The events are serialized using a compressed binary encoding. To access the event data from the log
file, you first have to export them.

## JSON export
To export data from the log file to JSON, you can use the following command:

```bash
$ hq event-log export <event-log-path>
```

The events will be read from the provided log file and printed to `stdout` encoded in JSON, one
event per line (this corresponds to line-delimited JSON, i.e. [NDJSON](http://ndjson.org/)).

!!! warning

    The JSON format of the events and their definition is currently unstable and can change
    with a new HyperQueue version.

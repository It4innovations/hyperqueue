# Streaming

The main goal of streaming is to avoid creating many files when a large array job is submitted. For example
when you submit:

``hq submit --array=1-10000 my-computation.sh``

will create 20000 files (10000 for stdout and 10000 for stderr).

This can be solved by using ``--log`` option:

``hq submit --log=<LOG_FILENAME> --array=1-10000 my-computation.sh``

All stdout and stderr streams will be streamed into server and into a single file specified via <LOG_FILENAME>.
The streamed data is stored with additional metadata, so the resulting file can be filtered/sorted by tasks or type of stream.

# Superseded streams

It may happen that a task started a stream but the worker where the task is running crashed.
In such situation, the scheduler starts a task on another worker and it produces a new stream.
To avoid mixing outputs from different runs, HyperQueue automatically marks all previous runs as superseded and ignores them by default.

# Log `summary`

``hq log <LOG_FILENAME> summary``

Displays summary of the log.


# Log `show`

``hq log <LOG_FILENAME> show`` show the content of the log with metadata printed in the order how it was received by the server.

Prompt has form ``X:Y> `` where X is task id and Y is 0 for stdout and 1 for stderr.

You can filter only stdout/stderr stream via ``--channel=X`` where X is ``stdout`` or ``stderr``.

By default, HQ does not show closing information from streams that are empty, you can change that with the flag ``--show-empty``.

Note: Superseded streams are completely ignored by ``show`` command.


# Log `cat`

``hq log <LOG_FILENAME> cat <stdout/stderr>``

prints raw content of stdout or stderr ordered by task id.
All outputs are concatenated one after another.

By default, this command will fail if there is an unfinished stream (i.e. a task is still running). If you want to use ``cat`` even with running tasks, use option ``--allow-unfinished``.

If you want to see only output of a specific task, use option ``--task=<task_id>``


Note: Superseded streams are completely ignored by ``cat`` command.

# Partial redirection

If you want to stream only one channel and redirect the other one into a file, you can still use ``--stdout`` / ``--stderr`` options.


Redirecting stdout into a file, stderr will be streamed into ``my-log``.

``hq submit --log=my-log --stdout=stdout-%{TASK_ID} ...``

Disabling stderr and streaming only stdout into log file.

``hq submit --log=my-log --stderr=none ...``


# Guarantees

When a task is *finished* or *failed* with a non-streaming error then it is guaranteed that its stream is fully flushed into the log file.

When a task is *canceled* or *failed* with a streaming error, then the stream is not necessarily fully written into the log file in the moment when the state occurs
 and some part may be written later, but the stream will be eventually closed. In this case, HQ is also allowed to drop any suffix of the buffered part of the stream.


# Current limitations

The current version does not support streaming of more jobs into the same file,

In other words, if you submit more jobs with the same log filename, like this:

```
hq submit --log=my-log ...
hq submit --log=my-log ...
```

Then log will contain only data from one job and other will be overwritten.

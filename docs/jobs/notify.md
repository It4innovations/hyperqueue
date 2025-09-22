# Task Notifications

HyperQueue contains a mechanism for task notifications that can be used to
notify users that some user-defined event happens.

Properties of notifications:

* A task may send an arbitrary number of notifications.
* Each notification may contain a message with a size limit of 1KiB.
* The system propagates notifications to listening clients while preserving send order within each task. However, no
  ordering is guaranteed across notifications from different tasks.
* Notifications are not persisted. They are never stored in a journal; they are never replayed.
  They are just sent to listening clients (if there are any) and then immediately discarded.
* Notifications can be observed in the event live stream, but not in event stream replayed from a journal.

## CLI

The task notification is sent via calling `hq task notify <message>` inside a task.

The task notification is handled via `--on-notify=<COMMAND>` option used in the submit command.
The `<COMMAND>` is invoked for each incoming notification. You may also pass more arguments, e.g.
`"--on-notify=python3 handle.py"`.

## Example

### Task producing notifications (`task.sh`):

```bash
./compute-something
$HQ task notify "Phase 1 finished"
./compute-something2
```

Note: Variable `$HQ` contains a path to the `hq` executable that executed the task.

### The script that reacts on incoming notifications (`handle.sh`):

The client script that is invoked for each incoming notification.

```bash
echo "Notification from job: $HQ_JOB_ID: $0."
```

The message of the notification is passed as an argument to the script.
There are also variables `$HQ_JOB_ID`, `$HQ_TASK_ID`, and `$HQ_WORKER_ID` that contain information
about the task that sent the notification.

### Submitting the job:

```bash
hq submit --on-notify=handle.sh -- task.sh
```


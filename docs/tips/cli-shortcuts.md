Various HyperQueue CLI command options let you enter some value in a specific syntactical format for convenience. Here
you can find a list of such shortcuts.

## ID selector
When you enter (job/task/worker) IDs to various HyperQueue CLI commands, you can use the following **selectors** to
select multiple IDs at once or to reference the most recently created ID:

- `<id>` Single ID
    - `hq worker stop 1` - stop a worker with ID `1`
    - `hq cancel 5` - cancel a job with ID `5`
- `<start>-<end>:<step>` Inclusive range of IDs, starting at `start` and ending at `end` with step `step`
    - `hq submit --array=1-10` - create a task array with `10` tasks
    - `hq worker stop 1-3` - stop workers with IDs `1`, `2` and `3`
    - `hq cancel 2-10:2` - cancel jobs with IDs `2`, `4`, `6`, `8` and `10`
- `all` All valid IDs
    - `hq worker stop all` - stop all workers
    - `hq cancel all` - cancel all jobs
- `last` The most recently created ID
    - `hq worker stop last` - stop most recently connected worker 
    - `hq cancel last` - cancel most recently submitted job

You can also combine the first two types of selectors with a comma. For example, the command

```
$ hq worker stop 1,3,5-8
```

would stop workers with IDs `1`, `3`, `5`, `6`, `7` and `8`.

### Supported commands and options
- `hq submit --array=<selector>`
- `hq worker stop <selector>`
- `hq job <selector>`
    - does not support `all` (use `hq jobs` instead)
- `hq cancel <selector>`
- `hq wait <selector>`
- `hq progress <selector>`

## Duration
You can enter durations using various time suffixes, for example:

- `1h` - one hour
- `3m` - three minutes
- `14s` - fourteen seconds
- `15days 2min 2s` - fifteen days, two minutes and two seconds

You can also combine these suffixed values together by separating them with a space. The full specification of allowed
suffixed can be found [here](https://docs.rs/humantime/2.1.0/humantime/fn.parse_duration.html).

### Supported commands and options
- `hq worker start --time-limit=<duration>`
- `hq worker start --idle-timeout=<duration>`
- `hq alloc add pbs --time-limit=<duration>`
- `hq submit --time-limit=<duration> ...`
- `hq submit --time-request=<duration> ...`

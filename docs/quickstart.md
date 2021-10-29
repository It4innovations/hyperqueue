Here we provide a "Hello world" example of deploying *HyperQueue* on a local computer.

## Hello world example
Run each of the following three commands in separate terminals.

**1) Start the HyperQueue server**

```bash
$ hq server start
```

The server will manage computing resources (workers) and distribute submitted tasks amongst them.

**2) Start a HyperQueue worker**

```bash
$ hq worker start
```

The worker will connect to the server and execute submitted tasks.

**3) Submit a simple computation**

```bash
$ hq submit echo "Hello world"
```

This command will submit a task that will execute `echo "Hello world"` on a connected worker. You can find the output of
the task in `job-1/0.stdout`.

That's it! For a more in-depth explanation of how HyperQueue works and what it can do, check the [Deployment](setup.md)
and [Jobs](jobs.md) sections.

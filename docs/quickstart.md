Here we provide an example of deploying *HyperQueue* on a local computer and running a simple "Hello world" script.

Run each of the following three commands in separate terminals.

1. **Start the HyperQueue server**

    ```bash
    $ hq server start
    ```

    The [server](deployment/server.md) will manage computing resources (workers) and distribute submitted tasks amongst
    them.

2. **Start a HyperQueue worker**

    ```bash
    $ hq worker start
    ```

    The [worker](deployment/worker.md) will connect to the server and execute submitted tasks.

3. **Submit a simple computation**

    ```bash
    $ hq submit echo "Hello world"
    ```

    This command will submit a [job](jobs) with a single task that will execute `echo "Hello world"` on a worker. You
    can find the output of the task in `job-1/0.stdout`.

That's it! For a more in-depth explanation of how HyperQueue works and what it can do, check
the [Deployment](deployment/index.md) and [Jobs](jobs/jobs.md) sections.

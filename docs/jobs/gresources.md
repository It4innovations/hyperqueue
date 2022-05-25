## Generic resource management

Generic resource management serves for defining arbitrary resources provided by workers and
also corresponding **resource requests** required by tasks. HyperQueue will take care of matching
task resource requests so that only workers that can fulfill them will be able to execute such tasks.

Some generic resources are [automatically detected](#automatically-detected-resources); however,
users may also define their own resources.

!!! important

    Generic resources in HyperQueue exist on a purely logical level. They can correspond to physical
    things (like GPUs), but it is the responsibility of the user to make sure that this correspondence
    makes sense. HyperQueue by itself does not attach any semantics to generic resources, they are
    just numbers used for scheduling.

## Worker resources

Each worker can have several generic resources attached. Each generic resource is a **resource pool**
identified by a name. A resource pool represents some resources provided by a worker; each task can
then ask for a part of resources contained in that pool.

There are two kind of resource pools:

* **Indexed pool**: This pool represents an enumerated set of resources represented by integers.
Each resource has its own identity. Tasks do not ask for specific values from the set, they just specify
how many resources do they require and HyperQueue will allocate the specified amount of resources
from the pool for each task.

    This pool is useful for resources that have their own identity, for example individual GPU or
    FPGA accelerators.

    HyperQueue guarantees that no individual resource from the indexed pool is allocated to more than
    a single task at any given time and that a task will not be executed on a worker if it does not
    currently have enough individual resources to fulfill the [resource request](#resource-request)
    of the task.

* **Sum pool**: This pool represents a resource that has a certain size which be split into individual
tasks. A typical example is memory; if a worker has `2000` bytes of memory, it can serve e.g. four
tasks, if each task asks for `500` bytes of memory.

    HyperQueue guarantees that the sum of resource request sizes of *running* tasks on a worker does
    not exceed the total size of the sum pool.

### Specifying worker resources

You can specify the resource pools of a worker when you start it:

```
$ hq worker start --resource "<NAME1>=<DEF1>" --resource "<NAME2>=<DEF2>" ...
```
where `NAMEi` is a name (string ) of the `i`-th resource pool and `DEFi` is a definition of the
`i-th` resource pool. You can define resource pools using one of the following formats:

* ``list(<VALUE0>,<VALUE1>,...,<VALUEN>)`` where ``VALUEi`` is a non-negative integer. This will
create an indexed pool containing the specified values.
* ``range(<START>-<END>)`` where ``START`` and ``END`` are non-negative integers. This will create
an indexed pool with numbers in the inclusive range `[START, END]`.
* ``sum(<SIZE>)`` where ``SIZE`` is a positive integer. This will create a sum pool with the given
size.

!!! tip

    You might encounter a problem in your shell when you try to specify worker resources, because
    the definition contains parentheses (`()`). In that case just wrap the resource definition in
    quotes, like this:

    ```bash
    $ hq worker start --resources "foo=sum(5)"
    ```

### Automatically detected resources

* Nvidia GPUs that are available when a worker is started are automatically detected under the resource
name `gpus`. You can use the environment variable `CUDA_VISIBLE_DEVICES` when starting a worker to
override the list of available GPUs:

```bash
$ CUDA_VISIBLE_DEVICES=2,3 hq worker start
```

## Resource request

When you submit a job, you can define a **resource requests** with the `--resource` flag:

```bash
$ hq submit --resource <NAME1>=<AMOUNT1> --resources <NAME2>=<AMOUNT2> ...
```

Where `NAME` is a name of the requested resource and the `AMOUNT` is a positive integer defining the
size of the request.

Tasks with such resource requests will only be executed on workers that fulfill all the specified
task requests.

!!! important

    Notice that task resource requests always ask for an amount of resources required by a task,
    regardless whether that resource corresponds to an indexed or a sum pool on workers.

    For example, let's say that a worker has an indexed pool of GPUs:
    ```bash
    $ hq worker start --resource "gpus=range(1-3)"
    ```
    And we create two jobs, each with a single task. The first job wants 1 GPU, the second one wants
    two GPUs.

    ```bash
    $ hq submit --resource gpus=1 ...
    $ hq submit --resource gpus=2 ...
    ```

    Then the first job can be allocated e.g. the GPU `2` and the second job can be allocated the GPUs
    `1` and `3`. 

### Resource environment variables
When a task that has resource requests is executed, the following variables are passed to it for
each resource request named `<NAME>`:

* `HQ_RESOURCE_REQUEST_<NAME>` contains the amount of requested resources.
* `HQ_RESOURCE_VALUES_<NAME>` contains the specific resource values allocated for the task as a
comma-separated list. This variable is only filled for indexed resource pool.

!!! tip

    HQ has a special case for a resource named `gpus`. For that resource, it will also pass the following
    environment variables to the spawned task:

    * `CUDA_DEVICE_ORDER` set to the value `PCI_BUS_ID`
    * `CUDA_VISIBLE_DEVICES` set to the same value as `HQ_RESOURCE_VALUES_gpus`

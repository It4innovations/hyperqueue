## Generic resource management

Generic resource management serves for defining resources of workers and possibility to define requirements of these resources by tasks.
Some resources are auto-detected (GPUs in the current version); however, the user may define its own resources.

In the current version, there are two kind of resource pools:

* Indexed pool: A resource pool where each resource is identified by a non-negative integer and when a task asks for `N` such resources, then  `N` indices are reserved only for these task. An example: GPUs.
* Sum pool: A resource pool where worker only guarantees the following: The sum of resource requests of *running* tasks does not exceed the size of the pool.


## Defining resources

``hq worker start --resource <NAME1>=<DEF1> --resource <NAME2>=<DEF2> ...`` where `NAMEi` is a string name of the `i`-th resource and `DEFi` is a definition of the `i-th` resource that have to one of the following format:

* ``indices(<START>-<END>)`` - where ``START`` and ``END`` are non-negative integers. It define indexed pool in range START-END.
* ``sum(<SIZE>)`` - where ``SIZE`` is a size of the sum pool.


## Automatically detected resources

* Nvidia GPUs is automatically detected under resource name "gpus"


## Resource request

When a job is submitted, resource requests may be defined as follows:

``hq submit --resource <NAME1>=<AMOUNT1> <NAME1>=<AMOUNT1> ...``

Where ``NAME`` is a string name of the requested resource and the amount is a positive integer defining the size of the request.

When the task is executed the following variables are created:

* ``HQ_RESOURCE_REQUEST_<NAME>`` that contains the amount of request resources
* ``HQ_RESOURCE_INDICES_<NAME>`` if resource pool is an indexed pool then this variable contains comma-separated indices allocated for the task.

Special case: In case of resource ``gpus``, HQ also creates variables:

* ``CUDA_DEVICE_ORDER`` set to value ``PCI_BUS_ID``
* ``CUDA_VISIBLE_DEVICES`` set to the same value as ``HQ_RESOURCE_INDICES_gpus``
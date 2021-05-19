# Resource management

* ResourceDescription - Precise hardware info about worker 

* ResourcePool - Detailed resource description + current state of usage
                 (i.e. if an CPU id is free or not)
* ResourceAllocation - Particular resources allocated to a task (on CPU id level)


* WorkerResources - Server representation of ResourceDescription for faster comparison
                    and lookup (for scheduler purposes)
* WorkerLoad - Sum of resources of tasks allocated on a given worker (for scheduler purposes)

* ResourceRequest - Specification of resources needed by a task
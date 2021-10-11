
# Deployment

This section describes, how to HyperQueue server


## Starting server

Server may run on any computer as long as computing nodes are able to connect to these machine. It is not necessary to be able to connect from
server to computing nodes. In the most simple scenario, we expect that the user starts its own instance of HyperQueue directly on login of a HPC system.

The server can be simply started by the following command:

``hq server start``


Note: The server opens two TCP/IP ports: one for submitting jobs and one for connecting workers. By default, these ports are automatically assigned by the operation system. A user does not remmber them, they are stored in the "server directory". Other components automatically reads these settings.


## Server directory

When a HQ server is started, it creates a server directory where it stores informations needed for submiting jobs and connecting workers.

**Important:** Encryption keys are stored in the server directory. Who has access to server directory may submit jobs, connect workers to HyperQueue instance, and decrypt communication between HyperQueue components.

By default, server directory is stored in ``$HOME/.hq-server``. It may be changed via option ``--server-dir=<PATH>``. In such case,
all commands need to use the ``--server-dir`` settings.

You can run more instances of HyperQueue under the same user. All you need is to set a different server directories for each instance.


## Stopping server

A server can be stopped by command:

``hq server stop``


## Starting worker

A worker can be started by command. It reads server directory and connectes to the server.

``hq worker start``


### Starting worker in PBS

* Start worker on the first node of a PBS job

  ``qsub <qsub-settings> -- hq worker start``

* Start worker on all nodes of a PBS job

  ``$ qsub <your-params-of-qsub> -- `which pbsdsh` hq worker start``


### Starting worker in SLURM

* Start worker on the first node of a SLURM job

  ``sbatch <your-params-of-sbatch> --wrap "hq worker start"``

* Start worker on all nodes of a SLURM job``

  ``$ sbatch <your-params-of-sbatch> --wrap "srun hq worker start"``


### Worker's Time limit

 When a worker is started in PBS or SLURM, it automatically gets the time limit
 from the outer system and propagates it into HQ scheduler. If you want to set
 time limit for workers outside of PBS or SLURM (or you want to override the
 detected settings), then there is an option ``--time-limit=DURATION`` (e.g.
 ``hq worker start --time-limit=2h``). If time limit is reached, the worker is
 terminated.


## List of workers

``hq worker list``

State of workers:

* **Running** - Worker is running and is able to process tasks
* **Connection lost** - Worker closes connection. Probably someone manually killed the worker or wall time in PBS/SLURM job was reached
* **Heartbeat lost** - Communication between server and worker was interrputed. It usually means a network problem or an hardware crash of the computational node
* **Stopped** - Worker was stopped by ``hq worker stop ...``
* **Idle timeout** - Idle timeout is enabled on server and worker did not received any task for more then the limit.


## Stopping worker

Stop a specific worker:

``hq worker stop <id>``

Stop all workers:

``hq worker stop all``


## CPUs configuration

Worker automatically detect number of CPUs and on Linux system it also detects partitioning into sockets.
In most cases, it should work without need of any touch. If you want to see how is your seen by
a worker without actually starting it, you can start ``$ hq worker hwdetect`` that only prints CPUs layout.

### Manual specification of CPU configration

If automatic detection fails, or you want to manually configure set CPU configuration, you can use
``--cpus`` parameter; for example as follows:

- 8 CPUs for worker
  ``$ hq worker start --cpus=8``

- 2 sockets with 12 cores per socket
  ``$ hq worker --cpus=2x12``


## Idle timeout

Idle timeout allows to automatically stop a worker when no tasks are given to the worker for a specified time.

When a worker is started, an idle timeout may be configurated via ``--idle-timeout=<TIMEOUT>`` for ``hq worker start`` where ``TIMEOUT`` is a string like ``"2min 30s"``. (All possible formats are documented at https://docs.rs/humantime/2.1.0/humantime/fn.parse_duration.html).

Idle timeout can be also configured for all workers at once by ``hq server start --idle-timeout=<TIMEOUT>``. This value is then used for each worker that does not explicitly specifies its own timeout.


## Server address

By default, the server stores its own hostname as an address for connection of clients and workers. This can be changed by ``hq server start --host=HOST``, where HOST is a hostname/address under which is server visible.
<p align="center">
<img src="imgs/hq.png">
</p>


**HyperQueue** lets you build a computation plan consisting of a large amount of tasks and then
execute it transparently over a system like SLURM/PBS. It dynamically groups jobs into SLURM/PBS jobs and distributes
them to fully utilize allocated notes. You thus do not have to manually aggregate your tasks into SLURM/PBS jobs.


Project repository: [https://github.com/It4innovations/hyperqueue](https://github.com/It4innovations/hyperqueue)


## Submiting a simple task

* Start server (e.g. on a login node or in a cluster partition)

  ``$ hq server start``

* Submit a job (command ``echo 'Hello world'`` in this case)

  ``$ hq submit echo 'Hello world'``

* Ask for computing resource

    * Start worker manually

      ``$ hq worker start``

    * Automatic resource request

      [Not implemented yet]

    * Manual request in PBS

      ``$ qsub <your-params-of-qsub> -- hq worker start``

    * Manual request in SLURM

      ``sbatch <your-params-of-sbatch> -- hq worker start``

* Monitor the state of jobs

  ``$ hq jobs``




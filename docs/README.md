<p align="center">
<img src="imgs/hq.png">
</p>


**HyperQueue** lets you build a computation plan consisting of a large amount of tasks and then
execute it transparently over a system like SLURM/PBS. It dynamically groups jobs into SLURM/PBS jobs and distributes
them to fully utilize allocated notes. You thus do not have to manually aggregate your tasks into SLURM/PBS jobs.


Project repository: [https://github.com/It4innovations/hyperqueue](https://github.com/It4innovations/hyperqueue)


## Submiting a simple task

* Start server (e.g. on a login node or in a cluster partition)

  ``$ hq server start &``

* Submit a job (command ``echo 'Hello world'`` in this case)

  ``$ hq submit echo 'Hello world'``

* Ask for computing resources

    * Start worker manually

      ``$ hq worker start &``

    * Automatic resource request

      [Not implemented yet]

    * Manual request in PBS

      - Start worker on the first node of a PBS job

        ``$ qsub <your-params-of-qsub> -- hq worker start``

      - Start worker on all nodes of a PBS job

        ``$ qsub <your-params-of-qsub> -- `which pbsdsh` hq worker start``

    * Manual request in SLURM

      - Start worker on the first node of a Slurm job

        ``$ sbatch <your-params-of-sbatch> --wrap "hq worker start"``

      - Start worker on all nodes of a Slurm job

        ``$ sbatch <your-params-of-sbatch> --wrap "srun hq worker start"``

* Monitor the state of jobs

  ``$ hq jobs``




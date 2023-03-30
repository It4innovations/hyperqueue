import itertools
import logging
from typing import Dict, Optional, Sequence

from tqdm import tqdm

from .common import GenericPath
from .ffi.client import (
    ClientConnection,
    FailedTaskContext,
    JobId,
    TaskFailureMap,
    TaskId,
)
from .job import Job, SubmittedJob
from .task.function import PythonEnv
from .utils.string import pluralize

MAX_PRINTED_TASKS = 5

JobMap = Dict[JobId, Job]


class FailedJobsException(Exception):
    """
    This exception is triggered if a task fails.
    """

    def __init__(self, failed_tasks: TaskFailureMap, job_map: JobMap):
        self.failed_tasks = failed_tasks
        self.job_map = job_map

    def task_label(self, job_id: JobId, task_id: TaskId) -> str:
        return self.job_map[job_id].task_by_id(task_id).label

    def __str__(self):
        error = ""
        for job_id, tasks in self.failed_tasks.items():
            error += f"The following tasks of job `{job_id}` have failed:\n"
            for task_id, ctx in itertools.islice(tasks.items(), MAX_PRINTED_TASKS):
                task_label = self.task_label(job_id, task_id)
                error += f"Task {task_label} (id={task_id}):\n{ctx.error}\n"
                if ctx.cwd or ctx.stdout or ctx.stderr:
                    error += "You can find more information here:\n"
                    if ctx.cwd:
                        error += f"Working directory: {ctx.cwd}\n"
                    if ctx.stdout:
                        error += f"Stdout: {ctx.stdout}\n"
                    if ctx.stderr:
                        error += f"Stderr: {ctx.stderr}\n"
        return f"{error}\n"


class Client:
    def __init__(
        self,
        server_dir: Optional[GenericPath] = None,
        python_env: Optional[PythonEnv] = None,
    ):
        """
        A client serves as a gateway for submitting jobs and querying information about a running
        HyperQueue server.

        :param server_dir: Path to a server directory of a running HyperQueue server.
        :param python_env: Python environment which configures Python tasks created by
        [`function`](`hyperqueue.job.Job.function`).
        """
        server_dir = str(server_dir) if server_dir else None
        self.connection = ClientConnection(server_dir)
        if python_env is None:
            python_env = PythonEnv()
        self.python_env = python_env

    def submit(self, job: Job) -> SubmittedJob:
        """
        Submit a job into HyperQueue.

        :param job: Job that will be submitted.
        """
        job_desc = job._build(self)
        task_count = len(job_desc.tasks)
        if task_count < 1:
            raise Exception("Submitted job must have at least a single task")

        job_id = self.connection.submit_job(job_desc)
        logging.info(
            f"Submitted job {job_id} with {task_count} {pluralize('task', task_count)}"
        )
        return SubmittedJob(job=job, id=job_id)

    def wait_for_jobs(self, jobs: Sequence[SubmittedJob], raise_on_error=True) -> bool:
        """Returns True if all tasks were successfully finished"""

        job_ids = tuple(job.id for job in jobs)
        job_ids_str = ",".join(str(id) for id in job_ids)
        if len(jobs) > 1:
            job_ids_str = "{" + job_ids_str + "}"
        logging.info(
            f"Waiting for {pluralize('job', len(jobs))} {job_ids_str} to finish"
        )

        callback = create_progress_callback()

        failed_jobs = self.connection.wait_for_jobs(job_ids, callback)
        if failed_jobs and raise_on_error:
            failed_tasks = self.connection.get_failed_tasks(failed_jobs)
            job_map = {job.id: job.job for job in jobs}
            raise FailedJobsException(failed_tasks, job_map)
        return len(failed_jobs) == 0

    def get_failed_tasks(self, job: SubmittedJob) -> Dict[TaskId, FailedTaskContext]:
        result = self.connection.get_failed_tasks([job.id])
        return result[job.id]


def create_progress_callback():
    bar = None

    def cb(jobs):
        nonlocal bar

        if bar is None:
            total_tasks = sum(s["total"] for s in jobs.values())
            bar = tqdm(total=total_tasks, unit="task")

        job_count = len(jobs)
        completed = sum(1 if s["completed"] else 0 for s in jobs.values())

        job_text = pluralize("job", job_count)
        bar.set_postfix_str(f"{completed}/{job_count} {job_text} completed")

        finished_tasks = sum(s["finished"] for s in jobs.values())

        delta = finished_tasks - bar.n
        if delta:
            bar.update(delta)

    return cb

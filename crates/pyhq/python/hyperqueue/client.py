import itertools
import logging
from pathlib import Path
from typing import Dict, Optional, Sequence

from tqdm import tqdm

from .ffi.client import (
    ClientConnection,
    FailedTaskContext,
    JobId,
    TaskFailureMap,
    TaskId,
)
from .job import Job
from .task.function import PythonEnv
from .utils import pluralize

MAX_PRINTED_TASKS = 5


class FailedJobsException(Exception):
    def __init__(self, jobs: TaskFailureMap):
        self.jobs = jobs

    def __str__(self):
        error = ""
        for (job_id, tasks) in self.jobs.items():
            error += f"The following tasks of job `{job_id}` have failed:\n"
            for (task_id, ctx) in itertools.islice(tasks.items(), MAX_PRINTED_TASKS):
                error += f"Task {task_id}:\n{ctx.error}\n"
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
        self, path: Optional[Path] = None, python_env: Optional[PythonEnv] = None
    ):
        path = str(path) if path else None
        self.connection = ClientConnection(path)
        if python_env is None:
            python_env = PythonEnv()
        self.python_env = python_env

    def submit(self, job: Job) -> JobId:
        job_desc = job._build(self)
        task_count = len(job_desc.tasks)
        if task_count < 1:
            raise Exception("Submitted job must have at least a single task")

        job_id = self.connection.submit_job(job_desc)
        logging.info(
            f"Submitted job {job_id} with {task_count} {pluralize('task', task_count)}"
        )
        return job_id

    def wait_for_jobs(self, job_ids: Sequence[JobId], raise_on_error=True) -> bool:
        """Returns True if all tasks were successfully finished"""
        job_ids_str = ','.join(str(id) for id in job_ids)
        if len(job_ids) > 1:
            job_ids_str = "{" + job_ids_str + "}"
        logging.info(
            f"Waiting for {pluralize('job', len(job_ids))} {job_ids_str} to finish"
        )

        callback = create_progress_callback()

        failed_jobs = self.connection.wait_for_jobs(job_ids, callback)
        if failed_jobs and raise_on_error:
            failed_jobs = self.connection.get_failed_tasks(failed_jobs)
            raise FailedJobsException(failed_jobs)
        return len(failed_jobs) == 0

    def get_failed_tasks(self, job_id: JobId) -> Dict[TaskId, FailedTaskContext]:
        result = self.connection.get_failed_tasks([job_id])
        return result[job_id]


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

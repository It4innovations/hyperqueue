from pathlib import Path
from typing import Dict, Optional, Sequence

from .ffi.connection import Connection, JobId, TaskId
from .job import Job
from .task.function import PythonEnv


class TaskFailedException(Exception):
    pass


class Client:
    def __init__(
        self, path: Optional[Path] = None, python_env: Optional[PythonEnv] = None
    ):
        path = str(path) if path else None
        self.connection = Connection(path)
        if python_env is None:
            python_env = PythonEnv()
        self.python_env = python_env

    def submit(self, job: Job) -> JobId:
        return self.connection.submit_job(job._build(self))

    def wait_for_jobs(self, job_ids: Sequence[JobId], raise_on_error=True):
        """Returns True if all tasks were successfully finished"""
        finished = bool(self.connection.wait_for_jobs(job_ids))
        if not finished and raise_on_error:
            errors = self.connection.get_error_messages(job_ids)
            for es in errors.values():
                if not es:
                    continue
                min_id = min(es.keys())
                raise TaskFailedException(f"Task {min_id} failed:\n{es[min_id]}")
        else:
            return finished

    def get_error_messages(self, job_id: JobId) -> Dict[TaskId, str]:
        result = self.connection.get_error_messages([job_id])
        return result[job_id]

from pathlib import Path
from typing import Optional, Sequence

from .ffi.connection import Connection
from .job import Job, JobId
from .task.function import PythonEnv


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

    def wait_for_job(self, job_id):
        finished = self.connection.wait_for_jobs([job_id])
        if not finished:
            raise Exception(f"Job {job_id} failed.")

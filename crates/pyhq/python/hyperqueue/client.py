from pathlib import Path
from typing import Optional, Sequence, Dict

from .ffi.connection import Connection, TaskId, JobId
from .job import Job
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

    def wait_for_job(self, job_id: JobId):
        """ Returns True if all tasks were successfully finished """
        finished = self.connection.wait_for_jobs([job_id])
        return bool(finished)

    def get_error_messages(self, job_id: JobId) -> Dict[TaskId, str]:
        result = self.connection.get_error_messages([job_id])
        return result[job_id]


from pathlib import Path
from typing import Optional

from .ffi.connection import Connection
from .job import Job, JobId


class Client:
    def __init__(self, path: Optional[Path] = None, python_bin="python3"):
        path = str(path) if path else None
        self.connection = Connection(path)
        self.python_bin = python_bin

    def submit(self, job: Job) -> JobId:
        return self.connection.submit_job(job._build(self))

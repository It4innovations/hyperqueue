from pathlib import Path
from typing import Optional

from .ffi.connection import Connection
from .job import Job, JobId


class Client:

    def __init__(self, path: Optional[Path] = None):
        path = str(path) if path else None
        self.connection = Connection(path)

    def submit(self, job: Job) -> JobId:
        return self.connection.submit_job(job.build())

from pathlib import Path
from typing import Optional

from .ffi.ffi import connect_to_server, submit_job
from .job import Job, JobId


class Client:
    def __init__(self, path: Optional[Path] = None):
        path = str(path) if path else None
        self.ctx = connect_to_server(path)

    def submit(self, job: Job) -> JobId:
        return submit_job(self.ctx, job)

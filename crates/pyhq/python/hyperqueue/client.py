from .ffi.ffi import JobDescription, TaskDescription, connect_to_server, submit_job
from .job import Job


class Client:
    def __init__(self, *args, **kwargs):
        self.ctx = connect_to_server()

    def submit(self, job: Job):
        configs = job.build()
        job = JobDescription(
            tasks=[TaskDescription(args=config.args) for config in configs]
        )
        return submit_job(self.ctx, job)

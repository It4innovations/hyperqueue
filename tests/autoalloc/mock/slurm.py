import datetime
from subprocess import Popen
from typing import List

from ...conftest import HqEnv
from .command import (
    CommandHandler,
    CommandInput,
    CommandOutput,
    response,
)
from .manager import JobData, JobId, JobStatus, Manager


class SlurmCommandHandler(CommandHandler):
    def __init__(self, manager: Manager):
        self.manager = manager

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        self.manager.set_job_data(allocation_id, JobData.running())

        return hq_env.start_worker(
            env={"SLURM_JOB_ID": allocation_id},
            args=["--manager", "slurm", "--time-limit", "30m"],
        )

    async def handle_command(self, input: CommandInput) -> CommandOutput:
        cmd = input.command
        if cmd == "sbatch":
            return await self.handle_submit(input)
        elif cmd == "scontrol":
            return await self.handle_status(input)
        elif cmd == "scancel":
            return await self.handle_delete(input)
        else:
            raise Exception(f"Unknown PBS command {cmd}")

    async def handle_submit(self, input: CommandInput) -> CommandOutput:
        job_id = await self.manager.handle_submit(input)
        # Job ID has to be the fourth item (separated by spaces)
        msg = f"Submitted batch job {job_id}"
        return response(stdout=msg)

    async def handle_status(self, input: CommandInput) -> CommandOutput:
        job_ids = parse_slurm_status_job_ids(input)
        assert len(job_ids) == 1
        job_id = job_ids[0]

        job_data = await self.manager.handle_status(input, job_ids)

        content = ""
        job = job_data.get(job_id)
        if job is not None:
            if job.status == JobStatus.Queued:
                status = "PENDING"
            elif job.status == JobStatus.Running:
                status = "RUNNING"
            elif job.status == JobStatus.Finished:
                status = "COMPLETED"
            elif job.status == JobStatus.Failed:
                status = "FAILED"
            else:
                assert False

            time_limit = datetime.timedelta(hours=1)
            # TODO: calculate properly
            run_time = datetime.timedelta(minutes=1)
            content += f"JobState={status}"
            if job.stime:
                content += f"\nStartTime={to_slurm_time(job.stime)}"
            if job.mtime:
                content += f"\nEndTime={to_slurm_time(job.mtime)}"
            content += f"\nRunTime={to_slurm_duration(run_time)}"
            content += f"\nTimeLimit={to_slurm_duration(time_limit)}"
            content += "\n"
        return response(content)

    async def handle_delete(self, input: CommandInput) -> CommandOutput:
        await self.manager.handle_delete(input, input.arguments[0])
        return response()


def to_slurm_time(time: datetime.datetime) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def to_slurm_duration(duration: datetime.timedelta) -> str:
    seconds = int(duration.total_seconds())
    hours = seconds // 3600
    seconds = seconds % 3600
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def parse_slurm_status_job_ids(input: CommandInput) -> List[JobId]:
    assert input.arguments[:2] == ["show", "job"]
    job_id = input.arguments[2]
    return [job_id]

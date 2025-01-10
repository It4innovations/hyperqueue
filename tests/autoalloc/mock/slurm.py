import datetime
from subprocess import Popen
from typing import Optional, List

from aiohttp.web_request import Request

from ...conftest import HqEnv
from .handler import (
    CommandHandler,
    CommandResponse,
    MockInput,
    extract_mock_input,
    response,
)
from .manager import DefaultManager, JobData, JobStatus, Manager


class SlurmCommandAdapter(CommandHandler):
    def __init__(self, handler: Manager):
        self.handler = handler

    async def handle_command(self, request: Request, cmd: str) -> Optional[CommandResponse]:
        input = await extract_mock_input(request)

        if cmd == "sbatch":
            return await self.handler.handle_submit(input)
        elif cmd == "scontrol":
            return await self.handler.handle_status(input)
        elif cmd == "scancel":
            return await self.handler.handle_delete(input)
        else:
            raise Exception(f"Unknown Slurm command {cmd}")


def adapt_slurm(handler: Manager) -> CommandHandler:
    return SlurmCommandAdapter(handler)


def to_slurm_time(time: datetime.datetime) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def to_slurm_duration(duration: datetime.timedelta) -> str:
    seconds = int(duration.total_seconds())
    hours = seconds // 3600
    seconds = seconds % 3600
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"


class SlurmManager(DefaultManager):
    def parse_status_job_ids(self, input: MockInput) -> List[str]:
        assert input.arguments[:2] == ["show", "job"]
        job_id = input.arguments[2]
        return [job_id]

    async def handle_status(self, input: MockInput) -> CommandResponse:
        job_ids = self.parse_status_job_ids(input)
        assert len(job_ids) == 1
        job_id = job_ids[0]

        content = ""
        job_data = self.jobs[job_id]
        if job_data is not None:
            if job_data.status == JobStatus.Queued:
                status = "PENDING"
            elif job_data.status == JobStatus.Running:
                status = "RUNNING"
            elif job_data.status == JobStatus.Finished:
                status = "COMPLETED"
            elif job_data.status == JobStatus.Failed:
                status = "FAILED"
            else:
                assert False

            time_limit = datetime.timedelta(hours=1)
            # TODO: calculate properly
            run_time = datetime.timedelta(minutes=1)
            content += f"JobState={status}"
            if job_data.stime:
                content += f"\nStartTime={to_slurm_time(job_data.stime)}"
            if job_data.mtime:
                content += f"\nEndTime={to_slurm_time(job_data.mtime)}"
            content += f"\nRunTime={to_slurm_duration(run_time)}"
            content += f"\nTimeLimit={to_slurm_duration(time_limit)}"
            content += "\n"
        return response(content)

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        self.set_job_data(allocation_id, JobData.running())

        return hq_env.start_worker(
            env={"SLURM_JOB_ID": allocation_id},
            args=["--manager", "slurm", "--time-limit", "30m"],
        )

    def submit_response(self, job_id: str) -> str:
        # Job ID has to be the fourth item (separated by spaces)
        return f"Submitted batch job {job_id}"

import datetime
import json
from subprocess import Popen
from typing import List, Dict, Optional

from .command import (
    CommandOutput,
    CommandInput,
    response,
    CommandHandler,
)
from .manager import JobData, JobStatus
from .manager import Manager, JobId
from ...conftest import HqEnv


class PbsCommandHandler(CommandHandler):
    def __init__(self, manager: Manager):
        self.manager = manager

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        self.manager.set_job_data(allocation_id, JobData.running())

        return hq_env.start_worker(
            env={"PBS_JOBID": allocation_id, "PBS_ENVIRONMENT": "1"},
            args=["--manager", "pbs", "--time-limit", "30m"],
        )

    async def handle_command(self, input: CommandInput) -> CommandOutput:
        cmd = input.command
        if cmd == "qsub":
            return await self.handle_submit(input)
        elif cmd == "qstat":
            return await self.handle_status(input)
        elif cmd == "qdel":
            return await self.handle_delete(input)
        else:
            raise Exception(f"Unknown PBS command {cmd}")

    async def handle_submit(self, input: CommandInput) -> CommandOutput:
        job_id = await self.manager.handle_submit(input)
        return response(stdout=job_id)

    async def handle_status(self, input: CommandInput) -> CommandOutput:
        job_ids = parse_pbs_job_ids(input)
        job_data = await self.manager.handle_status(input, job_ids)

        return response(stdout=json.dumps({"Jobs": create_pbs_job_data(job_data)}))

    async def handle_delete(self, input: CommandInput) -> CommandOutput:
        await self.manager.handle_delete(input, parse_pbs_job_ids(input)[0])
        return response()


def to_pbs_time(time: datetime.datetime) -> str:
    return time.strftime("%a %b %d %H:%M:%S %Y")


def create_pbs_job_data(job_data: Dict[JobId, Optional[JobData]]):
    job_id_to_state = {}
    for job_id, job in job_data.items():
        if job is None:
            continue

        if job.status == JobStatus.Queued:
            status = "Q"
        elif job.status == JobStatus.Running:
            status = "R"
        elif job.status in (JobStatus.Failed, JobStatus.Finished):
            status = "F"
        else:
            assert False

        job_data = {"job_state": status}
        if job.qtime is not None:
            job_data["qtime"] = to_pbs_time(job.qtime)
        if job.stime is not None:
            job_data["stime"] = to_pbs_time(job.stime)
        if job.mtime is not None:
            job_data["mtime"] = to_pbs_time(job.mtime)
        if job.exit_code is not None:
            job_data["Exit_status"] = job.exit_code

        job_id_to_state[job_id] = job_data
    return job_id_to_state


def parse_pbs_job_ids(input: CommandInput) -> List[JobId]:
    job_ids = []
    args = input.arguments
    for index, arg in enumerate(args[:-1]):
        if arg == "-f":
            job_ids.append(args[index + 1])
    if not job_ids:
        raise Exception(f"Did not find -f in arguments: {args}")

    for job_id in job_ids:
        assert job_id[0].isdigit()
    return job_ids

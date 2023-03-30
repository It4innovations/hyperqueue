import datetime
import json
from subprocess import Popen
from typing import List, Optional

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


def to_pbs_time(time: datetime.datetime) -> str:
    return time.strftime("%a %b %d %H:%M:%S %Y")


class PbsCommandAdapter(CommandHandler):
    def __init__(self, handler: Manager):
        self.handler = handler

    async def handle_command(
        self, request: Request, cmd: str
    ) -> Optional[CommandResponse]:
        input = await extract_mock_input(request)

        if cmd == "qsub":
            return await self.handler.handle_submit(input)
        elif cmd == "qstat":
            return await self.handler.handle_status(input)
        elif cmd == "qdel":
            return await self.handler.handle_delete(input)
        else:
            raise Exception(f"Unknown PBS command {cmd}")


def adapt_pbs(handler: Manager) -> CommandHandler:
    return PbsCommandAdapter(handler)


class PbsManager(DefaultManager):
    async def handle_status(self, input: MockInput) -> CommandResponse:
        job_ids = []
        args = input.arguments
        for index, arg in enumerate(args[:-1]):
            if arg == "-f":
                job_ids.append(args[index + 1])

        if not job_ids:
            raise Exception(f"Did not find -f in arguments: {args}")
        for job_id in job_ids:
            assert job_id[0].isdigit()

        return response(stdout=json.dumps({"Jobs": self.create_job_data(job_ids)}))

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        self.set_job_data(allocation_id, JobData.running())

        return hq_env.start_worker(
            env={"PBS_JOBID": allocation_id, "PBS_ENVIRONMENT": "1"},
            args=["--manager", "pbs", "--time-limit", "30m"],
        )

    def create_job_data(self, job_ids: List[str]):
        job_id_to_state = {}
        for job_id in job_ids:
            job = self.jobs[job_id]
            if job is not None:
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

    def submit_response(self, job_id: str) -> str:
        return job_id

import datetime
import json
from subprocess import Popen
from typing import List, Dict, Optional

from .manager import JobData, JobStatus, ManagerAdapter, CommandInput, CommandType, CommandOutput, response, JobId
from ...conftest import HqEnv


class PbsAdapter(ManagerAdapter):
    def parse_command_type(self, input: CommandInput) -> CommandType:
        cmd = input.command
        if cmd == "qsub":
            return "submit"
        elif cmd == "qstat":
            return "status"
        elif cmd == "qdel":
            return "delete"
        else:
            raise Exception(f"Unknown PBS command {cmd}")

    def format_submit_output(self, job_id: JobId) -> CommandOutput:
        return response(stdout=job_id)

    def parse_status_job_ids(self, input: CommandInput) -> List[JobId]:
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

    def format_status_output(self, job_data: Dict[JobId, JobData]) -> CommandOutput:
        return response(stdout=json.dumps({"Jobs": create_pbs_job_data(job_data)}))

    def start_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        return hq_env.start_worker(
            env={"PBS_JOBID": allocation_id, "PBS_ENVIRONMENT": "1"},
            args=["--manager", "pbs", "--time-limit", "30m"],
        )


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

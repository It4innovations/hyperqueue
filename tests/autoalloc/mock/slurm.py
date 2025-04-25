import datetime
from subprocess import Popen
from typing import List, Dict

from ...conftest import HqEnv
from .manager import JobData, JobId, JobStatus, ManagerAdapter, CommandType, CommandInput, CommandOutput, response


class SlurmAdapter(ManagerAdapter):
    def parse_command_type(self, input: CommandInput) -> CommandType:
        cmd = input.command
        if cmd == "sbatch":
            return "submit"
        elif cmd == "scontrol":
            return "status"
        elif cmd == "scancel":
            return "delete"
        else:
            raise Exception(f"Unknown Slurm command {cmd}")

    def format_submit_output(self, job_id: JobId) -> CommandOutput:
        # Job ID has to be the fourth item (separated by spaces)
        msg = f"Submitted batch job {job_id}"
        return response(stdout=msg)

    def parse_status_job_ids(self, input: CommandInput) -> List[JobId]:
        job_ids = parse_slurm_status_job_ids(input)
        assert len(job_ids) == 1
        return job_ids

    def format_status_output(self, job_data: Dict[JobId, JobData]) -> CommandOutput:
        assert len(job_data) == 1

        content = ""
        job = next(iter(job_data.values()))
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

    def start_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        return hq_env.start_worker(
            env={"SLURM_JOB_ID": allocation_id},
            args=["--manager", "slurm", "--time-limit", "30m"],
        )


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

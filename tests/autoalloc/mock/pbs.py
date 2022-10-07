import dataclasses
import datetime
import json
from subprocess import Popen
from typing import List, Optional

from aiohttp.web_request import Request

from ...conftest import HqEnv
from .handler import (
    CommandHandler,
    CommandResponse,
    DefaultManager,
    JobType,
    Manager,
    MockInput,
    extract_mock_input,
    response,
)


@dataclasses.dataclass(frozen=True)
class JobState:
    @staticmethod
    def running() -> "JobState":
        return JobState(
            status="R",
            stime=to_pbs_time(now()),
            qtime=to_pbs_time(now() - datetime.timedelta(seconds=1)),
            mtime=to_pbs_time(now()),
        )

    @staticmethod
    def queued() -> "JobState":
        return JobState(
            status="Q",
            qtime=to_pbs_time(now() - datetime.timedelta(seconds=1)),
        )

    @staticmethod
    def finished() -> "JobState":
        return JobState(
            status="F",
            stime=to_pbs_time(now()),
            qtime=to_pbs_time(now() - datetime.timedelta(seconds=1)),
            mtime=to_pbs_time(now() + datetime.timedelta(seconds=1)),
            exit_code=0,
        )

    @staticmethod
    def failed() -> "JobState":
        return JobState(
            status="F",
            stime=to_pbs_time(now()),
            qtime=to_pbs_time(now() - datetime.timedelta(seconds=1)),
            mtime=to_pbs_time(now() + datetime.timedelta(seconds=1)),
            exit_code=1,
        )

    status: str
    qtime: Optional[str] = None
    stime: Optional[str] = None
    mtime: Optional[str] = None
    exit_code: Optional[int] = None


def now() -> datetime.datetime:
    return datetime.datetime.now()


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


class PbsManager(DefaultManager[JobState]):
    async def handle_status(self, input: MockInput) -> CommandResponse:
        job_ids = []
        args = input.arguments
        for (index, arg) in enumerate(args[:-1]):
            if arg == "-f":
                job_ids.append(args[index + 1])

        if not job_ids:
            raise Exception(f"Did not find -f in arguments: {args}")

        return response(stdout=json.dumps({"Jobs": self.create_job_data(job_ids)}))

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        self.set_job_status(allocation_id, JobState.running())

        return hq_env.start_worker(
            env={"PBS_JOBID": allocation_id, "PBS_ENVIRONMENT": "1"},
            args=["--manager", "pbs", "--time-limit", "30m"],
        )

    def queue_job_state(self) -> JobType:
        return JobState.queued()

    def create_job_data(self, job_ids: List[str]):
        job_id_to_state = {}
        for job_id in job_ids:
            state = self.jobs[job_id]
            if state is not None:
                job_data = {"job_state": state.status}
                if state.qtime is not None:
                    job_data["qtime"] = state.qtime
                if state.stime is not None:
                    job_data["stime"] = state.stime
                if state.mtime is not None:
                    job_data["mtime"] = state.mtime
                if state.exit_code is not None:
                    job_data["Exit_status"] = state.exit_code

                job_id_to_state[job_id] = job_data
        return job_id_to_state

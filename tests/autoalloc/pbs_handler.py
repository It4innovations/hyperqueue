import dataclasses
import datetime
import json
from subprocess import Popen
from typing import Dict, List, Optional

from aiohttp.web_request import Request

from ..conftest import HqEnv
from .mock.handler import (
    CommandHandler,
    CommandResponse,
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


class PbsCommandHandler(CommandHandler):
    def __init__(self):
        self.job_counter = 0
        # None = job is purposely missing
        self.jobs: Dict[str, Optional[JobState]] = {}
        self.deleted_jobs = set()

    async def handle_command(
        self, request: Request, cmd: str
    ) -> Optional[CommandResponse]:
        input = await extract_mock_input(request)

        if cmd == "qsub":
            await self.inspect_qsub(input)
            return await self.handle_qsub(input)
        elif cmd == "qstat":
            await self.inspect_qstat(input)
            return await self.handle_qstat(input)
        elif cmd == "qdel":
            await self.inspect_qdel(input)
            return await self.handle_qdel(input)
        else:
            raise Exception(f"Unknown PBS command {cmd}")

    async def handle_qsub(self, _input: MockInput) -> CommandResponse:
        # By default, create a new job
        job_id = self.job_id(self.job_counter)
        self.job_counter += 1

        # The state of this job could already have been set before manually
        if job_id not in self.jobs:
            self.jobs[job_id] = JobState.queued()
        return response(stdout=job_id)

    async def inspect_qsub(self, input: MockInput):
        pass

    async def handle_qstat(self, input: MockInput) -> CommandResponse:
        job_ids = []
        args = input.arguments
        for (index, arg) in enumerate(args[:-1]):
            if arg == "-f":
                job_ids.append(args[index + 1])

        if not job_ids:
            raise Exception(f"Did not find -f in arguments: {args}")

        return response(stdout=json.dumps({"Jobs": self.create_job_data(job_ids)}))

    async def inspect_qstat(self, input: MockInput):
        pass

    async def handle_qdel(self, input: MockInput) -> Optional[CommandResponse]:
        job_id = input.arguments[0]
        assert job_id in self.jobs
        self.deleted_jobs.add(job_id)
        return None

    async def inspect_qdel(self, input: MockInput):
        pass

    def job_id(self, index: int) -> str:
        return f"{index + 1}.job"

    def add_worker(self, hq_env: HqEnv, allocation_id: str) -> Popen:
        self.set_job_status(allocation_id, JobState.running())

        return hq_env.start_worker(
            env={"PBS_JOBID": allocation_id, "PBS_ENVIRONMENT": "1"},
            args=["--manager", "pbs", "--time-limit", "30m"],
        )

    def set_job_status(self, job_id: str, status: Optional[JobState]):
        self.jobs[job_id] = status

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


# class PbsMock:
#     def __init__(
#         self,
#         hq_env: HqEnv,
#         new_job_responses: List[NewJobResponse] = None,
#         qdel_code: Optional[str] = None,
#     ):
#         if new_job_responses is None:
#             new_job_responses = list(NewJobId(id=str(i)) for i in range(1000))
#         self.new_job_responses = new_job_responses
#         self.hq_env = hq_env
#         self.qstat_path = join(self.hq_env.work_path, "pbs-qstat")
#         self.qsub_path = join(self.hq_env.work_path, "pbs-qsub")
#         self.qdel_dir = join(self.hq_env.work_path, "pbs-qdel")
#         os.makedirs(self.qdel_dir)
#
#         with open(self.qsub_path, "w") as f:
#             responses = [
#                 (True, r.id) if isinstance(r, NewJobId) else (False, r.message)
#                 for r in self.new_job_responses
#             ]
#             f.write(json.dumps(responses))
#
#         self.qsub_code = f"""
# import json
# import sys
#
# with open("{self.qsub_path}") as f:
#     jobs = json.loads(f.read())
#
# if not jobs:
#     raise Exception("No more jobs can be scheduled")
#
# is_success, data = jobs.pop(0)
# with open("{self.qsub_path}", "w") as f:
#     f.write(json.dumps(jobs))
#
# if is_success:
#     print(data)
# else:
#     print(data, file=sys.stderr)
#     exit(1)
# """
#         self.qstat_code = f"""
# import sys
# import json
#
# job_ids = []
# args = sys.argv[1:]
# for (index, arg) in enumerate(args[:-1]):
#     if arg == "-f":
#         job_ids.append(args[index + 1])
#
# if not job_ids:
#     raise Exception(f"Did not find -f in arguments: {{args}}")
#
# with open("{self.qstat_path}") as f:
#     jobdata = json.loads(f.read())
#
# data = dict(
#     Jobs=jobdata
# )
# print(json.dumps(data))
# """
#         self.qdel_code = (
#             qdel_code
#             or f"""
# import sys
# import json
# import os
#
# jobid = sys.argv[1]
#
# with open(os.path.join("{self.qdel_dir}", jobid), "w") as f:
#     f.write(jobid)
# """
#         )
#
#         self.jobs: Dict[str, JobState] = {}
#         self.write_job_data()
#
#     def job_id(self, index: int) -> str:
#         return self.new_job_responses[index].id
#
#     @contextlib.contextmanager
#     def activate(self):
#         with self.hq_env.mock.mock_program("qsub", self.qsub_code):
#             with self.hq_env.mock.mock_program("qstat", self.qstat_code):
#                 with self.hq_env.mock.mock_program("qdel", self.qdel_code):
#                     yield
#
#     def update_job_state(self, job_id: str, state: Optional[JobState]):
#         if job_id in self.jobs:
#             if state is None:
#                 del self.jobs[job_id]
#             else:
#                 changes = {
#                     k: v
#                     for (k, v) in dataclasses.asdict(state).items()
#                     if v is not None
#                 }
#                 self.jobs[job_id] = dataclasses.replace(self.jobs[job_id], **changes)
#         elif state is not None:
#             self.jobs[job_id] = state
#         self.write_job_data()
#
#     def write_job_data(self):
#         job_id_to_state = {}
#         for (job_id, state) in self.jobs.items():
#             job_data = {"job_state": state.status}
#             if state.qtime is not None:
#                 job_data["qtime"] = state.qtime
#             if state.stime is not None:
#                 job_data["stime"] = state.stime
#             if state.mtime is not None:
#                 job_data["mtime"] = state.mtime
#             if state.exit_code is not None:
#                 job_data["Exit_status"] = state.exit_code
#
#             job_id_to_state[job_id] = job_data
#         with open(self.qstat_path, "w") as f:
#             f.write(json.dumps(job_id_to_state))
#
#     def deleted_jobs(self) -> List[str]:
#         return list(os.listdir(self.qdel_dir))

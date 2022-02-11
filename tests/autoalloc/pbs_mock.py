import contextlib
import dataclasses
import json
import os
from os.path import join
from typing import Dict, List, Optional

from ..conftest import HqEnv


@dataclasses.dataclass(frozen=True)
class JobState:
    status: str
    qtime: Optional[str] = None
    stime: Optional[str] = None
    mtime: Optional[str] = None
    exit_code: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class NewJobResponse:
    pass


@dataclasses.dataclass(frozen=True)
class NewJobId(NewJobResponse):
    id: str


@dataclasses.dataclass(frozen=True)
class NewJobFailed(NewJobResponse):
    message: str


class PbsMock:
    def __init__(
        self,
        hq_env: HqEnv,
        new_job_responses: List[NewJobResponse] = None,
        qdel_code: Optional[str] = None,
    ):
        if new_job_responses is None:
            new_job_responses = list(NewJobId(id=str(i)) for i in range(1000))
        self.new_job_responses = new_job_responses
        self.hq_env = hq_env
        self.qstat_path = join(self.hq_env.work_path, "pbs-qstat")
        self.qsub_path = join(self.hq_env.work_path, "pbs-qsub")
        self.qdel_dir = join(self.hq_env.work_path, "pbs-qdel")
        os.makedirs(self.qdel_dir)

        with open(self.qsub_path, "w") as f:
            responses = [
                (True, r.id) if isinstance(r, NewJobId) else (False, r.message)
                for r in self.new_job_responses
            ]
            f.write(json.dumps(responses))

        self.qsub_code = f"""
import json
import sys

with open("{self.qsub_path}") as f:
    jobs = json.loads(f.read())

if not jobs:
    raise Exception("No more jobs can be scheduled")

is_success, data = jobs.pop(0)
with open("{self.qsub_path}", "w") as f:
    f.write(json.dumps(jobs))

if is_success:
    print(data)
else:
    print(data, file=sys.stderr)
    exit(1)
"""
        self.qstat_code = f"""
import sys
import json

job_ids = []
args = sys.argv[1:]
for (index, arg) in enumerate(args[:-1]):
    if arg == "-f":
        job_ids.append(args[index + 1])

if not job_ids:
    raise Exception(f"Did not find -f in arguments: {{args}}")

with open("{self.qstat_path}") as f:
    jobdata = json.loads(f.read())

data = dict(
    Jobs=jobdata
)
print(json.dumps(data))
"""
        self.qdel_code = (
            qdel_code
            or f"""
import sys
import json
import os

jobid = sys.argv[1]

with open(os.path.join("{self.qdel_dir}", jobid), "w") as f:
    f.write(jobid)
"""
        )

        self.jobs: Dict[str, JobState] = {}

    def job_id(self, index: int) -> str:
        return self.new_job_responses[index].id

    @contextlib.contextmanager
    def activate(self):
        with self.hq_env.mock.mock_program("qsub", self.qsub_code):
            with self.hq_env.mock.mock_program("qstat", self.qstat_code):
                with self.hq_env.mock.mock_program("qdel", self.qdel_code):
                    yield

    def update_job_state(self, job_id: str, state: JobState):
        if job_id in self.jobs:
            changes = {
                k: v for (k, v) in dataclasses.asdict(state).items() if v is not None
            }
            self.jobs[job_id] = dataclasses.replace(self.jobs[job_id], **changes)
        else:
            self.jobs[job_id] = state
        self.write_job_data()

    def write_job_data(self):
        job_id_to_state = {}
        for (job_id, state) in self.jobs.items():
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
        with open(self.qstat_path, "w") as f:
            f.write(json.dumps(job_id_to_state))

    def deleted_jobs(self) -> List[str]:
        return list(os.listdir(self.qdel_dir))

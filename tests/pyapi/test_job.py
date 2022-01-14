from pathlib import Path
from typing import Tuple

from hyperqueue.client import Client
from hyperqueue.job import Job

from ..conftest import HqEnv
from ..utils import wait_for_job_state


def test_submit_simple(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(args=["hostname"])
    job_id = client.submit(job)

    wait_for_job_state(hq_env, job_id, "FINISHED")


def test_submit_cwd(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    cwd = Path(hq_env.server_dir) / "workdir"
    cwd.mkdir()

    job.program(args=["hostname"], cwd=str(cwd))
    job_id = client.submit(job)

    table = hq_env.command(["job", "info", str(job_id)], as_table=True)
    table.check_row_value("Working directory", str(cwd))


def test_submit_env(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    job.program(args=["hostname"], env={"FOO": "BAR", "BAZ": "123"})
    job_id = client.submit(job)

    table = hq_env.command(["job", "info", str(job_id)], as_table=True)
    table.check_row_value("Environment", "BAZ=123\nFOO=BAR")


def prepare_job_client(hq_env: HqEnv) -> Tuple[Job, Client]:
    hq_env.start_server()
    hq_env.start_worker()
    client = Client(hq_env.server_dir)
    return (Job(), client)

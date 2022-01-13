from hyperqueue.client import Client
from hyperqueue.job import Job

from ..conftest import HqEnv
from ..utils import wait_for_job_state


def test_submit_simple(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    client = Client(hq_env.server_dir)

    job = Job()
    job.program(args=["hostname"])
    job_id = client.submit(job)

    wait_for_job_state(hq_env, job_id, "FINISHED")

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from . import bash, prepare_job_client


def test_single_dep(hq_env: HqEnv):
    """
    Check that the second task does not start computing before the first one.
    """
    (job, client) = prepare_job_client(hq_env, with_worker=False)
    hq_env.start_worker(cpus=2)

    t1 = job.program(args=bash("sleep 1; echo 'hello' > foo.txt"))
    job.program(args=bash("cat foo.txt"), depends=[t1])
    job_id = client.submit(job)

    wait_for_job_state(hq_env, job_id, "FINISHED")

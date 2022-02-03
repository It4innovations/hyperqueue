from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.io import check_file_contents
from . import prepare_job_client


def test_submit_pyfunction(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    def body(a, b, c):
        print(f"stdout {a} {b} {c}")

    job.function(body, args=(1, 2), kwargs={"c": 3}, stdout="out", stderr="err")
    job_id = client.submit(job)
    wait_for_job_state(hq_env, job_id, "FINISHED")

    check_file_contents("out", "stdout 1 2 3\n")

import os.path
from pathlib import Path

from hyperqueue.client import Client, PythonEnv
from hyperqueue.job import Job

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


def test_submit_python_prologue(hq_env: HqEnv):
    init_sh = Path("init.sh")
    init_sh.write_text("""export ABC=xyz""")

    hq_env.start_server()
    hq_env.start_worker()
    client = Client(
        hq_env.server_dir, python_env=PythonEnv(prologue=f"source {init_sh.resolve()}")
    )

    def body():
        print(os.environ.get("ABC"))

    job = Job()
    job.function(body, stdout="out")
    job_id = client.submit(job)
    wait_for_job_state(hq_env, job_id, "FINISHED")
    check_file_contents("out", "xyz\n")

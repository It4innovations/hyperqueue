import os.path
import time
from pathlib import Path

from hyperqueue.client import Client, PythonEnv
from hyperqueue.ffi.protocol import ResourceRequest
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


def test_submit_pyfunction_fail(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    def body():
        raise Exception("MyException")

    job.function(body, stderr="err")
    job_id = client.submit(job)
    client.wait_for_jobs([job_id], raise_on_error=False)
    errors = client.get_failed_tasks(job_id)
    assert list(errors.keys()) == [0]
    assert errors[0].error.endswith(
        '    raise Exception("MyException")\nException: MyException\n'
    )
    assert errors[0].stderr == os.path.abspath("err")


def test_function_resources(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.function(lambda: 1, resources=ResourceRequest(cpus="1"))
    job.function(lambda: 1, resources=ResourceRequest(cpus="2"))
    job.function(lambda: 1, resources=ResourceRequest(cpus="all"))
    job_id = client.submit(job)
    time.sleep(2.0)

    table = hq_env.command(["job", "tasks", str(job_id)], as_table=True)
    assert table.get_column_value("State") == ["FINISHED", "WAITING", "FINISHED"]


def test_default_workdir(hq_env: HqEnv):
    workdir = Path("foo").resolve()
    (job, client) = prepare_job_client(hq_env, default_workdir=workdir)

    def fn():
        assert os.getcwd() == str(workdir)

    job.function(fn)
    job_id = client.submit(job)
    client.wait_for_jobs([job_id])

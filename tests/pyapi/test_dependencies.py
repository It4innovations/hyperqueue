from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.cmd import bash
from . import prepare_job_client
from hyperqueue import Client, Job
import time


def test_single_dep(hq_env: HqEnv):
    """
    Check that the second task does not start computing before the first one.
    """
    (job, client) = prepare_job_client(hq_env, with_worker=False)
    hq_env.start_worker(cpus=2)

    t1 = job.program(args=bash("sleep 1; echo 'hello' > foo.txt"))
    job.program(args=bash("cat foo.txt"), deps=[t1])
    submitted_job = client.submit(job)

    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")


def test_dep_failed(hq_env: HqEnv):
    """
    Check that consumers of a failed tasks are canceled
    """

    (job, client) = prepare_job_client(hq_env, with_worker=True)

    t1 = job.program(args=bash("exit 1"))
    t2 = job.program(args=bash("echo 'hello' > foo1.txt"), deps=[t1])
    job.program(args=bash("echo 'hello' > foo2.txt"), deps=[t2])
    job.program(args=bash("exit 0"))
    submitted_job = client.submit(job)

    wait_for_job_state(hq_env, submitted_job.id, "FAILED")

    table = hq_env.command(["task", "list", "1"], as_table=True)
    assert table.get_row_value("0") == "FAILED"
    assert table.get_row_value("1") == "CANCELED"
    assert table.get_row_value("2") == "CANCELED"
    assert table.get_row_value("3") == "FINISHED"


def test_kill_worker_with_deps(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="4")
    hq_env.start_worker(cpus="4")
    client = Client(hq_env.server_dir)

    job = Job()
    jobs = [job.program(bash("sleep 1")) for _ in range(16)]
    job.program(bash("sleep 1"), deps=jobs)
    submitted_job = client.submit(job)
    wait_for_job_state(hq_env, submitted_job.id, "RUNNING")
    hq_env.kill_worker(1)
    hq_env.kill_worker(2)
    time.sleep(2.0)
    wait_for_job_state(hq_env, submitted_job.id, "WAITING")


def test_long_chain_of_deps(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="4")
    client = Client(hq_env.server_dir)
    job = Job()
    prev = job.program(bash("sleep 0"))
    for i in range(50):
        prev = job.program(bash("sleep 0"), deps=[prev])
    submitted_job = client.submit(job)
    hq_env.command(["job", "info", str(submitted_job.id)])
    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")


def test_long_many_pairs_deps(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="4")
    client = Client(hq_env.server_dir)
    job = Job()
    for i in range(150):
        prev = job.program(bash("sleep 0"))
        job.program(bash("sleep 0"), deps=[prev])
    submitted_job = client.submit(job)
    hq_env.command(["job", "info", str(submitted_job.id)])
    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")

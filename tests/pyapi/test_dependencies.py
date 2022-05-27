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

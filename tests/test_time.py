import time

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.job import list_jobs


def test_job_time_request1(hq_env: HqEnv):
    # Tests that tasks are sent only to worker3 and worker 4 (because of time requests)
    hq_env.start_server()
    hq_env.start_worker(args=["--time-limit", "2s"])
    hq_env.start_worker(args=["--time-limit", "4s"])
    hq_env.start_worker(args=["--time-limit", "10s"])
    hq_env.start_worker()

    hq_env.command(["submit", "--array=1-20", "--time-request=5s", "--", "ls"])
    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert {"worker3", "worker4"} == set(table.get_row_value("Workers").split(", "))


def test_job_time_request2(hq_env: HqEnv):
    # Test that a tasks with time request is not sent to worker without remaining lifetime
    hq_env.start_server()
    hq_env.start_worker(args=["--time-limit", "4s"])
    hq_env.command(["submit", "--time-request=2s", "--", "ls"])
    time.sleep(2.2)
    hq_env.command(["submit", "--time-request=2s", "--", "ls"])
    time.sleep(1.0)
    # hq_env.start_worker(args=["--time-limit", "5s"])

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = list_jobs(hq_env)
    assert table.get_column_value("State")[0] == "FINISHED"
    assert table.get_column_value("State")[1] == "WAITING"

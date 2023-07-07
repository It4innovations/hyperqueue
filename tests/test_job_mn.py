import time

import pytest

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.job import default_task_output


def test_submit_mn(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(2)

    hq_env.command(["submit", "--nodes=3", "--", "bash", "-c", "sleep 1; cat ${HQ_NODE_FILE}"])
    time.sleep(0.5)
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Resources", "nodes: 3")
    table.check_row_value("State", "WAITING")

    hq_env.start_workers(2)

    wait_for_job_state(hq_env, 1, "RUNNING")

    table = hq_env.command(["task", "list", "1"], as_table=True)
    ws = table.get_column_value("Worker")[0].split("\n")
    assert len(ws) == 3
    assert set(ws).issubset(["worker1", "worker2", "worker3", "worker4"])

    wait_for_job_state(hq_env, 1, "FINISHED", timeout_s=1.2)
    with open(default_task_output(1)) as f:
        hosts = f.read().rstrip().split("\n")
        assert hosts == ws
    # assert len(nodes) == 3


def test_reservation_in_mn(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--nodes=3", "--", "bash", "-c", "sleep 3"])
    workers = hq_env.start_workers(3, args=["--heartbeat=500ms", "--idle-timeout=1s"])
    wait_for_job_state(hq_env, 1, "FINISHED")
    hq_env.check_running_processes()
    time.sleep(2)
    for w in workers:
        hq_env.check_process_exited(w, expected_code=None)


def test_failed_mn_task(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(3)
    hq_env.command(["submit", "--nodes=3", "--", "bash", "-c", "exit 1"])
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["task", "list", "1"], as_table=True)
    ws = table.get_column_value("Worker")[0].split("\n")
    assert len(ws) == 3
    assert set(ws).issubset(["worker1", "worker2", "worker3"])


def test_cancel_mn_task_running(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(3)
    hq_env.command(["submit", "--nodes=3", "--", "bash", "-c", "sleep 10"])
    wait_for_job_state(hq_env, 1, "RUNNING")
    hq_env.command(["job", "cancel", "1"])
    wait_for_job_state(hq_env, 1, "CANCELED")
    hq_env.command(["submit", "--nodes=3", "--", "bash", "-c", "exit 0"])
    wait_for_job_state(hq_env, 2, "FINISHED")


def test_cancel_mn_task_waiting(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "--nodes=2", "--", "bash", "-c", "sleep 10"])
    wait_for_job_state(hq_env, 1, "WAITING")
    hq_env.command(["job", "cancel", "all"])
    wait_for_job_state(hq_env, 1, "CANCELED")


@pytest.mark.parametrize("root", (True, False))
def test_worker_lost_mn_task(hq_env: HqEnv, root: bool):
    hq_env.start_server()
    hq_env.start_workers(3, cpus=1)
    hq_env.command(["submit", "--nodes=3", "--", "bash", "-c", "sleep 2"])
    wait_for_job_state(hq_env, 1, "RUNNING")

    table = hq_env.command(["task", "list", "1"], as_table=True)
    ws = table.get_column_value("Worker")[0].split("\n")
    worker_ids = [int(w[len("worker") :]) for w in ws]
    hq_env.kill_worker(worker_ids[0 if root else 1])
    wait_for_job_state(hq_env, 1, "WAITING")
    hq_env.start_workers(1, cpus=1)
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_submit_mn_different_groups(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(2, args=["--group=g1"])
    hq_env.start_workers(2, args=["--group=g2"])

    hq_env.command(["submit", "--nodes=3", "--", "/bin/hostname"])
    time.sleep(0.5)
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("State", "WAITING")
    hq_env.start_workers(1, args=["--group=g2"])
    wait_for_job_state(hq_env, 1, "FINISHED")

import time
from typing import Optional

from .conftest import HqEnv
from .utils import wait_for_worker_state, wait_until


def test_worker_list(hq_env: HqEnv):
    hq_env.start_server()
    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 1
    assert table[0][:2] == ["Id", "State"]

    hq_env.start_workers(2)

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[0][:2] == ["Id", "State"]
    assert table[1][:2] == ["1", "RUNNING"]
    assert table[2][:2] == ["2", "RUNNING"]

    hq_env.kill_worker(2)
    wait_for_worker_state(hq_env, 2, "CONNECTION LOST")

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[0][:2] == ["Id", "State"]
    assert table[1][:2] == ["1", "RUNNING"]
    assert table[2][:2] == ["2", "CONNECTION LOST"]

    hq_env.kill_worker(1)
    wait_for_worker_state(hq_env, 1, "CONNECTION LOST")

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[0][:2] == ["Id", "State"]
    assert table[1][:2] == ["1", "CONNECTION LOST"]
    assert table[2][:2] == ["2", "CONNECTION LOST"]

    hq_env.start_worker()
    wait_for_worker_state(hq_env, 3, "RUNNING")

    table = hq_env.command(["worker", "list"], as_table=True)

    assert len(table) == 4
    assert table[0][:2] == ["Id", "State"]
    assert table[1][:2] == ["1", "CONNECTION LOST"]
    assert table[2][:2] == ["2", "CONNECTION LOST"]
    assert table[3][:2] == ["3", "RUNNING"]


def test_worker_stop(hq_env: HqEnv):
    hq_env.start_server()
    process = hq_env.start_worker()

    wait_for_worker_state(hq_env, 1, "RUNNING")
    hq_env.command(["worker", "stop", "1"])
    wait_for_worker_state(hq_env, 1, "STOPPED")
    hq_env.check_process_exited(process)


def test_worker_list_online_offline_state(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(2)

    wait_for_worker_state(hq_env, [1, 2], "RUNNING")

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[1][:2] == ["1", "RUNNING"]
    assert table[2][:2] == ["2", "RUNNING"]
    hq_env.kill_worker(2)

    wait_for_worker_state(hq_env, 2, "CONNECTION LOST")
    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[1][:2] == ["1", "RUNNING"]
    assert table[2][:2] == ["2", "CONNECTION LOST"]

    table = hq_env.command(["worker", "list", "--offline"], as_table=True)
    assert len(table) == 2
    assert table[1][:2] == ["2", "CONNECTION LOST"]

    table = hq_env.command(["worker", "list", "--running"], as_table=True)
    assert len(table) == 2
    assert table[1][:2] == ["1", "RUNNING"]


def test_worker_list_resources(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="10")
    hq_env.start_worker(cpus="4x5")

    wait_for_worker_state(hq_env, [1, 2], "RUNNING")

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[1][:2] == ["1", "RUNNING"]
    assert table[2][:2] == ["2", "RUNNING"]
    assert table[0][3] == "Resources"
    assert table[1][3] == "1x10 cpus"
    assert table[2][3] == "4x5 cpus"


def test_idle_timeout(hq_env: HqEnv):
    hq_env.start_server(args=["--idle-timeout", "1s"])
    w = hq_env.start_worker(args=["--heartbeat", "500ms"])
    time.sleep(0.5)
    hq_env.command(["submit", "--", "sleep", "1"])
    time.sleep(1.0)
    table = hq_env.command(["worker", "list"], as_table=True)
    assert table[1][1] == "RUNNING"

    time.sleep(1.5)
    hq_env.check_process_exited(w, expected_code=None)
    table = hq_env.command(["worker", "list"], as_table=True)
    assert table[1][1] == "IDLE TIMEOUT"

from conftest import HqEnv
import time


def test_worker_list(hq_env: HqEnv):
    hq_env.start_server()
    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 1
    assert table[0][:2] == ["Id", "State"]

    hq_env.start_worker()
    hq_env.start_worker()

    time.sleep(0.2)

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[0][:2] == ["Id", "State"]
    assert table[1][:2] == ["1", "RUNNING"]
    assert table[2][:2] == ["2", "RUNNING"]

    hq_env.kill_worker(2)
    time.sleep(0.1)

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[0][:2] == ["Id", "State"]
    assert table[1][:2] == ["1", "RUNNING"]
    assert table[2][:2] == ["2", "OFFLINE"]


    hq_env.kill_worker(1)
    time.sleep(0.1)

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 3
    assert table[0][:2] == ["Id", "State"]
    assert table[1][:2] == ["1", "OFFLINE"]
    assert table[2][:2] == ["2", "OFFLINE"]

    hq_env.start_worker()
    time.sleep(0.2)

    table = hq_env.command(["worker", "list"], as_table=True)

    assert len(table) == 4
    assert table[0][:2] == ["Id", "State"]
    assert table[1][:2] == ["1", "OFFLINE"]
    assert table[2][:2] == ["2", "OFFLINE"]
    assert table[3][:2] == ["3", "RUNNING"]
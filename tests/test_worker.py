import json
import os
import time
from socket import gethostname

import pytest

from .conftest import HqEnv, get_hq_binary
from .utils import wait_for_job_state, wait_for_worker_state
from .utils.table import Table


def test_worker_list(hq_env: HqEnv):
    hq_env.start_server()
    table = list_all_workers(hq_env)
    assert len(table) == 0
    assert table.header[:2] == ["ID", "State"]

    hq_env.start_workers(2)

    table = list_all_workers(hq_env)
    assert len(table) == 2
    table.check_columns_value(["ID", "State"], 0, ["1", "RUNNING"])
    table.check_columns_value(["ID", "State"], 1, ["2", "RUNNING"])

    hq_env.kill_worker(2)
    wait_for_worker_state(hq_env, 2, "CONNECTION LOST")

    table = list_all_workers(hq_env)
    assert len(table) == 2
    table.check_columns_value(["ID", "State"], 0, ["1", "RUNNING"])
    table.check_columns_value(["ID", "State"], 1, ["2", "CONNECTION LOST"])

    hq_env.kill_worker(1)
    wait_for_worker_state(hq_env, 1, "CONNECTION LOST")

    table = list_all_workers(hq_env)
    assert len(table) == 2
    table.check_columns_value(["ID", "State"], 0, ["1", "CONNECTION LOST"])
    table.check_columns_value(["ID", "State"], 1, ["2", "CONNECTION LOST"])

    hq_env.start_worker()
    wait_for_worker_state(hq_env, 3, "RUNNING")

    table = list_all_workers(hq_env)

    assert len(table) == 3
    table.check_columns_value(["ID", "State"], 0, ["1", "CONNECTION LOST"])
    table.check_columns_value(["ID", "State"], 1, ["2", "CONNECTION LOST"])
    table.check_columns_value(["ID", "State"], 2, ["3", "RUNNING"])


def test_worker_list_filter(hq_env: HqEnv):
    hq_env.start_server()
    workers = hq_env.start_workers(10)
    hq_env.command(["worker", "stop", "8-10"])
    for worker in workers[7:]:
        worker.wait(timeout=5)
        hq_env.check_process_exited(worker)

    table = hq_env.command(["worker", "list", "--filter", "offline"], as_table=True)
    assert len(table.rows) == 3
    for i in table.rows:
        assert i[1] == "STOPPED"

    table = hq_env.command(["worker", "list", "--filter", "running"], as_table=True)
    assert len(table.rows) == 7
    for i in table.rows:
        assert i[1] == "RUNNING"

    table = hq_env.command(["worker", "list", "--all"], as_table=True)
    assert len(table.rows) == 10
    for i in table.rows:
        assert i[1] in ("RUNNING", "STOPPED")


def test_worker_stop(hq_env: HqEnv):
    hq_env.start_server()
    process = hq_env.start_worker()

    wait_for_worker_state(hq_env, 1, "RUNNING")
    hq_env.command(["worker", "stop", "1"])
    wait_for_worker_state(hq_env, 1, "STOPPED")
    hq_env.check_process_exited(process)

    response = hq_env.command(["worker", "stop", "1"])
    assert "worker is already stopped" in response
    response = hq_env.command(["worker", "stop", "2"])
    assert "worker not found" in response


def test_worker_stop_some(hq_env: HqEnv):
    hq_env.start_server()
    processes = [hq_env.start_worker() for _ in range(3)]

    wait_for_worker_state(hq_env, [1, 2, 3], "RUNNING")
    r = hq_env.command(["worker", "stop", "1-4"])
    wait_for_worker_state(hq_env, [1, 2, 3], "STOPPED")

    for process in processes:
        hq_env.check_process_exited(process)
    assert "Stopping worker 4 failed" in r


def test_worker_stop_all(hq_env: HqEnv):
    hq_env.start_server()
    processes = [hq_env.start_worker() for _ in range(4)]

    wait_for_worker_state(hq_env, [1, 2, 3, 4], ["RUNNING" for _ in range(4)])
    hq_env.command(["worker", "stop", "all"])
    wait_for_worker_state(hq_env, [1, 2, 3, 4], ["STOPPED" for _ in range(4)])

    for process in processes:
        hq_env.check_process_exited(process)


def test_worker_stop_last(hq_env: HqEnv):
    hq_env.start_server()
    processes = [hq_env.start_worker() for _ in range(4)]

    wait_for_worker_state(hq_env, [1, 2, 3, 4], ["RUNNING" for _ in range(4)])
    hq_env.command(["worker", "stop", "last"])
    wait_for_worker_state(hq_env, [4], ["STOPPED" for _ in range(4)])

    hq_env.check_process_exited(processes[3])


def test_worker_list_only_online(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(2)

    wait_for_worker_state(hq_env, [1, 2], "RUNNING")

    table = list_all_workers(hq_env)
    assert len(table) == 2
    table.check_columns_value(["ID", "State"], 0, ["1", "RUNNING"])
    table.check_columns_value(["ID", "State"], 1, ["2", "RUNNING"])
    hq_env.kill_worker(2)

    wait_for_worker_state(hq_env, 2, "CONNECTION LOST")
    table = list_all_workers(hq_env)
    assert len(table) == 2
    table.check_columns_value(["ID", "State"], 0, ["1", "RUNNING"])
    table.check_columns_value(["ID", "State"], 1, ["2", "CONNECTION LOST"])

    table = hq_env.command(["worker", "list"], as_table=True)
    assert len(table) == 1
    table.check_columns_value(["ID", "State"], 0, ["1", "RUNNING"])


def test_worker_list_resources(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="10")
    hq_env.start_worker(cpus="4x5")
    hq_env.start_worker(cpus="[[100, 200, 300], [400, 500, 600]]")
    hq_env.start_worker(cpus="[[0, 1], [2], [10, 11, 12]]")

    wait_for_worker_state(hq_env, [1, 2, 3, 4], "RUNNING")

    table = list_all_workers(hq_env)
    assert len(table) == 4
    table.check_columns_value(["ID", "State", "Resources"], 0, ["1", "RUNNING", "cpus 10"])
    table.check_columns_value(["ID", "State", "Resources"], 1, ["2", "RUNNING", "cpus 4x5"])
    table.check_columns_value(["ID", "State", "Resources"], 2, ["3", "RUNNING", "cpus 2x3"])
    table.check_columns_value(["ID", "State", "Resources"], 3, ["4", "RUNNING", "cpus 1x1 1x2 1x3"])


def test_idle_timeout_server_cfg(hq_env: HqEnv):
    hq_env.start_server(args=["--idle-timeout", "1s"])
    w = hq_env.start_worker(args=["--heartbeat", "500ms"])
    time.sleep(0.5)
    hq_env.command(["submit", "--", "sleep", "1"])
    time.sleep(1.0)
    table = list_all_workers(hq_env)
    table.check_column_value("State", 0, "RUNNING")

    time.sleep(1.5)
    hq_env.check_process_exited(w, expected_code=None)
    table = list_all_workers(hq_env)
    table.check_column_value("State", 0, "IDLE TIMEOUT")


def test_idle_timeout_worker_cfg(hq_env: HqEnv):
    hq_env.start_server()
    w = hq_env.start_worker(args=["--heartbeat", "500ms", "--idle-timeout", "1s"])
    time.sleep(0.5)
    hq_env.command(["submit", "--", "sleep", "1"])
    time.sleep(1.0)
    table = list_all_workers(hq_env)
    table.check_column_value("State", 0, "RUNNING")

    time.sleep(1.5)
    hq_env.check_process_exited(w, expected_code=None)
    table = list_all_workers(hq_env)
    table.check_column_value("State", 0, "IDLE TIMEOUT")


def test_worker_time_limit(hq_env: HqEnv):
    hq_env.start_server()
    w = hq_env.start_worker(args=["--time-limit", "1s 200ms"])
    wait_for_worker_state(hq_env, 1, "RUNNING")
    wait_for_worker_state(hq_env, 1, "TIME LIMIT REACHED")
    hq_env.check_process_exited(w, expected_code=None)

    table = hq_env.command(["worker", "info", "1"], as_table=True)
    table.check_row_value("Time Limit", "1s 200ms")


def test_worker_info(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="10", args=["--heartbeat", "10s", "--manager", "none"])

    table = hq_env.command(["worker", "info", "1"], as_table=True)
    table.check_row_value("Worker ID", "1")
    table.check_row_value("Heartbeat", "10s")
    table.check_row_value("Resources", "cpus: 10")
    table.check_row_value("Manager", "None")
    table.check_row_value("Group", "default")


def test_worker_group(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="10", args=["--group", "test_1"])

    table = hq_env.command(["worker", "info", "1"], as_table=True)
    table.check_row_value("Group", "test_1")


def test_worker_address(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.start_worker(set_hostname=False, wait_for_start=False)
    wait_for_worker_state(hq_env, [1, 2], "RUNNING")

    output = hq_env.command(["worker", "address", "1"]).strip()
    assert output == "worker1"

    output = hq_env.command(["worker", "address", "2"]).strip()
    assert output == gethostname()


@pytest.mark.parametrize("policy", ["stop", "finish-running"])
def test_server_lost_no_tasks(hq_env: HqEnv, policy: str):
    hq_env.start_server()
    worker = hq_env.start_worker(on_server_lost=policy)
    hq_env.kill_server()
    time.sleep(0.5)
    hq_env.check_process_exited(worker, expected_code=1)


def test_server_lost_stop_with_task(hq_env: HqEnv):
    hq_env.start_server()
    worker = hq_env.start_worker(on_server_lost="stop")
    path = os.path.join(hq_env.work_path, "finished")
    hq_env.command(["submit", "--array=1-10", "--", "bash", "-c", f"sleep 3; touch {path}"])
    wait_for_job_state(hq_env, 1, "RUNNING")
    hq_env.kill_server()
    hq_env.check_process_exited(worker, expected_code=1)
    assert not os.path.isfile(path)


def test_server_lost_finish_running_with_task(hq_env: HqEnv):
    hq_env.start_server()
    worker = hq_env.start_worker(on_server_lost="finish-running")
    path = os.path.join(hq_env.work_path, "finished")
    hq_env.command(
        [
            "submit",
            "--array=1-10",
            "--",
            "bash",
            "-c",
            f"sleep 2; touch {path}-$HQ_TASK_ID",
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")
    hq_env.kill_server()
    time.sleep(0.5)
    assert worker.poll() is None
    time.sleep(2.0)
    hq_env.check_process_exited(worker, expected_code=1)

    files = [path for path in os.listdir(hq_env.work_path) if path.startswith("finished-")]
    assert len(files) == 1


def test_server_lost_finish_running_explicit_stop(hq_env: HqEnv):
    hq_env.start_server()
    worker = hq_env.start_worker(on_server_lost="finish-running")
    hq_env.command(
        [
            "submit",
            "sleep",
            "3600",
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")
    hq_env.command(["worker", "stop", "1"])
    wait_for_worker_state(hq_env, 1, "STOPPED")
    hq_env.check_process_exited(worker)


def list_all_workers(hq_env: HqEnv) -> Table:
    return hq_env.command(["worker", "list", "--all"], as_table=True)


def test_worker_wait(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(on_server_lost="finish-running")
    hq_env.start_worker(on_server_lost="finish-running")
    done = hq_env.command(["worker", "wait", "2"])
    assert "" == done


def test_deploy_ssh_error(hq_env: HqEnv):
    hq_env.start_server()
    with hq_env.mock.mock_program_with_code(
        "ssh",
        """
            exit(42)
    """,
    ):
        nodefile = prepare_localhost_nodefile()
        hq_env.command(["worker", "deploy-ssh", nodefile], expect_fail="Worker exited with code 42")


def ssh_code_dump_args_to_file(path: str) -> str:
    return f"""
import json
import sys

with open("{path}", "w") as f:
    json.dump(sys.argv, f)
"""


def test_deploy_ssh_worker_command(hq_env: HqEnv):
    hq_env.start_server()
    with hq_env.mock.mock_program_with_code("ssh", ssh_code_dump_args_to_file("command.txt")):
        nodefile = prepare_localhost_nodefile()
        hq_env.command(["worker", "deploy-ssh", "--show-output", nodefile, "--time-limit", "3s"])
        with open("command.txt") as f:
            cmdline = json.load(f)
            cmdline = cmdline[cmdline.index("--") + 1 :]
            cmdline = [c.replace(get_hq_binary(), "<hq-binary>") for c in cmdline]
            assert cmdline == ["<hq-binary>", "worker", "start", "--time-limit", "3s"]


def test_deploy_ssh_wait_for_worker(hq_env: HqEnv):
    hq_env.start_server()
    with hq_env.mock.mock_program_with_code(
        "ssh",
        """
                import time
                time.sleep(1)
    """,
    ):
        nodefile = prepare_localhost_nodefile()
        hq_env.command(["worker", "deploy-ssh", nodefile])


def test_deploy_ssh_multiple_workers(hq_env: HqEnv):
    hq_env.start_server()
    with hq_env.mock.mock_program_with_code(
        "ssh",
        """
                import os
    
                os.makedirs("workers", exist_ok=True)
                with open(f"workers/{os.getpid()}", "w") as f:
                    f.write(str(os.getpid()))
                """,
    ):
        nodefile = prepare_localhost_nodefile(count=3)
        hq_env.command(["worker", "deploy-ssh", nodefile])
        assert len(os.listdir("workers")) == 3


def test_deploy_ssh_show_output(hq_env: HqEnv):
    hq_env.start_server()
    with hq_env.mock.mock_program_with_code(
        "ssh",
        """
                print("FOOBAR")
        """,
    ):
        nodefile = prepare_localhost_nodefile(count=3)
        output = hq_env.command(["worker", "deploy-ssh", "--show-output", nodefile])
        assert "FOOBAR" in output


def test_deploy_ssh_custom_port(hq_env: HqEnv):
    hq_env.start_server()
    path = "nodefile.txt"
    with open(path, "w") as f:
        print("localhost:1234", file=f)

    with hq_env.mock.mock_program_with_code("ssh", ssh_code_dump_args_to_file("command.txt")) as mock:
        hq_env.command(["worker", "deploy-ssh", path])
        with open("command.txt") as f:
            cmdline = json.load(f)
            cmdline = cmdline[: cmdline.index("--")]
            assert cmdline == [str(mock), "localhost", "-t", "-t", "-p", "1234"]


def prepare_localhost_nodefile(count: int = 1) -> str:
    path = "nodefile.txt"
    with open(path, "w") as f:
        for _ in range(count):
            print("localhost", file=f)
    return path


def test_worker_state_info(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    table = hq_env.command(["worker", "info", "1"], as_table=True)
    table.check_row_value("State", "RUNNING")
    table.check_row_value("Runtime Info", "assigned tasks: 0")
    table.check_row_value("Last task started", "")

    hq_env.command(["submit", "--array=1-2", "--", "sleep", "1"])
    wait_for_job_state(hq_env, 1, "RUNNING")
    table = hq_env.command(["worker", "info", "1"], as_table=True)
    table.check_row_value("State", "RUNNING")
    assert "Job: 1;" in table.get_row_value("Last task started")
    table.check_row_value("Runtime Info", "assigned tasks: 2; running tasks: 1")
    hq_env.start_worker()
    hq_env.command(["submit", "--nodes=2", "--", "sleep", "1"])
    wait_for_job_state(hq_env, 2, "RUNNING")
    table = hq_env.command(["worker", "info", "1"], as_table=True)
    a = table.get_row_value("Runtime Info")
    assert "Job: 2;" in table.get_row_value("Last task started")
    table = hq_env.command(["worker", "info", "2"], as_table=True)
    b = table.get_row_value("Runtime Info")
    assert "Job: 2;" in table.get_row_value("Last task started")
    assert {a, b} == {"running multinode task; main node", "running multinode task; secondary node"}

import os
import signal
import subprocess
import time
from datetime import datetime
from os.path import isdir, isfile, join
from pathlib import Path
from typing import Callable, Optional

import pytest

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.cmd import python
from .utils.io import check_file_contents, read_file
from .utils.job import default_task_output, list_jobs
from .utils.table import parse_multiline_cell
from .utils.wait import wait_for_pid_exit, wait_for_worker_state, wait_until


def test_job_submit(hq_env: HqEnv):
    hq_env.start_server()
    table = list_jobs(hq_env)
    assert len(table) == 0
    assert table.header[:3] == ["ID", "Name", "State"]

    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello2'"])

    wait_for_job_state(hq_env, [1, 2], "WAITING")

    table = list_jobs(hq_env)
    assert len(table) == 2
    table.check_columns_value(["ID", "Name", "State"], 0, ["1", "bash", "WAITING"])
    table.check_columns_value(["ID", "Name", "State"], 1, ["2", "bash", "WAITING"])

    hq_env.start_worker(cpus=1)

    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    table = list_jobs(hq_env)
    assert len(table) == 2
    table.check_columns_value(["ID", "Name", "State"], 0, ["1", "bash", "FINISHED"])
    table.check_columns_value(["ID", "Name", "State"], 1, ["2", "bash", "FINISHED"])

    hq_env.command(["submit", "--", "sleep", "1"])

    wait_for_job_state(hq_env, 3, "RUNNING", sleep_s=0.2)

    table = list_jobs(hq_env)
    assert len(table) == 3
    table.check_columns_value(["ID", "Name", "State"], 0, ["1", "bash", "FINISHED"])
    table.check_columns_value(["ID", "Name", "State"], 1, ["2", "bash", "FINISHED"])
    table.check_columns_value(["ID", "Name", "State"], 2, ["3", "sleep", "RUNNING"])

    wait_for_job_state(hq_env, 3, "FINISHED")

    table = list_jobs(hq_env)
    assert len(table) == 3
    table.check_columns_value(["ID", "Name", "State"], 0, ["1", "bash", "FINISHED"])
    table.check_columns_value(["ID", "Name", "State"], 1, ["2", "bash", "FINISHED"])
    table.check_columns_value(["ID", "Name", "State"], 2, ["3", "sleep", "FINISHED"])


def test_job_submit_output(hq_env: HqEnv):
    hq_env.start_server()

    output = hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    assert output.strip() == "Job submitted successfully, job ID: 1"


def test_job_submit_dir(hq_env: HqEnv):
    hq_env.start_server()

    submit_dir = Path(hq_env.server_dir) / "submit"
    submit_dir.mkdir()
    hq_env.command(["submit", "echo", "tt"], cwd=submit_dir)

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Submission directory", str(submit_dir))


def test_job_custom_name(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(["submit", "--name=sleep_prog", "sleep", "1"])
    wait_for_job_state(hq_env, 1, "WAITING")

    table = list_jobs(hq_env)
    assert len(table) == 1
    table.check_columns_value(["ID", "Name", "State"], 0, ["1", "sleep_prog", "WAITING"])

    with pytest.raises(Exception):
        hq_env.command(["submit", "--name=second_sleep \n", "sleep", "1"])
    with pytest.raises(Exception):
        hq_env.command(["submit", "--name=second_sleep \t", "sleep", "1"])

    table = list_jobs(hq_env)
    assert len(table) == 1


def test_job_truncate_long_name(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        [
            "submit",
            "--name=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "sleep",
            "1",
        ]
    )
    wait_for_job_state(hq_env, 1, "WAITING")

    table = list_jobs(hq_env)
    table.check_column_value(
        "Name",
        0,
        "aaaaaaaaaaaaaaaaaaaaaaaa...bbbbbbbbbbbbbbbbbbbbbbb",
    )


def test_job_custom_working_dir(hq_env: HqEnv, tmpdir):
    hq_env.start_server()

    test_string = "cwd_test_string"
    work_dir = tmpdir.mkdir("test_dir")
    test_file = work_dir.join("testfile")
    test_file.write(test_string)

    submit_dir = tmpdir.mkdir("submit_dir")
    hq_env.command(
        ["submit", f"--cwd={work_dir}", "--", "bash", "-c", "cat testfile"],
        cwd=submit_dir,
    )
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Working directory", str(work_dir))

    hq_env.start_worker(cpus=1)
    wait_for_job_state(hq_env, 1, ["FINISHED"])

    check_file_contents(default_task_output(working_dir=work_dir), test_string)


def test_job_output_default(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    hq_env.command(["submit", "--", "ls", "/non-existent"])
    hq_env.command(["submit", "--", "/non-existent-program"])

    wait_for_job_state(hq_env, [1, 2, 3], ["FINISHED", "FAILED"])

    check_file_contents(os.path.join(tmp_path, default_task_output(job_id=1, type="stdout")), "hello\n")
    check_file_contents(os.path.join(tmp_path, default_task_output(job_id=1, type="stderr")), "")
    check_file_contents(os.path.join(tmp_path, default_task_output(job_id=2, type="stdout")), "")

    with open(os.path.join(tmp_path, default_task_output(job_id=2, type="stderr"))) as f:
        data = f.read()
        assert "No such file or directory" in data
        assert data.startswith("ls:")

    check_file_contents(os.path.join(tmp_path, default_task_output(job_id=3, type="stdout")), "")
    check_file_contents(os.path.join(tmp_path, default_task_output(job_id=3, type="stderr")), "")


def test_job_create_output_folders(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(
        [
            "submit",
            "--stdout",
            "foo/1/job.out",
            "--stderr",
            "foo/1/job.err",
            "--",
            "echo",
            "hi",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    assert isdir("foo/1")
    assert isfile("foo/1/job.out")
    assert isfile("foo/1/job.err")


def test_job_output_configured(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--stdout=abc", "--stderr=xyz", "--", "bash", "-c", "echo 'hello'"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    check_file_contents(os.path.join(tmp_path, "abc"), "hello\n")
    check_file_contents(os.path.join(tmp_path, "xyz"), "")


def test_job_output_absolute_path(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(
        [
            "submit",
            f"--stdout={os.path.abspath('abc')}",
            f"--stderr={os.path.abspath('xyz')}",
            "--",
            "bash",
            "-c",
            "echo 'hello'",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    check_file_contents(os.path.join(tmp_path, "abc"), "hello\n")
    check_file_contents(os.path.join(tmp_path, "xyz"), "")


def test_job_output_none(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--stdout=none", "--stderr=none", "--", "bash", "-c", "echo 'hello'"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    assert not os.path.exists(os.path.join(tmp_path, "none"))
    assert not os.path.exists(os.path.join(tmp_path, default_task_output(job_id=1, task_id=0, type="stdout")))
    assert not os.path.exists(os.path.join(tmp_path, default_task_output(job_id=1, task_id=0, type="stderr")))


def test_job_filters(hq_env: HqEnv):
    hq_env.start_server()

    table_empty = hq_env.command(["job", "list"], as_table=True)
    assert len(table_empty) == 0

    hq_env.command(["submit", "--", "bash", "-c", "echo 'to cancel'"])
    hq_env.command(["submit", "--", "bash", "-c", "echo 'bye'"])
    hq_env.command(["submit", "--", "ls", "failed"])

    wait_for_job_state(hq_env, [1, 2, 3], "WAITING")

    r = hq_env.command(["job", "cancel", "1"])
    assert "Job 1 canceled" in r

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "CANCELED")
    table.check_column_value("State", 1, "WAITING")
    table.check_column_value("State", 2, "WAITING")
    assert len(table) == 3

    table_canceled = hq_env.command(["job", "list", "--filter", "canceled"], as_table=True)
    assert len(table_canceled) == 1

    table_waiting = hq_env.command(["job", "list", "--filter", "waiting"], as_table=True)
    assert len(table_waiting) == 2

    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "sleep", "1"])

    wait_for_job_state(hq_env, 4, "RUNNING")

    table_running = hq_env.command(["job", "list", "--filter", "running"], as_table=True)
    assert len(table_running) == 1

    table_finished = hq_env.command(["job", "list", "--filter", "finished"], as_table=True)
    assert len(table_finished) == 1

    table_failed = hq_env.command(["job", "list", "--filter", "failed"], as_table=True)
    assert len(table_failed) == 1

    table_failed = hq_env.command(["job", "list", "--filter", "failed,finished"], as_table=True)
    assert len(table_failed) == 2


def test_job_list_hidden_jobs(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "hostname"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    output = hq_env.command(["job", "list"])
    assert "There is 1 job in total." in output

    hq_env.command(["submit", "hostname"])
    wait_for_job_state(hq_env, 2, "FINISHED")

    output = hq_env.command(["job", "list"])
    assert "There are 2 jobs in total." in output


def test_job_summary(hq_env: HqEnv):
    hq_env.start_server()

    def check(running=0, waiting=0, finished=0, failed=0, canceled=0):
        table = hq_env.command(["job", "summary"], as_table=True)

        items = (
            ("RUNNING", running),
            ("WAITING", waiting),
            ("FINISHED", finished),
            ("FAILED", failed),
            ("CANCELED", canceled),
        )
        for index, (status, count) in enumerate(items):
            table.check_column_value("Status", index, status)
            table.check_column_value("Count", index, str(count))

    check()

    hq_env.command(["submit", "ls"])
    wait_for_job_state(hq_env, 1, "WAITING")
    check(waiting=1)

    hq_env.start_worker()

    wait_for_job_state(hq_env, 1, "FINISHED")
    check(finished=1)

    hq_env.command(["submit", "non-existent"])
    wait_for_job_state(hq_env, 2, "FAILED")
    check(finished=1, failed=1)


def test_job_fail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "/non-existent-program"])
    wait_for_job_state(hq_env, 1, "FAILED")

    table = list_jobs(hq_env)
    assert len(table) == 1
    table.check_column_value("ID", 0, "1")
    table.check_column_value("Name", 0, "non-existent-program")
    table.check_column_value("State", 0, "FAILED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table[0].check_row_value("ID", "1")
    table[0].check_row_value("State", "FAILED")

    table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    table.check_column_value("Task ID", 0, "0")
    assert "No such file or directory" in table.get_column_value("Error")[0]


def test_job_invalid(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    result = hq_env.command(["job", "info", "5"])
    assert "Job 5 not found" in result


def test_cancel_without_workers(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "/bin/hostname"])
    r = hq_env.command(["job", "cancel", "1"])
    assert "Job 1 canceled" in r
    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "CANCELED")
    hq_env.start_worker(cpus=1)
    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "CANCELED")


def test_cancel_running(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "sleep", "10"])

    wait_for_job_state(hq_env, 1, "RUNNING")

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "RUNNING")
    r = hq_env.command(["job", "cancel", "1"])
    assert "Job 1 canceled" in r
    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "CANCELED")

    r = hq_env.command(["job", "cancel", "1"])
    assert "Canceling job 1 failed" in r


def test_cancel_finished(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])

    wait_for_job_state(hq_env, [1, 2], ["FINISHED", "FAILED"])

    r = hq_env.command(["job", "cancel", "1"])
    assert "Canceling job 1 failed" in r
    r = hq_env.command(["job", "cancel", "2"])
    assert "Canceling job 2 failed" in r

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "FINISHED")
    table.check_column_value("State", 1, "FAILED")


def test_cancel_last(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])

    wait_for_job_state(hq_env, [1, 2], ["FINISHED", "FAILED"])

    r = hq_env.command(["job", "cancel", "last"])
    assert "Canceling job 2 failed" in r


def test_cancel_some(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "sleep", "100"])
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])

    r = hq_env.command(["job", "cancel", "1-4"])
    assert "Canceling job 4 failed" in r

    table = list_jobs(hq_env)
    for i in range(3):
        table.check_column_value("State", i, "CANCELED")


def test_cancel_all(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])

    wait_for_job_state(hq_env, [1, 2], ["FINISHED", "FAILED"])

    hq_env.command(["submit", "sleep", "100"])

    r = hq_env.command(["job", "cancel", "all"]).splitlines()
    assert len(r) == 1
    assert "Job 3 canceled" in r[0]


def test_cancel_terminate_process_children(hq_env: HqEnv):
    def cancel(worker_process):
        hq_env.command(["job", "cancel", "1"])
        wait_for_job_state(hq_env, 1, "CANCELED")

    check_child_process_exited(hq_env, cancel)


def test_cancel_send_sigint(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--",
            *python(
                """
import sys
import time
import signal

def signal_handler(sig, frame):
    print("sigint", flush=True)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

print("ready", flush=True)
time.sleep(3600)
"""
            ),
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")
    wait_until(lambda: read_file(default_task_output()).strip() == "ready")

    hq_env.command(["job", "cancel", "1"])
    wait_for_job_state(hq_env, 1, "CANCELED")

    wait_until(lambda: read_file(default_task_output()).splitlines(keepends=False)[1] == "sigint")


def test_cancel_kill_if_sigint_fails(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--",
            *python(
                """
import os
import sys
import time
import signal

def signal_handler(sig, frame):
    print(os.getpid(), flush=True)
    time.sleep(3600)

signal.signal(signal.SIGINT, signal_handler)

print("ready", flush=True)
time.sleep(3600)
"""
            ),
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")
    wait_until(lambda: read_file(default_task_output()).strip() == "ready")

    hq_env.command(["job", "cancel", "1"])
    wait_for_job_state(hq_env, 1, "CANCELED")

    wait_until(lambda: len(read_file(default_task_output()).splitlines()) == 2)

    pid = int(read_file(default_task_output()).splitlines()[1])
    wait_for_pid_exit(pid)


def test_reporting_state_after_worker_lost(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(2, cpus=1)
    hq_env.command(["submit", "sleep", "2"])
    hq_env.command(["submit", "sleep", "2"])

    wait_for_job_state(hq_env, [1, 2], "RUNNING")

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "RUNNING")
    table.check_column_value("State", 1, "RUNNING")
    hq_env.kill_worker(1)

    def task_is_waiting():
        table = list_jobs(hq_env)
        if table.get_column_value("State")[0] == "WAITING":
            return 0, 1
        elif table.get_column_value("State")[1] == "WAITING":
            return 1, 0
        else:
            return None

    idx, other = wait_until(task_is_waiting)
    table = list_jobs(hq_env)
    assert table.get_column_value("State")[other] == "RUNNING"

    wait_for_job_state(hq_env, other + 1, "FINISHED")

    table = list_jobs(hq_env)
    assert table.get_column_value("State")[other] == "FINISHED"
    assert table.get_column_value("State")[idx] == "RUNNING"

    wait_for_job_state(hq_env, idx + 1, "FINISHED")

    table = list_jobs(hq_env)
    assert table.get_column_value("State")[other] == "FINISHED"
    assert table.get_column_value("State")[idx] == "FINISHED"


def test_set_env(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(1)
    hq_env.command(
        [
            "submit",
            "--env",
            "FOO=BAR",
            "--env",
            "FOO2=BAR2",
            "--",
            "bash",
            "-c",
            "echo $FOO $FOO2",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    check_file_contents(os.path.join(hq_env.work_path, default_task_output()), "BAR BAR2\n")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Environment", "FOO=BAR\nFOO2=BAR2")


def test_max_fails_0(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--array",
            "1-2",
            "--stdout",
            "none",
            "--stderr",
            "none",
            "--max-fails",
            "0",
            "--",
            "bash",
            "-c",
            "if [ $HQ_TASK_ID == 1 ]; then exit 1; fi; if [ $HQ_TASK_ID == 2 ]; then sleep 100; fi",
        ]
    )
    hq_env.start_workers(1, cpus=2)

    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    states = table[0].get_row_value("State").split("\n")
    assert "FAILED (1)" in states
    assert any(s.startswith("CANCELED") for s in states)


def test_max_fails_1(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--array",
            "1-200",
            "--stdout",
            "none",
            "--stderr",
            "none",
            "--max-fails",
            "1",
            "--",
            "bash",
            "-c",
            "if [ $HQ_TASK_ID == 137 ]; then exit 1; fi",
        ]
    )
    hq_env.start_workers(1)

    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    states = table[0].get_row_value("State").split("\n")
    assert "FAILED (1)" in states
    assert "FINISHED (199)" in states


def test_max_fails_many(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--array",
            "1-10",
            "--stdout",
            "none",
            "--stderr",
            "none",
            "--max-fails",
            "3",
            "--",
            "bash",
            "-c",
            "sleep 1; exit 1",
        ]
    )
    hq_env.start_workers(1)

    time.sleep(5)
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    states = table[0].get_row_value("State").split("\n")
    assert "FAILED (4)" in states
    assert "CANCELED (6)" in states


def test_job_last(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["job", "info", "last"])

    hq_env.command(["submit", "ls"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["job", "info", "last"], as_table=True)
    table.check_row_value("ID", "1")

    hq_env.command(["submit", "ls"])
    wait_for_job_state(hq_env, 2, "FINISHED")

    table = hq_env.command(["job", "info", "last"], as_table=True)
    table.check_row_value("ID", "2")


def test_job_priority(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--priority",
            "1",
            "--",
            "bash",
            "-c",
            "date --iso-8601=seconds && sleep 1",
        ]
    )
    hq_env.command(
        [
            "submit",
            "--priority",
            "3",
            "--",
            "bash",
            "-c",
            "date --iso-8601=seconds && sleep 1",
        ]
    )
    hq_env.command(
        [
            "submit",
            "--priority",
            "3",
            "--",
            "bash",
            "-c",
            "date --iso-8601=seconds && sleep 1",
        ]
    )
    hq_env.command(
        [
            "submit",
            "--",
            "bash",
            "-c",
            "date --iso-8601=seconds && sleep 1",
        ]
    )
    hq_env.start_worker(cpus=1)

    wait_for_job_state(hq_env, 1, "FINISHED")
    wait_for_job_state(hq_env, 2, "FINISHED")
    wait_for_job_state(hq_env, 3, "FINISHED")
    wait_for_job_state(hq_env, 4, "FINISHED")

    dates = []
    for file in [default_task_output(job_id=id, type="stdout") for id in range(1, 5)]:
        with open(os.path.join(tmp_path, file)) as f:
            dates.append(datetime.fromisoformat(f.read().strip()))

    assert dates[1] < dates[0]
    assert dates[2] < dates[0]
    assert dates[0] < dates[3]


def test_job_tasks_table(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(["submit", "echo", "test"])
    table = hq_env.command(["task", "list", "1"], as_table=True)
    wait_for_job_state(hq_env, 1, "WAITING")
    table.check_column_value("Worker", 0, "")

    hq_env.start_worker()

    table = hq_env.command(["task", "list", "1"], as_table=True)
    wait_for_job_state(hq_env, 1, "FINISHED")
    table.check_column_value("Worker", 0, "worker1")

    hq_env.command(["submit", "non-existent-program", "test"])
    table = hq_env.command(["task", "list", "2"], as_table=True)
    wait_for_job_state(hq_env, 2, "FAILED")
    worker = table.get_column_value("Worker")[0]
    assert worker == "" or worker == "worker1"


def test_job_tasks_makespan(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "sleep", "10"])
    hq_env.command(["job", "cancel", "1"])
    wait_for_job_state(hq_env, 1, "CANCELED")

    times_1 = hq_env.command(["task", "list", "1"], as_table=True).get_column_value("Makespan")
    times_2 = hq_env.command(["task", "list", "1"], as_table=True).get_column_value("Makespan")
    assert times_1 == times_2


def test_job_wait(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "sleep", "1"])
    r = hq_env.command(["job", "wait", "1"])
    assert "1 job finished" in r

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("State", "FINISHED")

    r = hq_env.command(["job", "wait", "all"])
    assert "1 job finished" in r


def test_job_submit_wait(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    r = hq_env.command(["submit", "--wait", "sleep", "1"])
    assert "1 job finished" in r

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("State", "FINISHED")


def test_job_wait_failure_exit_code(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    process = hq_env.command(["submit", "--wait", "non-existent-program"], wait=False)
    assert process.wait() == 1


def test_job_wait_cancellation_exit_code(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    process = hq_env.command(["submit", "--wait", "sleep", "100"], wait=False)
    wait_for_job_state(hq_env, 1, "RUNNING")

    hq_env.command(["job", "cancel", "last"])

    assert process.wait() == 1


def test_job_completion_time(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "sleep", "1"])
    wait_for_job_state(hq_env, 1, "RUNNING")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Makespan") != "0s"
    assert not table.get_row_value("Makespan").startswith("1s")

    wait_for_job_state(hq_env, 1, "FINISHED")
    time.sleep(1.2)  # This sleep is not redundant, we check that after finished time is not moving

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Makespan").startswith("1s")

    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
    parse_multiline_cell(table.get_row_value("Times"))["Makespan"].startswith("1s")


def test_job_timeout(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="3")

    hq_env.command(["submit", "--time-limit=500ms", "sleep", "2"])
    hq_env.command(["submit", "--time-limit=3s", "sleep", "2"])
    hq_env.command(["submit", "sleep", "2"])

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Task time limit", "500ms")

    table = hq_env.command(["job", "info", "2"], as_table=True)
    table.check_row_value("Task time limit", "3s")

    table = hq_env.command(["job", "info", "3"], as_table=True)
    table.check_row_value("Task time limit", "None")

    wait_for_job_state(hq_env, 1, "FAILED")
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table[0].check_row_value("Task time limit", "500ms")
    assert table[0].get_row_value("Makespan").startswith("5")
    assert table[0].get_row_value("Makespan").endswith("ms")

    table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert table.get_column_value("Error")[0] == "Time limit reached"

    wait_for_job_state(hq_env, 2, "FINISHED")
    table = hq_env.command(["job", "info", "2"], as_table=True)
    assert table.get_row_value("Makespan").startswith("2")

    wait_for_job_state(hq_env, 3, "FINISHED")
    table = hq_env.command(["job", "info", "3"], as_table=True)
    assert table.get_row_value("Makespan").startswith("2")


def test_job_submit_program_not_found(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "foo", "--bar", "--baz=5"])
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert (
        'Error: Cannot execute "foo --bar --baz=5": No such file or directory (os error 2)\n'
        "The program that you have tried to execute (`foo`) was not found." == table.get_column_value("Error")[0]
    )


def test_job_submit_program_not_found_file_exists(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    with open("foo", "w") as f:
        f.write("hostname")

    hq_env.command(["submit", "foo"])
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert f"""Error: Cannot execute "foo": No such file or directory (os error 2)
The program that you have tried to execute (`foo`) was not found.
The file `{join(os.getcwd(), 'foo')}` exists, maybe you have meant `./foo` instead?""" == table.get_column_value(
        "Error"
    )[0]


def test_job_stdin_basic(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")

    hq_env.command(["submit", "bash"])
    wait_for_job_state(hq_env, 1, "FINISHED")
    with open(default_task_output(1), "rb") as f:
        assert f.read() == b""

    hq_env.command(["submit", "bash"], stdin="echo Hello")
    wait_for_job_state(hq_env, 2, "FINISHED")
    with open(default_task_output(2), "rb") as f:
        assert f.read() == b""

    hq_env.command(["submit", "--stdin", "bash"], stdin="echo Hello")
    wait_for_job_state(hq_env, 3, "FINISHED")
    with open(default_task_output(3), "rb") as f:
        assert f.read() == b"Hello\n"


def test_job_stdin_read_part(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")

    code = "import sys; sys.stdin.readline()"
    hq_env.command(["submit", "--stdin", "python", "-c", code], stdin="Line\n" * 20_000)
    wait_for_job_state(hq_env, 1, "FINISHED")

    code = "import sys; sys.stdin.readline(); sys.stdin.close()"
    hq_env.command(["submit", "--stdin", "python", "-c", code], stdin="Line\n" * 20_000)
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_job_shell_script_fail_not_executable(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    Path("test.sh").write_text("""echo 'Hello' > out.txt""")
    hq_env.command(["submit", "./test.sh"])
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert """Error: Cannot execute "./test.sh": Permission denied (os error 13)
The script that you have tried to execute (`./test.sh`) is not executable.
Try making it executable or add a shebang line to it.""" == table.get_column_value("Error")[0]


def test_job_shell_script_read_interpreter(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    Path("test.sh").write_text(
        """#!/bin/bash
echo 'Hello' > out.txt
"""
    )
    for job_id, path in enumerate(("test.sh", "./test.sh", os.path.realpath("test.sh"))):
        hq_env.command(["submit", path])
        wait_for_job_state(hq_env, job_id + 1, "FINISHED")

    check_file_contents("out.txt", "Hello\n")


@pytest.mark.parametrize("mode", ["FINISHED", "FAILED", "CANCELED"])
def test_submit_task_dir(hq_env: HqEnv, mode):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--task-dir",
            "--",
            "bash",
            "-c",
            "echo $HQ_TASK_DIR; touch $HQ_TASK_DIR/xyz; sleep 2; {}".format("exit 1" if mode == "FAILED" else ""),
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")

    def file_created():
        task_dir = read_file(default_task_output()).rstrip()
        if task_dir is None:
            return
        return os.path.isfile(os.path.join(task_dir, "xyz"))

    wait_until(file_created)
    task_dir = read_file(default_task_output()).rstrip()

    if mode == "CANCELED":
        hq_env.command(["job", "cancel", "all"])

    wait_for_job_state(hq_env, 1, mode)
    assert not os.path.exists(task_dir)


def test_custom_error_message(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--task-dir",
            "--",
            "bash",
            "-c",
            # "echo $HQ_ERROR",
            "echo 'Testing message' > \"${HQ_ERROR_FILENAME}\"; exit 1",
        ]
    )
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert table.get_column_value("Error")[0] == "Error: Testing message"
    # print(table)


def test_long_custom_error_message(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--task-dir",
            "--",
            "python",
            "-c",
            "import os; f = open(os.environ['HQ_ERROR_FILENAME'], 'w');f.write('a' * 10_000); f.flush(); sys.exit(1)",
        ]
    )
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert table.get_column_value("Error")[0].endswith("aaaaaa\n[The message was truncated]")


def test_zero_custom_error_message(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--task-dir",
            "--",
            "python",
            "-c",
            "import os; f = open(os.environ['HQ_ERROR_FILENAME'], 'w');sys.exit(1)",
        ]
    )
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert table.get_column_value("Error")[0] == "Error: Task created an error file, but it is empty"
    # print(table)


@pytest.mark.parametrize("count", [None, 1, 7])
def test_crashing_job_status_default(count: Optional[int], hq_env: HqEnv):
    hq_env.start_server()

    count = count if count is not None else 5

    hq_env.command(["submit", f"--crash-limit={count}", "sleep", "10"])

    for i in range(count):
        hq_env.start_worker()
        wait_for_job_state(hq_env, 1, "RUNNING")
        hq_env.kill_worker(i + 1)
        if i < count - 1:
            wait_for_job_state(hq_env, 1, "WAITING")

    wait_for_job_state(hq_env, 1, "FAILED")

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "FAILED")


def test_crashing_job_by_files(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--", "bash", "-c", "sleep 3; echo done > output.txt"])

    # Crashing tasks threshold is 5
    for i in range(5):
        hq_env.start_worker()
        wait_for_job_state(hq_env, 1, "RUNNING")
        hq_env.kill_worker(i + 1)
        if i < 4:
            wait_for_job_state(hq_env, 1, "WAITING")

    hq_env.start_worker()
    wait_for_job_state(hq_env, 1, "FAILED")

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "FAILED")
    assert not os.path.exists("output.txt")


def test_kill_task_when_worker_dies(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--",
            *python(
                """
import os
import time

print(os.getpid(), flush=True)
time.sleep(3600)
"""
            ),
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")

    def get_pid():
        pid = read_file(default_task_output()).strip()
        if not pid:
            return None
        return int(pid)

    pid = wait_until(get_pid)

    hq_env.kill_worker(1)

    wait_for_pid_exit(pid)


def test_kill_task_subprocess_when_worker_is_interrupted(hq_env: HqEnv):
    def interrupt_worker(worker_process):
        hq_env.kill_worker(1, signal=signal.SIGINT)

    check_child_process_exited(hq_env, interrupt_worker)


def test_kill_task_subprocess_when_worker_is_terminated(hq_env: HqEnv):
    def terminate_worker(worker_process):
        hq_env.kill_worker(1, signal=signal.SIGTERM)

    check_child_process_exited(hq_env, terminate_worker)


@pytest.mark.xfail
def test_kill_task_subprocess_when_worker_is_killed(hq_env: HqEnv):
    def terminate_worker(worker_process):
        hq_env.kill_worker(1, signal=signal.SIGKILL)

    check_child_process_exited(hq_env, terminate_worker)


def test_kill_task_subprocess_when_worker_is_stopped(hq_env: HqEnv):
    def stop_worker(worker_process):
        hq_env.command(["worker", "stop", "1"])
        wait_for_worker_state(hq_env, 1, "STOPPED")
        hq_env.check_process_exited(worker_process)

    check_child_process_exited(hq_env, stop_worker)


def test_fail_to_start_issue629(hq_env: HqEnv, tmpdir):
    """
    Regression test for https://github.com/It4innovations/hyperqueue/issues/629.
    By using an invalid stdout path, we should cause task spawning to fail, which should be handled gracefully.
    """
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--stdout=/dev/null/foo.txt", "ls"])
    wait_for_job_state(hq_env, 1, "FAILED")


def check_child_process_exited(hq_env: HqEnv, stop_fn: Callable[[subprocess.Popen], None]):
    """
    Creates a task that spawns a child, and then calls `stop_fn`, which should kill either the task
    or the worker. The function then checks that both the task process and its child have been killed.
    """
    hq_env.start_server()
    worker_process = hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--",
            *python(
                """
import os
import sys
import time
print(os.getpid(), flush=True)
pid = os.fork()
if pid > 0:
    print(pid, flush=True)
time.sleep(3600)
"""
            ),
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")
    wait_until(lambda: len(read_file(default_task_output()).splitlines()) == 2)
    pids = [int(pid) for pid in read_file(default_task_output()).splitlines()]

    stop_fn(worker_process)

    parent, child = pids
    wait_for_pid_exit(parent)
    wait_for_pid_exit(child)


def test_job_task_ids(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--array=2,7,9,20-30",
            "--",
            "python",
            "-c",
            "import os; assert os.environ['HQ_TASK_ID'] not in ['25', '26', '27', '28']",
        ]
    )
    hq_env.start_workers(1, cpus=1)
    wait_for_job_state(hq_env, 1, "FAILED")

    result = hq_env.command(["job", "task-ids", "1"])
    assert result == "2,7,9,20-30\n"

    result = hq_env.command(["job", "task-ids", "1", "--filter", "finished"])
    assert result == "2,7,9,20-24,29-30\n"

    result = hq_env.command(["job", "task-ids", "1", "--filter", "failed"])
    assert result == "25-28\n"

    result = hq_env.command(["job", "task-ids", "1", "--filter", "canceled"])
    assert result == "\n"


def test_job_open(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "open"])
    table = hq_env.command(["job", "list", "--all"], as_table=True)
    table.check_row_value("*1", "job")
    assert table.get_column_value("State") == ["OPENED"]

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Session", "open")

    assert "Job 1 closed" in hq_env.command(["job", "close", "1"])
    assert "job is already closed" in hq_env.command(["job", "close", "1"])
    assert "job is already closed" in hq_env.command(["job", "close", "1"])

    table = hq_env.command(["job", "list", "--all"], as_table=True)
    table.check_row_value("1", "job")
    assert table.get_column_value("State") == ["FINISHED"]

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Session", "closed")


def test_job_wait_for_close(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "open"])

    p = hq_env.command(["job", "wait", "1"], wait=False)
    time.sleep(1)
    assert p.poll() is None
    hq_env.command(["job", "close", "last"])
    wait_for_job_state(hq_env, 1, "FINISHED")
    time.sleep(0.3)
    r = p.poll()
    assert r == 0


def test_submit_job_opts_to_open_job(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "open"])

    hq_env.command(
        ["submit", "--job=1", "--name=Abc", "--", "hostname"],
        expect_fail="Parameter --name is not allowed when submitting to an open job.",
    )
    hq_env.command(
        ["submit", "--job=1", "--max-fails=12", "--", "hostname"],
        expect_fail="Parameter --max-fails is not allowed when submitting to an open job.",
    )


def test_job_wait_for_open_job_without_close(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "open"])

    hq_env.command(["job", "wait", "1", "--without-close"])

    hq_env.start_worker()
    hq_env.command(["submit", "--job=1", "--", "sleep", "0"])

    hq_env.command(["job", "wait", "1", "--without-close"])


def test_invalid_attach(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--", "sleep", "0"])
    hq_env.command(["job", "open"])
    hq_env.command(["job", "open"])
    hq_env.command(["job", "close", "3"])
    hq_env.command(["submit", "--job=123", "--", "sleep", "0"], expect_fail="Job 123 not found")
    hq_env.command(["submit", "--job=1", "--", "sleep", "0"], expect_fail="Job 1 is not open")
    hq_env.command(["submit", "--job=3", "--", "sleep", "0"], expect_fail="Job 3 is not open")
    hq_env.command(["submit", "--job=2", "--", "sleep", "0"])
    hq_env.command(["submit", "--job=2", "--array=0-2", "--", "sleep", "0"], expect_fail="Task 0 already exists in job")


def test_attach_to_open_job_array(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "open"])
    hq_env.command(["submit", "--job=1", "--array=10-20", "--", "bash", "-c", "echo 'test' > task.${HQ_TASK_ID}"])
    hq_env.command(["submit", "--job=1", "--array=1,3,5", "--", "bash", "-c", "echo 'test' > task.${HQ_TASK_ID}"])
    hq_env.command(["submit", "--job=1", "--array=2,4,6", "--", "bash", "-c", "echo 'test' > task.${HQ_TASK_ID}"])
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Tasks", "17; Ids: 1-6,10-20")
    hq_env.start_worker()
    hq_env.command(["job", "close", "1"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    ids = set(list(range(1, 7)) + list(range(10, 21)))
    for task_id in range(25):
        filename = f"task.{task_id}"
        assert os.path.isfile(filename) == (task_id in ids)


def test_job_list_keep_open_jobs(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "open"])
    hq_env.command(["job", "submit", "--job=1", "--", "hostname"])
    hq_env.command(["job", "cancel", "1"])
    table = hq_env.command(["job", "list"], as_table=True)
    assert len(table) == 1


def test_attach_to_open_job_consecutive(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "open"])
    for _ in range(4):
        hq_env.command(["submit", "--job=1", "--", "bash", "-c", "echo 'test' > task.${HQ_TASK_ID}"])
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Tasks", "4; Ids: 0-3")
    assert table.get_row_value("State").endswith("WAITING (4)")
    table = hq_env.command(["task", "list", "1"], as_table=True)
    assert table.get_column_value("State") == ["WAITING"] * 4

    hq_env.start_worker()
    wait_for_job_state(hq_env, 1, "OPENED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Tasks", "4; Ids: 0-3")
    assert table.get_row_value("State").endswith("FINISHED (4)")

    table = hq_env.command(["task", "list", "1"], as_table=True)
    assert table.get_column_value("State") == ["FINISHED"] * 4

    for task_id in range(4):
        filename = f"task.{task_id}"
        assert os.path.isfile(filename)
        os.unlink(filename)

    for _ in range(2):
        hq_env.command(["submit", "--job=1", "--", "bash", "-c", "echo 'test' > task.${HQ_TASK_ID}"])
    wait_for_job_state(hq_env, 1, "OPENED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Tasks", "6; Ids: 0-5")
    assert table.get_row_value("State").endswith("FINISHED (6)")

    table = hq_env.command(["task", "list", "1"], as_table=True)
    assert table.get_column_value("State") == ["FINISHED"] * 6

    for task_id in range(4):
        filename = f"task.{task_id}"
        assert not os.path.isfile(filename)

    for task_id in range(4, 6):
        filename = f"task.{task_id}"
        assert os.path.isfile(filename)

    hq_env.command(["job", "close", "1"])
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_close_job_before_worker(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "open"])
    for _ in range(4):
        hq_env.command(["submit", "--job=1", "--", "sleep", "0"])
    hq_env.command(["job", "close", "1"])
    wait_for_job_state(hq_env, 1, "WAITING")
    hq_env.start_worker()
    wait_for_job_state(hq_env, 1, "FINISHED")

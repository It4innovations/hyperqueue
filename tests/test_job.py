import os
import time
from datetime import datetime
from os.path import isdir, isfile, join
from pathlib import Path

import pytest

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.io import check_file_contents
from .utils.job import default_task_output


def test_job_submit(hq_env: HqEnv):
    hq_env.start_server()
    table = hq_env.command(["job", "list"], as_table=True)
    assert len(table) == 0
    assert table.header[:3] == ["ID", "Name", "State"]

    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello2'"])

    wait_for_job_state(hq_env, [1, 2], "WAITING")

    table = hq_env.command(["job", "list"], as_table=True)
    assert len(table) == 2
    table.check_columns_value(["ID", "Name", "State"], 0, ["1", "bash", "WAITING"])
    table.check_columns_value(["ID", "Name", "State"], 1, ["2", "bash", "WAITING"])

    hq_env.start_worker(cpus=1)

    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    table = hq_env.command(["job", "list"], as_table=True)
    assert len(table) == 2
    table.check_columns_value(["ID", "Name", "State"], 0, ["1", "bash", "FINISHED"])
    table.check_columns_value(["ID", "Name", "State"], 1, ["2", "bash", "FINISHED"])

    hq_env.command(["submit", "--", "sleep", "1"])

    wait_for_job_state(hq_env, 3, "RUNNING", sleep_s=0.2)

    table = hq_env.command(["job", "list"], as_table=True)
    assert len(table) == 3
    table.check_columns_value(["ID", "Name", "State"], 0, ["1", "bash", "FINISHED"])
    table.check_columns_value(["ID", "Name", "State"], 1, ["2", "bash", "FINISHED"])
    table.check_columns_value(["ID", "Name", "State"], 2, ["3", "sleep", "RUNNING"])

    wait_for_job_state(hq_env, 3, "FINISHED")

    table = hq_env.command(["job", "list"], as_table=True)
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


def test_custom_name(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(["submit", "--name=sleep_prog", "sleep", "1"])
    wait_for_job_state(hq_env, 1, "WAITING")

    table = hq_env.command(["job", "list"], as_table=True)
    assert len(table) == 1
    table.check_columns_value(
        ["ID", "Name", "State"], 0, ["1", "sleep_prog", "WAITING"]
    )

    with pytest.raises(Exception):
        hq_env.command(["submit", "--name=second_sleep \n", "sleep", "1"])
    with pytest.raises(Exception):
        hq_env.command(["submit", "--name=second_sleep \t", "sleep", "1"])
    with pytest.raises(Exception):
        hq_env.command(
            [
                "submit",
                "--name=sleep_sleep_sleep_sleep_sleep_sleep_sleep_sleep",
                "sleep",
                "1",
            ]
        )

    table = hq_env.command(["job", "list"], as_table=True)
    assert len(table) == 1


def test_custom_working_dir(hq_env: HqEnv, tmpdir):
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

    check_file_contents(default_task_output(submit_dir=submit_dir), test_string)


def test_job_output_default(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    hq_env.command(["submit", "--", "ls", "/non-existent"])
    hq_env.command(["submit", "--", "/non-existent-program"])

    wait_for_job_state(hq_env, [1, 2, 3], ["FINISHED", "FAILED"])

    check_file_contents(
        os.path.join(tmp_path, default_task_output(job_id=1, type="stdout")), "hello\n"
    )
    check_file_contents(
        os.path.join(tmp_path, default_task_output(job_id=1, type="stderr")), ""
    )
    check_file_contents(
        os.path.join(tmp_path, default_task_output(job_id=2, type="stdout")), ""
    )

    with open(
        os.path.join(tmp_path, default_task_output(job_id=2, type="stderr"))
    ) as f:
        data = f.read()
        assert "No such file or directory" in data
        assert data.startswith("ls:")

    check_file_contents(
        os.path.join(tmp_path, default_task_output(job_id=3, type="stdout")), ""
    )
    check_file_contents(
        os.path.join(tmp_path, default_task_output(job_id=3, type="stderr")), ""
    )


def test_create_output_folders(hq_env: HqEnv):
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
    hq_env.command(
        ["submit", "--stdout=abc", "--stderr=xyz", "--", "bash", "-c", "echo 'hello'"]
    )
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
    hq_env.command(
        ["submit", "--stdout=none", "--stderr=none", "--", "bash", "-c", "echo 'hello'"]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    assert not os.path.exists(os.path.join(tmp_path, "none"))
    assert not os.path.exists(
        os.path.join(tmp_path, default_task_output(job_id=1, task_id=0, type="stdout"))
    )
    assert not os.path.exists(
        os.path.join(tmp_path, default_task_output(job_id=1, task_id=0, type="stderr"))
    )


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

    table = hq_env.command(["job", "list"], as_table=True)
    table.check_column_value("State", 0, "CANCELED")
    table.check_column_value("State", 1, "WAITING")
    table.check_column_value("State", 2, "WAITING")
    assert len(table) == 3

    table_canceled = hq_env.command(["job", "list", "canceled"], as_table=True)
    assert len(table_canceled) == 1

    table_waiting = hq_env.command(["job", "list", "waiting"], as_table=True)
    assert len(table_waiting) == 2

    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "sleep", "1"])

    wait_for_job_state(hq_env, 4, "RUNNING")

    table_running = hq_env.command(["job", "list", "running"], as_table=True)
    assert len(table_running) == 1

    table_finished = hq_env.command(["job", "list", "finished"], as_table=True)
    assert len(table_finished) == 1

    table_failed = hq_env.command(["job", "list", "failed"], as_table=True)
    assert len(table_failed) == 1


def test_job_fail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "/non-existent-program"])
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["job", "list"], as_table=True)
    assert len(table) == 1
    table.check_column_value("ID", 0, "1")
    table.check_column_value("Name", 0, "non-existent-program")
    table.check_column_value("State", 0, "FAILED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("ID", "1")
    table.check_row_value("State", "FAILED")

    table = hq_env.command(["job", "tasks", "1"], as_table=True)
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
    table = hq_env.command(["job", "list"], as_table=True)
    table.check_column_value("State", 0, "CANCELED")
    hq_env.start_worker(cpus=1)
    table = hq_env.command(["job", "list"], as_table=True)
    table.check_column_value("State", 0, "CANCELED")


def test_cancel_running(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "sleep", "10"])

    wait_for_job_state(hq_env, 1, "RUNNING")

    table = hq_env.command(["job", "list"], as_table=True)
    table.check_column_value("State", 0, "RUNNING")
    r = hq_env.command(["job", "cancel", "1"])
    assert "Job 1 canceled" in r
    table = hq_env.command(["job", "list"], as_table=True)
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

    table = hq_env.command(["job", "list"], as_table=True)
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

    table = hq_env.command(["job", "list"], as_table=True)
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


def test_reporting_state_after_worker_lost(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(2, cpus=1)
    hq_env.command(["submit", "sleep", "1"])
    hq_env.command(["submit", "sleep", "1"])

    wait_for_job_state(hq_env, [1, 2], "RUNNING")

    table = hq_env.command(["job", "list"], as_table=True)
    table.check_column_value("State", 0, "RUNNING")
    table.check_column_value("State", 1, "RUNNING")
    hq_env.kill_worker(1)

    time.sleep(0.25)

    table = hq_env.command(["job", "list"], as_table=True)
    if table.get_column_value("State")[0] == "WAITING":
        idx, other = 0, 1
    elif table.get_column_value("State")[1] == "WAITING":
        idx, other = 1, 0
    else:
        assert 0
    assert table.get_column_value("State")[other] == "RUNNING"

    wait_for_job_state(hq_env, other + 1, "FINISHED")

    table = hq_env.command(["job", "list"], as_table=True)
    assert table.get_column_value("State")[other] == "FINISHED"
    assert table.get_column_value("State")[idx] == "RUNNING"

    wait_for_job_state(hq_env, idx + 1, "FINISHED")

    table = hq_env.command(["job", "list"], as_table=True)
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

    check_file_contents(
        os.path.join(hq_env.work_path, default_task_output()), "BAR BAR2\n"
    )

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

    wait_for_job_state(hq_env, 1, "CANCELED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    states = table.get_row_value("State").split("\n")
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
    states = table.get_row_value("State").split("\n")
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
    wait_for_job_state(hq_env, 1, "CANCELED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    states = table.get_row_value("State").split("\n")
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


def test_job_resubmit_with_status(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--array=3-9",
            "--",
            "python3",
            "-c",
            "import os; assert os.environ['HQ_TASK_ID'] not in ['4', '5', '6', '8']",
        ]
    )
    hq_env.start_workers(2, cpus=1)
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["job", "resubmit", "1", "--status=failed"], as_table=True)
    table.check_row_value("Tasks", "4; Ids: 4-6, 8")

    table = hq_env.command(["job", "resubmit", "1", "--status=finished"], as_table=True)
    table.check_row_value("Tasks", "3; Ids: 3, 7, 9")


def test_job_resubmit_all(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--array=2,7,9", "--", "/bin/hostname"])
    hq_env.start_workers(2, cpus=1)
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["job", "resubmit", "1"], as_table=True)
    table.check_row_value("Tasks", "3; Ids: 2, 7, 9")


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
    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    wait_for_job_state(hq_env, 1, "WAITING")
    table.check_column_value("Worker", 0, "")

    hq_env.start_worker()

    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    wait_for_job_state(hq_env, 1, "FINISHED")
    table.check_column_value("Worker", 0, "worker1")

    hq_env.command(["submit", "non-existent-program", "test"])
    table = hq_env.command(["job", "tasks", "2"], as_table=True)
    wait_for_job_state(hq_env, 2, "FAILED")
    worker = table.get_column_value("Worker")[0]
    assert worker == "" or worker == "worker1"


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
    time.sleep(
        1.2
    )  # This sleep is not redundant, we check that after finished time is not moving

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Makespan").startswith("1s")

    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    table.get_column_value("Time")[0].startswith("1s")


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
    table.check_row_value("Task time limit", "500ms")
    assert table.get_row_value("Makespan").startswith("5")
    assert table.get_row_value("Makespan").endswith("ms")

    table = hq_env.command(["job", "tasks", "1"], as_table=True)
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

    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    assert (
        'Error: Cannot execute "foo --bar --baz=5": No such file or directory (os error 2)\n'
        "The program that you have tried to execute (`foo`) was not found."
        == table.get_column_value("Error")[0]
    )


def test_job_submit_program_not_found_file_exists(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    with open("foo", "w") as f:
        f.write("hostname")

    hq_env.command(["submit", "foo"])
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    assert (
        f"""Error: Cannot execute "foo": No such file or directory (os error 2)
The program that you have tried to execute (`foo`) was not found.
The file "{join(os.getcwd(), 'foo')}" exists, maybe you have meant `./foo` instead?"""
        == table.get_column_value("Error")[0]
    )


def test_hq_directives_from_file(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")

    content = """#! /bin/bash
# Hello!
#HQ --name abc --array=1-10
#HQ --cpus="2 compact"

./do-something

#HQ --this-should-be-ignored
"""

    (tmp_path / "test.sh").write_text(content)
    (tmp_path / "test").write_text(content)
    (tmp_path / "input").write_text("line\n" * 5)

    hq_env.command(["submit", "test.sh"])
    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Name") == "abc"
    assert table.get_row_value("Resources") == "cpus: 2 compact"
    assert table.get_row_value("Tasks") == "10; Ids: 1-10"

    hq_env.command(["submit", "--name=xyz", "test.sh"])
    table = hq_env.command(["job", "info", "2"], as_table=True)
    assert table.get_row_value("Name") == "xyz"
    assert table.get_row_value("Resources") == "cpus: 2 compact"
    assert table.get_row_value("Tasks") == "10; Ids: 1-10"

    hq_env.command(["submit", "--name=xyz", "--each-line", "input", "test.sh"])
    table = hq_env.command(["job", "info", "3"], as_table=True)
    assert table.get_row_value("Name") == "xyz"
    assert table.get_row_value("Resources") == "cpus: 2 compact"
    assert table.get_row_value("Tasks") == "5; Ids: 0-4"

    hq_env.command(["submit", "test"])
    table = hq_env.command(["job", "info", "4"], as_table=True)
    assert table.get_row_value("Name") == "test"

    hq_env.command(["submit", "--directives", "test"])
    table = hq_env.command(["job", "info", "5"], as_table=True)
    assert table.get_row_value("Name") == "abc"


def test_job_shell_script_fail_without_interpreter(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")
    Path("test.sh").write_text("""echo 'Hello' > out.txt""")
    hq_env.command(["submit", "test.sh"])
    wait_for_job_state(hq_env, 1, "FAILED")


def test_job_shell_script_read_interpreter(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")
    Path("test.sh").write_text(
        """#!/bin/bash
echo 'Hello' > out.txt
"""
    )
    hq_env.command(["submit", "test.sh"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    check_file_contents("out.txt", "Hello\n")

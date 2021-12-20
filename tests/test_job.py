import os
import time
from datetime import datetime
from os.path import isdir, isfile

import pytest

from .conftest import HqEnv
from .utils import JOB_TABLE_ROWS, wait_for_job_state
from .utils.job import default_task_output


def test_job_submit(hq_env: HqEnv):
    hq_env.start_server()
    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 1
    assert table[0][:3] == ["Id", "Name", "State"]

    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello2'"])

    wait_for_job_state(hq_env, [1, 2], "WAITING")

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 3
    table.check_columns_value(["Id", "Name", "State"], 0, ["1", "bash", "WAITING"])
    table.check_columns_value(["Id", "Name", "State"], 1, ["2", "bash", "WAITING"])

    hq_env.start_worker(cpus=1)

    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 3
    table.check_columns_value(["Id", "Name", "State"], 0, ["1", "bash", "FINISHED"])
    table.check_columns_value(["Id", "Name", "State"], 1, ["2", "bash", "FINISHED"])

    hq_env.command(["submit", "--", "sleep", "1"])

    wait_for_job_state(hq_env, 3, "RUNNING", sleep_s=0.2)

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 4
    table.check_columns_value(["Id", "Name", "State"], 0, ["1", "bash", "FINISHED"])
    table.check_columns_value(["Id", "Name", "State"], 1, ["2", "bash", "FINISHED"])
    table.check_columns_value(["Id", "Name", "State"], 2, ["3", "sleep", "RUNNING"])

    wait_for_job_state(hq_env, 3, "FINISHED")

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 4
    table.check_columns_value(["Id", "Name", "State"], 0, ["1", "bash", "FINISHED"])
    table.check_columns_value(["Id", "Name", "State"], 1, ["2", "bash", "FINISHED"])
    table.check_columns_value(["Id", "Name", "State"], 2, ["3", "sleep", "FINISHED"])


def test_job_submit_output(hq_env: HqEnv):
    hq_env.start_server()

    output = hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    assert output.strip() == "Job submitted successfully, job ID: 1"


def test_custom_name(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(["submit", "sleep", "1", "--name=sleep_prog"])
    wait_for_job_state(hq_env, 1, "WAITING")

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 2
    table.check_columns_value(
        ["Id", "Name", "State"], 0, ["1", "sleep_prog", "WAITING"]
    )

    with pytest.raises(Exception):
        hq_env.command(["submit", "sleep", "1", "--name=second_sleep \n"])
    with pytest.raises(Exception):
        hq_env.command(["submit", "sleep", "1", "--name=second_sleep \t"])
    with pytest.raises(Exception):
        hq_env.command(
            [
                "submit",
                "sleep",
                "1",
                "--name=sleep_sleep_sleep_sleep_sleep_sleep_sleep_sleep",
            ]
        )

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 2


def test_custom_working_dir(hq_env: HqEnv, tmpdir):
    hq_env.start_server()

    test_string = "cwd_test_string"
    test_path = tmpdir.mkdir("test_dir")
    test_file = test_path.join("testfile")
    test_file.write(test_string)

    submit_dir = tmpdir.mkdir("submit_dir")

    hq_env.command(
        ["submit", "--cwd=" + str(test_path), "--", "bash", "-c", "cat testfile"],
        cwd=submit_dir,
    )
    table = hq_env.command(["job", "1"], as_table=True)
    table.check_row_value("Working Dir", str(test_path))

    hq_env.start_worker(cpus=1)
    wait_for_job_state(hq_env, 1, ["FINISHED"])

    with open(os.path.join(tmpdir, "submit_dir", default_task_output())) as f:
        assert f.read() == test_string


def test_job_output_default(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    hq_env.command(["submit", "--", "ls", "/non-existent"])
    hq_env.command(["submit", "--", "/non-existent-program"])

    wait_for_job_state(hq_env, [1, 2, 3], ["FINISHED", "FAILED"])

    with open(
        os.path.join(tmp_path, default_task_output(job_id=1, type="stdout"))
    ) as f:
        assert f.read() == "hello\n"
    with open(
        os.path.join(tmp_path, default_task_output(job_id=1, type="stderr"))
    ) as f:
        assert f.read() == ""

    with open(
        os.path.join(tmp_path, default_task_output(job_id=2, type="stdout"))
    ) as f:
        assert f.read() == ""
    with open(
        os.path.join(tmp_path, default_task_output(job_id=2, type="stderr"))
    ) as f:
        data = f.read()
        assert "No such file or directory" in data
        assert data.startswith("ls:")

    with open(
        os.path.join(tmp_path, default_task_output(job_id=3, type="stdout"))
    ) as f:
        assert f.read() == ""
    with open(
        os.path.join(tmp_path, default_task_output(job_id=3, type="stderr"))
    ) as f:
        assert f.read() == ""


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

    with open(os.path.join(tmp_path, "abc")) as f:
        assert f.read() == "hello\n"
    with open(os.path.join(tmp_path, "xyz")) as f:
        assert f.read() == ""


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

    table_empty = hq_env.command(["jobs"], as_table=True)
    assert len(table_empty) == 1

    hq_env.command(["submit", "--", "bash", "-c", "echo 'to cancel'"])
    hq_env.command(["submit", "--", "bash", "-c", "echo 'bye'"])
    hq_env.command(["submit", "--", "ls", "failed"])

    wait_for_job_state(hq_env, [1, 2, 3], "WAITING")

    r = hq_env.command(["cancel", "1"])
    assert "Job 1 canceled" in r

    table = hq_env.command(["jobs"], as_table=True)
    table.check_column_value("State", 0, "CANCELED")
    table.check_column_value("State", 1, "WAITING")
    table.check_column_value("State", 2, "WAITING")
    assert len(table) == 4

    table_canceled = hq_env.command(["jobs", "canceled"], as_table=True)
    assert len(table_canceled) == 2

    table_waiting = hq_env.command(["jobs", "waiting"], as_table=True)
    assert len(table_waiting) == 3

    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "sleep", "1"])

    wait_for_job_state(hq_env, 4, "RUNNING")

    table_running = hq_env.command(["jobs", "running"], as_table=True)
    assert len(table_running) == 2

    table_finished = hq_env.command(["jobs", "finished"], as_table=True)
    assert len(table_finished) == 2

    table_failed = hq_env.command(["jobs", "failed"], as_table=True)
    assert len(table_failed) == 2


def test_job_fail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "--", "/non-existent-program"])
    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 2
    table.check_column_value("Id", 0, "1")
    table.check_column_value("Name", 0, "non-existent-program")
    table.check_column_value("State", 0, "FAILED")

    table = hq_env.command(["job", "1", "--tasks"], as_table=True)
    table.check_row_value("Id", "1")
    table.check_row_value("State", "FAILED")

    table = table[JOB_TABLE_ROWS:]
    table.check_column_value("Task Id", 0, "0")
    assert "No such file or directory" in table.get_column_value("Message")[0]


def test_job_invalid(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    result = hq_env.command(["job", "5"])
    assert "Job 5 not found" in result


def test_cancel_without_workers(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "/bin/hostname"])
    r = hq_env.command(["cancel", "1"])
    assert "Job 1 canceled" in r
    table = hq_env.command(["jobs"], as_table=True)
    table.check_column_value("State", 0, "CANCELED")
    hq_env.start_worker(cpus=1)
    table = hq_env.command(["jobs"], as_table=True)
    table.check_column_value("State", 0, "CANCELED")


def test_cancel_running(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "sleep", "10"])

    wait_for_job_state(hq_env, 1, "RUNNING")

    table = hq_env.command(["jobs"], as_table=True)
    table.check_column_value("State", 0, "RUNNING")
    r = hq_env.command(["cancel", "1"])
    assert "Job 1 canceled" in r
    table = hq_env.command(["jobs"], as_table=True)
    table.check_column_value("State", 0, "CANCELED")

    r = hq_env.command(["cancel", "1"])
    assert "Canceling job 1 failed" in r


def test_cancel_finished(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])

    wait_for_job_state(hq_env, [1, 2], ["FINISHED", "FAILED"])

    r = hq_env.command(["cancel", "1"])
    assert "Canceling job 1 failed" in r
    r = hq_env.command(["cancel", "2"])
    assert "Canceling job 2 failed" in r

    table = hq_env.command(["jobs"], as_table=True)
    table.check_column_value("State", 0, "FINISHED")
    table.check_column_value("State", 1, "FAILED")


def test_cancel_last(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])

    wait_for_job_state(hq_env, [1, 2], ["FINISHED", "FAILED"])

    r = hq_env.command(["cancel", "last"])
    assert "Canceling job 2 failed" in r


def test_cancel_some(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "sleep", "100"])
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])

    r = hq_env.command(["cancel", "1-4"])
    assert "Canceling job 4 failed" in r

    table = hq_env.command(["jobs"], as_table=True)
    for i in range(3):
        table.check_column_value("State", i, "CANCELED")


def test_cancel_all(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])

    wait_for_job_state(hq_env, [1, 2], ["FINISHED", "FAILED"])

    hq_env.command(["submit", "sleep", "100"])

    r = hq_env.command(["cancel", "all"]).splitlines()
    assert len(r) == 1
    assert "Job 3 canceled" in r[0]


def test_reporting_state_after_worker_lost(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(2, cpus=1)
    hq_env.command(["submit", "sleep", "1"])
    hq_env.command(["submit", "sleep", "1"])

    wait_for_job_state(hq_env, [1, 2], "RUNNING")

    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "RUNNING"
    assert table[2][2] == "RUNNING"
    hq_env.kill_worker(1)

    time.sleep(0.25)

    table = hq_env.command(["jobs"], as_table=True)
    if table[1][2] == "WAITING":
        idx, other = 1, 2
    elif table[2][2] == "WAITING":
        idx, other = 2, 1
    else:
        assert 0
    assert table[other][2] == "RUNNING"

    wait_for_job_state(hq_env, other, "FINISHED")

    table = hq_env.command(["jobs"], as_table=True)
    assert table[other][2] == "FINISHED"
    assert table[idx][2] == "RUNNING"

    wait_for_job_state(hq_env, idx, "FINISHED")

    table = hq_env.command(["jobs"], as_table=True)
    assert table[other][2] == "FINISHED"
    assert table[idx][2] == "FINISHED"


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

    with open(os.path.join(hq_env.work_path, default_task_output())) as f:
        assert f.read().strip() == "BAR BAR2"

    table = hq_env.command(["job", "1"], as_table=True)
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

    table = hq_env.command(["job", "1"], as_table=True)
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

    table = hq_env.command(["job", "1"], as_table=True)
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

    table = hq_env.command(["job", "1"], as_table=True)
    states = table.get_row_value("State").split("\n")
    assert "FAILED (4)" in states
    assert "CANCELED (6)" in states


def test_job_last(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["job", "last"])

    hq_env.command(["submit", "ls"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["job", "last"], as_table=True)
    table.check_row_value("Id", "1")

    hq_env.command(["submit", "ls"])
    wait_for_job_state(hq_env, 2, "FINISHED")

    table = hq_env.command(["job", "last"], as_table=True)
    table.check_row_value("Id", "2")


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

    table = hq_env.command(["resubmit", "1", "--status=failed"], as_table=True)
    table.check_row_value("Tasks", "4; Ids: 4-6, 8")

    table = hq_env.command(["resubmit", "1", "--status=finished"], as_table=True)
    table.check_row_value("Tasks", "3; Ids: 3, 7, 9")


def test_job_resubmit_all(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--array=2,7,9", "--", "/bin/hostname"])
    hq_env.start_workers(2, cpus=1)
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["resubmit", "1"], as_table=True)
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
    table = hq_env.command(["job", "1", "--tasks"], as_table=True)[JOB_TABLE_ROWS:]
    wait_for_job_state(hq_env, 1, "WAITING")
    table.check_column_value("Worker", 0, "")

    hq_env.start_worker()

    table = hq_env.command(["job", "1", "--tasks"], as_table=True)[JOB_TABLE_ROWS:]
    wait_for_job_state(hq_env, 1, "FINISHED")
    table.check_column_value("Worker", 0, "worker1")

    hq_env.command(["submit", "non-existent-program", "test"])
    table = hq_env.command(["job", "2", "--tasks"], as_table=True)[JOB_TABLE_ROWS:]
    wait_for_job_state(hq_env, 2, "FAILED")
    worker = table.get_column_value("Worker")[0]
    assert worker == "" or worker == "worker1"


def test_job_wait(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "sleep", "1"])
    r = hq_env.command(["wait", "1"])
    assert "1 job finished" in r

    table = hq_env.command(["job", "1"], as_table=True)
    table.check_row_value("State", "FINISHED")

    r = hq_env.command(["wait", "all"])
    assert "1 job finished" in r


def test_job_submit_wait(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    r = hq_env.command(["submit", "sleep", "1", "--wait"])
    assert "1 job finished" in r

    table = hq_env.command(["job", "1"], as_table=True)
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

    hq_env.command(["cancel", "last"])

    assert process.wait() == 1


def test_job_completion_time(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "sleep", "1"])
    wait_for_job_state(hq_env, 1, "RUNNING")

    table = hq_env.command(["job", "1"], as_table=True)
    assert table.get_row_value("Makespan") != "0s"
    assert not table.get_row_value("Makespan").startswith("1s")

    wait_for_job_state(hq_env, 1, "FINISHED")
    time.sleep(
        1.2
    )  # This sleep is not redundant, we check that after finished time is not moving

    table = hq_env.command(["job", "1"], as_table=True)
    assert table.get_row_value("Makespan").startswith("1s")

    table = hq_env.command(["job", "1", "--tasks"], as_table=True)

    offset = JOB_TABLE_ROWS
    assert table[offset + 1][3].startswith("1s")


def test_job_timeout(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="3")

    hq_env.command(["submit", "--time-limit=500ms", "sleep", "2"])
    hq_env.command(["submit", "--time-limit=3s", "sleep", "2"])
    hq_env.command(["submit", "sleep", "2"])

    table = hq_env.command(["job", "1"], as_table=True)
    table.check_row_value("Task time limit", "500ms")

    table = hq_env.command(["job", "2"], as_table=True)
    table.check_row_value("Task time limit", "3s")

    table = hq_env.command(["job", "3"], as_table=True)
    table.check_row_value("Task time limit", "None")

    wait_for_job_state(hq_env, 1, "FAILED")
    table = hq_env.command(["job", "1"], as_table=True)
    table.check_row_value("Task time limit", "500ms")
    offset = JOB_TABLE_ROWS
    assert table[offset + 1][2] == "Time limit reached"
    assert table.get_row_value("Makespan").startswith("5")
    assert table.get_row_value("Makespan").endswith("ms")

    wait_for_job_state(hq_env, 2, "FINISHED")
    table = hq_env.command(["job", "2"], as_table=True)
    assert table.get_row_value("Makespan").startswith("2")

    wait_for_job_state(hq_env, 3, "FINISHED")
    table = hq_env.command(["job", "3"], as_table=True)
    assert table.get_row_value("Makespan").startswith("2")

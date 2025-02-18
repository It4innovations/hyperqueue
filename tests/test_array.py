import collections
import datetime
import os
import time

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.io import check_file_contents
from .utils.job import default_task_output, list_jobs


def test_job_array_submit(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4)
    hq_env.command(["submit", "--array=30-36", "--", "bash", "-c", "echo $HQ_JOB_ID-$HQ_TASK_ID"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i in list(range(0, 30)) + list(range(37, 40)):
        assert not os.path.isfile(os.path.join(hq_env.work_path, default_task_output(job_id=1, task_id=i)))
        assert not os.path.isfile(
            os.path.join(
                hq_env.work_path,
                default_task_output(job_id=1, task_id=i, type="stderr"),
            )
        )

    for i in range(36, 37):
        stdout = os.path.join(hq_env.work_path, default_task_output(job_id=1, task_id=i))
        assert os.path.isfile(stdout)
        assert os.path.isfile(
            os.path.join(
                hq_env.work_path,
                default_task_output(job_id=1, task_id=i, type="stderr"),
            )
        )
        check_file_contents(stdout, f"1-{i}\n")


def test_job_array_report(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4)
    hq_env.command(["submit", "--array=10-19", "--", "sleep", "1"])
    time.sleep(1.6)
    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "RUNNING")

    table = hq_env.command(["job", "info", "1"])
    print(table)

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Tasks", "10; Ids: 10-19")

    states = table.get_row_value("State").split("\n")
    assert "RUNNING (4)" in states
    assert "FINISHED (4)" in states
    assert "WAITING (2)" in states

    time.sleep(1.6)

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("State").split("\n")[-1] == "FINISHED (10)"


def test_job_array_error_some(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--array=0-9",
            "--",
            "python3",
            "-c",
            "import os; assert os.environ['HQ_TASK_ID'] not in ['2', '3', '7']",
        ]
    )
    hq_env.start_worker(cpus=2)

    wait_for_job_state(hq_env, 1, "FAILED")

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "FAILED")

    table = hq_env.command(["job", "info", "1"], as_table=True)

    states = table[0].get_row_value("State").split("\n")
    assert "FAILED (3)" in states
    assert "FINISHED (7)" in states

    table = table[1]
    assert table.header == ["Task ID", "Worker", "Error"]

    assert table.get_column_value("Task ID")[0] == "2"
    assert table.get_column_value("Error")[0] == "Error: Program terminated with exit code 1"

    assert table.get_column_value("Task ID")[1] == "3"
    assert table.get_column_value("Error")[1] == "Error: Program terminated with exit code 1"

    assert table.get_column_value("Task ID")[2] == "7"
    assert table.get_column_value("Error")[2] == "Error: Program terminated with exit code 1"

    table = hq_env.command(["task", "list", "1"], as_table=True)
    for i, state in enumerate(
        [
            "FINISHED",
            "FINISHED",
            "FAILED",
            "FAILED",
            "FINISHED",
            "FINISHED",
            "FINISHED",
            "FAILED",
        ]
        + 2 * ["FINISHED"]
    ):
        table.check_column_value("State", i, state)


def test_job_array_error_all(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--array=0-9", "--", "/non-existent"])
    hq_env.start_worker(cpus=2)

    wait_for_job_state(hq_env, 1, "FAILED")

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "FAILED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    states = table[0].get_row_value("State").split("\n")
    assert "FAILED (10)" in states

    errors = table[1].as_horizontal().get_column_value("Error")
    for error in errors:
        assert "No such file or directory" in error

    table = hq_env.command(["job", "info", "1"], as_table=True)
    states = table[0].get_row_value("State").split("\n")
    assert "FAILED (10)" in states

    task_table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    for i in range(10):
        assert task_table.get_column_value("Task ID")[i] == str(i)
        assert task_table.get_column_value("State")[i] == "FAILED"
        assert "No such file or directory" in task_table.get_column_value("Error")[i]


def test_job_array_cancel(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4)
    hq_env.command(["submit", "--array=0-9", "--", "sleep", "1"])
    time.sleep(1.6)
    hq_env.command(["job", "cancel", "1"])
    time.sleep(0.4)

    table = hq_env.command(["job", "info", "1"], as_table=True)
    states = table.get_row_value("State").split("\n")
    assert "FINISHED (4)" in states
    assert "CANCELED (6)" in states

    table = hq_env.command(["task", "list", "1"], as_table=True)
    task_states = table.get_column_value("State")
    c = collections.Counter(task_states)
    assert c.get("FINISHED") == 4
    assert c.get("CANCELED") == 6

    table = list_jobs(hq_env)
    table.check_column_value("State", 0, "CANCELED")


def test_array_reporting_state_after_worker_lost(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(1, cpus=2)
    hq_env.command(["submit", "--array=1-4", "sleep", "1"])
    time.sleep(0.25)
    hq_env.kill_worker(1)
    time.sleep(0.25)
    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert "WAITING (4)" in table.get_row_value("State").split("\n")

    table = hq_env.command(["task", "list", "1"], as_table=True)
    task_states = table.get_column_value("State")
    c = collections.Counter(task_states)
    assert c.get("WAITING") == 4
    hq_env.start_workers(1, cpus=2)

    time.sleep(2.2)
    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert "FINISHED (4)" in table.get_row_value("State").split("\n")

    table = hq_env.command(["task", "list", "1"], as_table=True)
    task_states = table.get_column_value("State")
    c = collections.Counter(task_states)
    assert c.get("FINISHED") == 4


def test_array_mix_with_simple_jobs(hq_env: HqEnv):
    hq_env.start_server()
    for i in range(100):
        hq_env.command(["submit", "--array=1-4", "uname"])
        hq_env.command(["submit", "uname"])
    hq_env.start_workers(1, cpus=2)

    wait_for_job_state(hq_env, list(range(1, 101)), "FINISHED")

    table = list_jobs(hq_env)
    for i in range(100):
        assert table.get_column_value("ID")[i] == str(i + 1)
        assert table.get_column_value("State")[i] == "FINISHED"
        assert table.get_column_value("Tasks")[i] == "4" if i % 2 == 0 else "1"


def test_array_times(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=1-3", "sleep", "1"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    time.sleep(1.2)  # This sleep is not redundant, we check that after finished time is not moving

    for i in range(1, 4):
        table = hq_env.command(["task", "info", "1", str(i)], as_table=True)
        start = datetime.datetime.strptime(table.get_row_value("Start"), "%d.%m.%Y %H:%M:%S")
        end = datetime.datetime.strptime(table.get_row_value("End"), "%d.%m.%Y %H:%M:%S")
        seconds = (end - start).total_seconds()
        assert 0.9 <= seconds <= 1.1

from .conftest import HqEnv
from .utils import wait_for_job_state


def test_task_list_single(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=0-3", "--", "bash", "-c", "uname"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    r = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert r.get_column_value("Job ID") is None
    assert r.get_column_value("Task ID") == ["0", "1", "2", "3"]
    assert r.get_column_value("State") == ["FINISHED"] * 4
    assert r.get_column_value("Error") == [""] * 4


def test_task_list_multi(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=5-10", "--", "bash", "-c", "uname"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    hq_env.command(["submit", "--array=0-3", "--", "bash", "-c", "uname"])
    wait_for_job_state(hq_env, 2, "FINISHED")

    r = hq_env.command(["task", "list", "1-2", "-v"], as_table=True)
    assert r.get_column_value("Job ID") == ["1"] * 6 + ["2"] * 4
    assert r.get_column_value("Task ID") == [
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "0",
        "1",
        "2",
        "3",
    ]
    assert r.get_column_value("State") == ["FINISHED"] * 10
    assert r.get_column_value("Error") == [""] * 10


def test_task_info(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=5-7", "--", "bash", "-c", "uname"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    r = hq_env.command(["task", "info", "1", "5"], as_table=True)
    assert r.get_row_value("Task ID") == "5"

    r = hq_env.command(["task", "info", "1", "5-6"], as_table=True)
    assert r[0].get_row_value("Task ID") == "5"
    assert r[1].get_row_value("Task ID") == "6"

    r = hq_env.command(["task", "info", "1", "4"])
    assert "WARN Task 4 not found" in r

    hq_env.command(["submit", "--", "bash", "-c", "uname"])
    r = hq_env.command(["task", "info", "last", "0"], as_table=True)
    assert r.get_row_value("Task ID") == "0"


def test_long_running_task(hq_env: HqEnv):
    """
    We had a very nasty bug (https://github.com/It4innovations/hyperqueue/issues/820) where
    tasks were getting killed when tokio periodically reaped blocking worker threads.
    This is not a perfect test against that, but at least we can check that tasks can live longer
    than ~15s...
    """
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "sleep", "20"])
    wait_for_job_state(hq_env, 1, "FINISHED", timeout_s=30)


def test_task_workdir_basic(hq_env: HqEnv):
    """Test basic task workdir functionality."""
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=1-3", "--", "echo", "test"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test single task workdir
    output = hq_env.command(["task", "workdir", "1", "2"])
    assert "Job 1:" in output
    assert "Task 2:" in output

    # Test multiple tasks workdir
    output = hq_env.command(["task", "workdir", "1", "1-3"])
    assert "Job 1:" in output
    assert "Task 1:" in output
    assert "Task 2:" in output
    assert "Task 3:" in output


def test_task_workdir_json_output(hq_env: HqEnv):
    """Test task workdir JSON output."""
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=1-2", "--", "echo", "test"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    output = hq_env.command(["task", "workdir", "1", "1-2", "--output-mode", "json"])
    import json

    data = json.loads(output)

    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["job_id"] == 1
    assert "tasks" in data[0]
    assert "1" in data[0]["tasks"]
    assert "2" in data[0]["tasks"]


def test_task_workdir_integration_with_task_info(hq_env: HqEnv):
    """Test that task workdir is consistent with task info."""
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=1-2", "--", "echo", "test"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Get workdir for task
    workdir_output = hq_env.command(["task", "workdir", "1", "1"])

    # Get task info
    info_output = hq_env.command(["task", "info", "1", "1"])

    # Both should reference working directories
    import os

    current_dir = os.getcwd()
    assert current_dir in workdir_output
    assert current_dir in info_output

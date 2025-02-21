from .conftest import HqEnv
from .utils import wait_for_job_state


def test_task_list_single(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=0-3", "--", "bash", "-c", "hostname"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    r = hq_env.command(["task", "list", "1", "-v"], as_table=True)
    assert r.get_column_value("Job ID") is None
    assert r.get_column_value("Task ID") == ["0", "1", "2", "3"]
    assert r.get_column_value("State") == ["FINISHED"] * 4
    assert r.get_column_value("Error") == [""] * 4


def test_task_list_multi(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=5-10", "--", "bash", "-c", "hostname"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    hq_env.command(["submit", "--array=0-3", "--", "bash", "-c", "hostname"])
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

    hq_env.command(["submit", "--array=5-7", "--", "bash", "-c", "hostname"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    r = hq_env.command(["task", "info", "1", "5"], as_table=True)
    assert r.get_row_value("Task ID") == "5"

    r = hq_env.command(["task", "info", "1", "5-6"], as_table=True)
    assert r[0].get_row_value("Task ID") == "5"
    assert r[1].get_row_value("Task ID") == "6"

    r = hq_env.command(["task", "info", "1", "4"])
    assert "WARN Task 4 not found" in r

    hq_env.command(["submit", "--", "bash", "-c", "hostname"])
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

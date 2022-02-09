from .conftest import HqEnv
from .utils import wait_for_job_state


def test_task_list_single(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array=0-3", "--", "bash", "-c", "hostname"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    r = hq_env.command(["task", "list", "1"], as_table=True)
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

    r = hq_env.command(["task", "list", "1-2"], as_table=True)
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

from ..conftest import HqEnv
from ..utils import wait_for_job_state


def test_print_worker_list(hq_env: HqEnv):
    hq_env.start_server()
    for i in range(9):
        hq_env.start_worker()
    output = hq_env.command(["--output-mode=quiet", "worker", "list"])
    output = output.splitlines(keepends=False)
    assert output == [f"{id + 1} RUNNING" for id in range(9)]


def test_print_job_list(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    for i in range(9):
        hq_env.command(["submit", "echo", "tt"])

    wait_for_job_state(hq_env, list(range(1, 10)), "FINISHED")

    output = hq_env.command(["--output-mode=quiet", "jobs"])
    output = output.splitlines(keepends=False)
    assert output == [f"{id + 1} FINISHED" for id in range(9)]


def test_submit(hq_env: HqEnv):
    hq_env.start_server()
    output = hq_env.command(["--output-mode=quiet", "submit", "echo", "tt"])
    assert output == "1\n"

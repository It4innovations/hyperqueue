from typing import List, Optional

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.wait import wait_until


def test_forget_waiting_job(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "hostname"])
    forget_jobs(hq_env, "1", forgotten=0, ignored=1)
    hq_env.start_worker()
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_forget_running_job(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "sleep", "100"])
    wait_for_job_state(hq_env, 1, "RUNNING")

    forget_jobs(hq_env, "1", forgotten=0, ignored=1)


def test_forget_finished_job(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "hostname"])
    hq_env.start_worker()
    wait_for_job_state(hq_env, 1, "FINISHED")
    forget_jobs(hq_env, "1", forgotten=1)
    wait_for_job_list_count(hq_env, 0)


def test_forget_failed_job(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "/non-existent"])
    hq_env.start_worker()
    wait_for_job_state(hq_env, 1, "FAILED")
    forget_jobs(hq_env, "1", forgotten=1, statutes=["failed"])
    wait_for_job_list_count(hq_env, 0)


def test_forget_canceled_job(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "sleep", "100"])
    hq_env.command(["job", "cancel", "1"])
    forget_jobs(hq_env, "1", forgotten=1, statutes=["canceled"])
    wait_for_job_list_count(hq_env, 0)


def test_forget_multiple_jobs(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "hostname"])
    hq_env.command(["job", "cancel", "1"])

    hq_env.start_worker()
    for _ in range(5):
        hq_env.command(["submit", "hostname"])
    wait_for_job_state(hq_env, [2, 3, 4, 5, 6], "FINISHED")

    hq_env.command(["submit", "/non-existing"])
    wait_for_job_state(hq_env, 7, "FAILED")

    forget_jobs(
        hq_env, "all", forgotten=6, ignored=1, statutes=["finished", "canceled"]
    )
    wait_for_job_list_count(hq_env, 1)


def wait_for_job_list_count(hq_env: HqEnv, count: int):
    wait_until(
        lambda: len(hq_env.command(["job", "list", "--all"], as_table=True)) == count
    )


def forget_jobs(
    hq_env: HqEnv,
    selector: str,
    forgotten: int,
    statutes: Optional[List[str]] = None,
    ignored: int = 0,
):
    pluralized = "jobs" if forgotten != 1 else "job"

    args = ["job", "forget", selector]
    if statutes:
        args.extend(["--filter", ",".join(statutes)])
    output = hq_env.command(args, as_lines=True)

    expected_msg = f"{forgotten} {pluralized} were forgotten"
    if ignored > 0:
        expected_msg += f", {ignored} were ignored due to wrong state or invalid ID"
    assert expected_msg in output[0]

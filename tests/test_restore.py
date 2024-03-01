from .conftest import HqEnv
from .utils import wait_for_job_state
import os


def test_restore_fully_finished(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.start_worker()
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    hq_env.command(["submit", "--", "sleep", "0"])
    wait_for_job_state(hq_env, [1, 2], "FINISHED")
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])
    hq_env.start_worker()
    hq_env.command(["submit", "--", "sleep", "0"])
    wait_for_job_state(hq_env, 3, "FINISHED")


def test_restore_waiting_task(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    hq_env.command(["submit", "--", "sleep", "2"])
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])
    table = hq_env.command(["job", "list"], as_table=True)
    print(table)
    xxx()
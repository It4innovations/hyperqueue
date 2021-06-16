import collections
import os
import time

from .utils import wait_for_job_state
from .conftest import HqEnv, print_table


def test_job_array_submit(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4)
    hq_env.command(
        ["submit", "--array=30-36", "--", "bash", "-c", "echo $HQ_JOB_ID-$HQ_TASK_ID"]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i in list(range(0, 30)) + list(range(37, 40)):
        assert not os.path.isfile(os.path.join(hq_env.work_path, f"stdout.1.{i}"))
        assert not os.path.isfile(os.path.join(hq_env.work_path, f"stderr.1.{i}"))

    for i in range(36, 37):
        stdout = os.path.join(hq_env.work_path, f"stdout.1.{i}")
        assert os.path.isfile(stdout)
        assert os.path.isfile(os.path.join(hq_env.work_path, f"stderr.1.{i}"))
        with open(stdout) as f:
            assert f.read() == f"1-{i}\n"


def test_job_array_report(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4)
    hq_env.command(["submit", "--array=10-19", "--", "sleep", "1"])
    time.sleep(1.6)
    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "RUNNING"

    table = hq_env.command(["job", "1"], as_table=True)

    assert table[6] == ["Tasks", "10; Ids: 10-19"]

    assert table[2][0] == "State"
    assert table[3][0] == ""
    assert table[4][0] == ""

    assert table[3][1] == "RUNNING (4)"
    assert table[4][1] == "FINISHED (4)"
    assert table[5][1] == "WAITING (2)"

    time.sleep(1.6)

    table = hq_env.command(["job", "1"], as_table=True)
    assert table[2][0] == "State"
    assert table[3][1] == "FINISHED (10)"


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

    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "FAILED"

    table = hq_env.command(["job", "1"], as_table=True)
    assert table[3][1] == "FAILED (3)"
    assert table[4][1] == "FINISHED (7)"

    offset = 12

    assert table[offset][0] == "Task Id"
    assert table[offset][1] == "Error"

    assert table[offset + 1][0] == "2"
    assert table[offset + 1][1] == "Error: Program terminated with exit code 1"

    assert table[offset + 2][0] == "3"
    assert table[offset + 2][1] == "Error: Program terminated with exit code 1"

    assert table[offset + 3][0] == "7"
    assert table[offset + 3][1] == "Error: Program terminated with exit code 1"

    table = hq_env.command(["job", "1", "--tasks"], as_table=True)
    for i, s in enumerate(
        ["FINISHED", "FAILED", "FAILED", "FINISHED", "FINISHED", "FINISHED", "FAILED"]
        + 3 * ["FINISHED"]
    ):
        assert table[offset + 1 + i][0] == str(i)


def test_job_array_error_all(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--array=0-9", "--", "/non-existent"])
    hq_env.start_worker(cpus=2)

    wait_for_job_state(hq_env, 1, "FAILED")

    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "FAILED"

    table = hq_env.command(["job", "1"], as_table=True)
    assert table[3][1] == "FAILED (10)"

    offset = 12

    for i in range(5):
        assert table[offset + i][0] == str(i)
        assert "No such file or directory" in table[offset + i][1]
    print_table(table)
    assert table[offset + 5] == []

    table = hq_env.command(["job", "1", "--tasks"], as_table=True)
    assert table[3][1] == "FAILED (10)"

    for i in range(10):
        assert table[offset + i][0] == str(i)
        assert table[offset + i][1] == "FAILED"
        assert "No such file or directory" in table[offset + i][2]


def test_job_array_cancel(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4)
    hq_env.command(["submit", "--array=0-9", "--", "sleep", "1"])
    time.sleep(1.6)
    hq_env.command(["cancel", "1"])
    time.sleep(0.4)

    table = hq_env.command(["job", "1", "--tasks"], as_table=True)
    assert table[3][1] == "FINISHED (4)"
    assert table[4][1] == "CANCELED (6)"
    c = collections.Counter([x[1] for x in table[9:]])
    assert c.get("FINISHED") == 4
    assert c.get("CANCELED") == 6

    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "CANCELED"


def test_array_reporting_state_after_worker_lost(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(1, cpus=2)
    hq_env.command(["submit", "--array=1-4", "sleep", "1"])
    time.sleep(0.25)
    hq_env.kill_worker(1)
    time.sleep(0.25)
    table = hq_env.command(["job", "1", "--tasks"], as_table=True)
    c = collections.Counter([x[1] for x in table[8:]])
    assert table[3][1] == "WAITING (4)"
    assert c.get("WAITING") == 4
    hq_env.start_workers(1, cpus=2)

    time.sleep(2.2)
    table = hq_env.command(["job", "1", "--tasks"], as_table=True)
    c = collections.Counter([x[1] for x in table[8:]])
    assert table[3][1] == "FINISHED (4)"
    assert c.get("FINISHED") == 4


def test_array_mix_with_simple_jobs(hq_env: HqEnv):
    hq_env.start_server()
    for i in range(100):
        hq_env.command(["submit", "--array=1-4", "/bin/hostname"])
        hq_env.command(["submit", "/bin/hostname"])
    hq_env.start_workers(1, cpus=2)

    wait_for_job_state(hq_env, list(range(1, 101)), "FINISHED")

    table = hq_env.command("jobs", as_table=True)
    for i in range(1, 101):
        assert table[i][0] == str(i)
        assert table[i][2] == "FINISHED"
        assert table[i][3] == "4" if i % 2 == 1 else "1"

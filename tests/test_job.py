from .conftest import HqEnv
import time
import os


def test_job_submit(hq_env: HqEnv):
    hq_env.start_server()
    #table = hq_env.command("jobs")
    #print(table)
    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 1
    assert table[0][:3] == ["Id", "Name", "State"]

    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello2'"])
    time.sleep(0.2)

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 3
    assert table[1][:3] == ["1", "bash", "WAITING"]
    assert table[2][:3] == ["2", "bash", "WAITING"]

    hq_env.start_worker(n_cpus=1)
    time.sleep(0.3)

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 3
    assert table[1][:3] == ["1", "bash", "FINISHED"]
    assert table[2][:3] == ["2", "bash", "FINISHED"]

    hq_env.command(["submit", "--", "sleep", "1"])
    time.sleep(0.2)

    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 4
    assert table[1][:3] == ["1", "bash", "FINISHED"]
    assert table[2][:3] == ["2", "bash", "FINISHED"]
    assert table[3][:3] == ["3", "sleep", "RUNNING"]

    time.sleep(1.0)
    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 4
    assert table[1][:3] == ["1", "bash", "FINISHED"]
    assert table[2][:3] == ["2", "bash", "FINISHED"]
    assert table[3][:3] == ["3", "sleep", "FINISHED"]


def test_job_output(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(n_cpus=1)
    hq_env.command(["submit", "--", "bash", "-c", "echo 'hello'"])
    hq_env.command(["submit", "--", "ls", "/non-existent"])
    hq_env.command(["submit", "--", "/non-existent-program"])
    time.sleep(0.2)
    print(hq_env.command("jobs"))
    with open(os.path.join(tmp_path, "stdout.1.0")) as f:
         assert f.read() == "hello\n"
    with open(os.path.join(tmp_path, "stderr.1.0")) as f:
         assert f.read() == ""

    with open(os.path.join(tmp_path, "stdout.2.0")) as f:
         assert f.read() == ""
    with open(os.path.join(tmp_path, "stderr.2.0")) as f:
         data = f.read()
         assert "No such file or directory" in data
         assert data.startswith("ls:")

    with open(os.path.join(tmp_path, "stdout.3.0")) as f:
         assert f.read() == ""
    with open(os.path.join(tmp_path, "stderr.3.0")) as f:
         assert f.read() == ""


def test_job_fail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(n_cpus=1)
    hq_env.command(["submit", "--", "/non-existent-program"])
    time.sleep(0.2)
    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 2
    assert table[1][:3] == ["1", "non-existent-program", "FAILED"]

    table = hq_env.command(["job", "1"], as_table=True)
    assert table[0] == ["Id", "1"]
    assert table[2] == ["State", "FAILED"]

    assert table[8][0] == "0"
    assert "No such file or directory" in table[8][1]


def test_job_invalid(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(n_cpus=1)
    result = hq_env.command(["job", "5"])
    assert "Job 5 not found" in result


def test_cancel_without_workers(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "/bin/hostname"])
    r = hq_env.command(["cancel", "1"])
    assert "Job 1 canceled" in r
    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "CANCELED"
    hq_env.start_worker(n_cpus=1)
    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "CANCELED"


def test_cancel_running(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(n_cpus=1)
    hq_env.command(["submit", "sleep", "10"])
    time.sleep(0.3)
    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "RUNNING"
    r = hq_env.command(["cancel", "1"])
    assert "Job 1 canceled" in r
    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "CANCELED"

    r = hq_env.command(["cancel", "1"])
    assert "Canceling job 1 failed" in r


def test_cancel_finished(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(n_cpus=1)
    hq_env.command(["submit", "hostname"])
    hq_env.command(["submit", "/invalid"])
    time.sleep(0.3)
    r = hq_env.command(["cancel", "1"])
    assert "Canceling job 1 failed" in r
    r = hq_env.command(["cancel", "2"])
    assert "Canceling job 2 failed" in r

    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "FINISHED"
    assert table[2][2] == "FAILED"


def test_reporting_state_after_worker_lost(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(2, n_cpus=1)
    hq_env.command(["submit", "sleep", "1"])
    hq_env.command(["submit", "sleep", "1"])
    time.sleep(0.25)
    table = hq_env.command(["jobs"], as_table=True)
    assert table[1][2] == "RUNNING"
    assert table[2][2] == "RUNNING"
    hq_env.kill_worker(1)
    time.sleep(0.25)
    table = hq_env.command(["jobs"], as_table=True)
    print(table)
    if table[1][2] == "WAITING":
        idx, other = 1, 2
    elif table[2][2] == "WAITING":
        idx, other = 2, 1
    else:
        assert 0
    assert table[other][2] == "RUNNING"

    time.sleep(1)
    table = hq_env.command(["jobs"], as_table=True)
    assert table[other][2] == "FINISHED"
    assert table[idx][2] == "RUNNING"
    time.sleep(1)
    table = hq_env.command(["jobs"], as_table=True)
    assert table[other][2] == "FINISHED"
    assert table[idx][2] == "FINISHED"
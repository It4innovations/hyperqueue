from conftest import HqEnv
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
    assert table[3][:3] == ["3", "sleep", "WAITING"]

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
    with open(os.path.join(tmp_path, "stdout.1")) as f:
         assert f.read() == "hello\n"
    with open(os.path.join(tmp_path, "stderr.1")) as f:
         assert f.read() == ""

    with open(os.path.join(tmp_path, "stdout.2")) as f:
         assert f.read() == ""
    with open(os.path.join(tmp_path, "stderr.2")) as f:
         data = f.read()
         assert "No such file or directory" in data
         assert data.startswith("ls:")

    with open(os.path.join(tmp_path, "stdout.3")) as f:
         assert f.read() == ""
    with open(os.path.join(tmp_path, "stderr.3")) as f:
         assert f.read() == ""


def test_job_fail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(n_cpus=1)
    hq_env.command(["submit", "--", "/non-existent-program"])
    time.sleep(0.2)
    table = hq_env.command("jobs", as_table=True)
    assert len(table) == 2
    assert table[1][:3] == ["1", "/non-existent-program", "FAILED"]

    table = hq_env.command(["job", "1"], as_table=True)
    assert table[0] == ["Id", "1"]
    assert table[2] == ["State", "FAILED"]
    assert table[3][0] == "Error"
    assert "No such file or directory" in table[3][1]


def test_job_invalid(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(n_cpus=1)
    result = hq_env.command(["job", "5"])
    assert "Job 5 not found" in result

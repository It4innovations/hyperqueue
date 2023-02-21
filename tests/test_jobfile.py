
from .conftest import HqEnv
from .utils import wait_for_job_state


def test_job_file_submit_minimal(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker()
    tmp_path.joinpath("job.toml").write_text("""
[[task]]
id = 0
command = ["sleep", "0"]
    """)
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_job_file_submit_maximal(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker()
    tmp_path.joinpath("job.toml").write_text("""
name = "test-job"
stream_log = "output.log"
max_fails = 11

[[task]]
id = 12
pin = "omp"
stdout = "testout-%{TASK_ID}"
stderr = "testerr-%{TASK_ID}"
command = ["bash", "-c", "echo $ABC $XYZ; >&2 echo error"]
    [[env]]
    ABC = 123
    XYZ = "aaaa"

[[task]]
id = 200
pin = "taskset"
cwd = "test-200"
command = ["bash", "-c", "echo test1"]

[[task]]
id = 13
command = ["sleep", "0"]
""")
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Name", "test-job")

    table = hq_env.command(["task", "info", "1", "12"], as_table=True)
    table.check_row_value("Pin", "omp")
    paths = table.get_row_value("Paths").split("\n")
    assert paths[1].endswith("testout-12")
    assert paths[2].endswith("testerr-12")

    table = hq_env.command(["task", "info", "1", "200"], as_table=True)
    table.check_row_value("Pin", "taskset")

    paths = table.get_row_value("Paths").split("\n")
    assert paths[0].endswith("test-200")
    assert paths[1].endswith("/test-200/job-1/200.stdout")
    assert paths[2].endswith("/test-200/job-1/200.stderr")

    table = hq_env.command(["task", "info", "1", "13"], as_table=True)
    table.check_row_value("Pin", "none")


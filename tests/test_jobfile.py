
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
command = ["sleep", "0"]

[[task]]
id = 200
pin = "taskset"
command = ["sleep", "0"]

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

    table = hq_env.command(["task", "info", "1", "200"], as_table=True)
    table.check_row_value("Pin", "taskset")

    table = hq_env.command(["task", "info", "1", "13"], as_table=True)
    table.check_row_value("Pin", "none")
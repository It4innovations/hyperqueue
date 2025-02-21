from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.job import default_task_output


def test_task_data_invalid_call(hq_env: HqEnv):
    hq_env.command(["task-data"])


def test_data_create_no_consumer(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
[[task]]
id = 0
command = ["bash", "-c", "set -e; echo 'abc' > test.txt; $HQ data put 1 test.txt"]
keep_outputs = true
"""
    )
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_data_create_same_worker_consumer(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 12
command = ["bash", "-c", "set -e; echo 'abc' > test.txt; $HQ data put 3 test.txt"]

[[task]]
id = 13
command = ["bash", "-c", "set -e; $HQ data get 0"]

[[task.data_deps]]
task_id = 12
data_id = 3 
"""
    )
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")

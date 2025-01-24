from conftest import HqEnv


def test_task_data_invalid_call(hq_env: HqEnv):
    hq_env.command(["task-data"])


def test_data_create_no_consumer(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
[[task]]
command = ["sleep", "0"]
keep_outputs = true
"""
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")

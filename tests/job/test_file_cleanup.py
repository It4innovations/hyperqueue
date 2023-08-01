from pathlib import Path

import pytest

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.cmd import python
from ..utils.io import create_file
from ..utils.job import default_task_output


def program_wait_for_file_exists(file_name: str) -> str:
    return f"""
import os.path
import time

while True:
    if os.path.isfile("{file_name}"):
        break
    time.sleep(0.1)
"""


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
def test_cleanup_stdout_on_success_default_path(hq_env: HqEnv, stream: str):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(
        [
            "submit",
            f"--{stream}=:rm-if-finished",
            "--",
            *python(program_wait_for_file_exists("finish")),
        ]
    )

    output = default_task_output(type=stream)
    wait_for_job_state(hq_env, 1, "RUNNING")
    assert Path(output).is_file()
    create_file("finish")

    wait_for_job_state(hq_env, 1, "FINISHED")
    assert not Path(output).is_file()


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
def test_cleanup_stdout_on_success_custom_path(hq_env: HqEnv, stream: str):
    hq_env.start_server()
    hq_env.start_worker()

    output = "foo.txt"
    hq_env.command(
        [
            "submit",
            f"--{stream}={output}:rm-if-finished",
            "--",
            *python(program_wait_for_file_exists("finish")),
        ]
    )

    wait_for_job_state(hq_env, 1, "RUNNING")
    assert Path(output).is_file()
    create_file("finish")

    wait_for_job_state(hq_env, 1, "FINISHED")
    assert not Path(output).is_file()


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
def test_do_not_cleanup_stdout_on_fail(hq_env: HqEnv, stream: str):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", f"--{stream}=:rm-if-finished", "--", "/non-existent"])

    output = default_task_output(type=stream)
    wait_for_job_state(hq_env, 1, "FAILED")
    assert Path(output).is_file()

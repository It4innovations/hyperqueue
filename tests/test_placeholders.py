import os
from os.path import abspath

import pytest

from .conftest import HqEnv
from .utils import JOB_TABLE_ROWS, wait_for_job_state
from .utils.job import default_task_output


def test_cwd_recursive_placeholder(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        ["submit", "--cwd", "%{CWD}/foo", "--", "bash", "-c", "echo 'hello'"],
        expect_fail="Working directory path cannot contain the working "
        "directory placeholder `%{CWD}`.",
    )


def test_job_paths_prefilled_placeholders(hq_env: HqEnv):
    """
    Checks that job paths are partially resolved after submit.
    """
    hq_env.start_server()
    hq_env.command(["submit", "hostname"])
    wait_for_job_state(hq_env, 1, "WAITING")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Stdout") == default_task_output(task_id="%{TASK_ID}")


def test_task_resolve_submit_placeholders(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(["submit", "echo", "test"])
    table = hq_env.command(["job", "info", "1", "--tasks"], as_table=True)[
        JOB_TABLE_ROWS:
    ].as_horizontal()
    wait_for_job_state(hq_env, 1, "WAITING")
    table.check_column_value("Working directory", 0, "")
    table.check_column_value("Stdout", 0, "")
    table.check_column_value("Stderr", 0, "")

    hq_env.start_worker()

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "info", "1", "--tasks"], as_table=True)[
        JOB_TABLE_ROWS:
    ].as_horizontal()
    table.check_column_value("Working directory", 0, os.getcwd())
    table.check_column_value("Stdout", 0, default_task_output())
    table.check_column_value("Stderr", 0, default_task_output(type="stderr"))


def test_task_resolve_worker_placeholders(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        [
            "submit",
            "--stdout",
            "%{INSTANCE_ID}.out",
            "--stderr",
            "%{INSTANCE_ID}.err",
            "--cwd",
            "%{INSTANCE_ID}-dir",
            "echo",
            "test",
        ]
    )
    table = hq_env.command(["job", "info", "1", "--tasks"], as_table=True)[
        JOB_TABLE_ROWS:
    ].as_horizontal()
    wait_for_job_state(hq_env, 1, "WAITING")
    table.check_column_value("Working directory", 0, "")
    table.check_column_value("Stdout", 0, "")
    table.check_column_value("Stderr", 0, "")

    hq_env.start_worker()

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "info", "1", "--tasks"], as_table=True)[
        JOB_TABLE_ROWS:
    ].as_horizontal()
    table.check_column_value("Working directory", 0, abspath("0-dir"))
    table.check_column_value("Stdout", 0, abspath("0.out"))
    table.check_column_value("Stderr", 0, abspath("0.err"))


def test_stream_submit_placeholder(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        ["submit", "--log", "log-%{JOB_ID}", "--", "bash", "-c", "echo Hello"]
    )
    hq_env.start_workers(1)
    wait_for_job_state(hq_env, 1, "FINISHED")

    lines = set(hq_env.command(["log", "log-1", "show"], as_lines=True))
    assert "0:0> Hello" in lines
    assert "0: > stream closed" in lines


@pytest.mark.parametrize("channel", ("stdout", "stderr"))
def test_warning_missing_placeholder_in_output(hq_env: HqEnv, channel: str):
    hq_env.start_server()
    output = hq_env.command(
        ["submit", "--array=1-4", f"--{channel}=foo", "/bin/hostname"]
    )
    assert (
        f"You have submitted an array job, but the `{channel}` "
        + "path does not contain the task ID placeholder."
        in output
    )
    assert f"Consider adding `%{{TASK_ID}}` to the `--{channel}` value."


def test_unknown_placeholder(hq_env: HqEnv):
    hq_env.start_server()
    output = hq_env.command(
        [
            "submit",
            "--log",
            "log-%{FOO}",
            "--stdout",
            "dir/%{BAR}/%{BAZ}",
            "--stderr",
            "dir/%{TAS_ID}",
            "--cwd",
            "%{BAR}",
            "--",
            "hostname",
        ]
    )
    assert "Found unknown placeholder `FOO` in log path" in output
    assert "Found unknown placeholders `BAR, BAZ` in stdout path" in output
    assert "Found unknown placeholder `TAS_ID` in stderr path" in output
    assert "Found unknown placeholder `BAR` in working directory path" in output

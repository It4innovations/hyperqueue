import os
from os.path import abspath
from typing import Tuple

import pytest

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.job import default_task_output
from .utils.table import Table, parse_multiline_cell


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


# (cwd, stdout, stderr)
def get_paths(task_table: Table) -> Tuple[str, str, str]:
    cell = parse_multiline_cell(task_table.get_column_value("Paths")[0])
    return (cell["Workdir"], cell["Stdout"], cell["Stderr"])


def test_task_resolve_submit_placeholders(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(["submit", "echo", "test"])
    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    wait_for_job_state(hq_env, 1, "WAITING")
    assert table.get_column_value("Paths")[0] == ""

    hq_env.start_worker()

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    assert get_paths(table) == (
        os.getcwd(),
        default_task_output(),
        default_task_output(type="stderr"),
    )


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
    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    wait_for_job_state(hq_env, 1, "WAITING")
    assert table.get_column_value("Paths")[0] == ""

    hq_env.start_worker()

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "tasks", "1"], as_table=True)
    assert get_paths(table) == (abspath("0-dir"), abspath("0.out"), abspath("0.err"))


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

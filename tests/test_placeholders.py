import os
from os.path import abspath
from typing import Tuple

import pytest

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.io import check_file_contents
from .utils.job import default_task_output
from .utils.table import Table, parse_multiline_cell


def test_cwd_recursive_placeholder(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        ["submit", "--cwd", "%{CWD}/foo", "--", "bash", "-c", "echo 'hello'"],
        expect_fail="Working directory path cannot contain the working directory placeholder `%{CWD}`.",
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


def test_job_paths_prefilled_placeholders_resolvable_cwd(hq_env: HqEnv):
    """
    Checks that job paths are partially resolved after submit.
    """
    hq_env.start_server()
    hq_env.command(["submit", "--cwd", "/tmp/foo", "hostname"])
    wait_for_job_state(hq_env, 1, "WAITING")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Stdout") == "/tmp/foo/job-1/%{TASK_ID}.stdout"

    hq_env.start_worker()
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
    assert get_paths(table) == (
        "/tmp/foo",
        "/tmp/foo/job-1/0.stdout",
        "/tmp/foo/job-1/0.stderr",
    )


def test_job_paths_prefilled_placeholders_unresolvable_cwd(hq_env: HqEnv):
    """
    Checks that job paths are partially resolved after submit.
    """
    hq_env.start_server()
    hq_env.command(["submit", "--cwd", "/tmp/%{TASK_ID}", "hostname"])
    wait_for_job_state(hq_env, 1, "WAITING")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Stdout") == "/tmp/%{TASK_ID}/job-1/%{TASK_ID}.stdout"

    hq_env.start_worker()
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
    assert get_paths(table) == (
        "/tmp/0",
        "/tmp/0/job-1/0.stdout",
        "/tmp/0/job-1/0.stderr",
    )


# (cwd, stdout, stderr)
def get_paths(task_table: Table) -> Tuple[str, str, str]:
    cell = parse_multiline_cell(task_table.get_row_value("Paths"))
    return (cell["Workdir"], cell["Stdout"], cell["Stderr"])


def test_task_resolve_submit_placeholders(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(["submit", "echo", "test"])
    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
    wait_for_job_state(hq_env, 1, "WAITING")
    assert table.get_row_value("Paths") == ""

    hq_env.start_worker()

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
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
    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
    wait_for_job_state(hq_env, 1, "WAITING")
    assert table.get_row_value("Paths") == ""

    hq_env.start_worker()

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
    assert get_paths(table) == (
        abspath("0-dir"),
        abspath("0-dir/0.out"),
        abspath("0-dir/0.err"),
    )


def test_stream_submit_placeholder(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("log-1")
    hq_env.command(["submit", "--stream", "log-%{JOB_ID}", "--", "bash", "-c", "echo Hello"])
    hq_env.start_workers(1)
    wait_for_job_state(hq_env, 1, "FINISHED")

    lines = set(hq_env.command(["read", "log-1", "show"], as_lines=True))
    assert "1.0:0> Hello" in lines


def test_server_uid_placeholder(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_workers(1)

    server_info = hq_env.command(["server", "--output-mode=json", "info"], as_json=True)
    server_uid = server_info["server_uid"]

    os.mkdir(f"log-{server_uid}-2")

    hq_env.command(
        [
            "submit",
            "--stdout",
            "out-%{SERVER_UID}-%{JOB_ID}",
            "--",
            "bash",
            "-c",
            "echo Hello",
        ]
    )
    hq_env.command(
        [
            "submit",
            "--stream",
            "log-%{SERVER_UID}-%{JOB_ID}",
            "--",
            "bash",
            "-c",
            "echo Hello",
        ]
    )
    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    check_file_contents(os.path.join(tmp_path, f"out-{server_uid}-1"), "Hello\n")
    assert len(os.listdir(os.path.join(tmp_path, f"log-{server_uid}-2"))) == 1

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Stdout", f"{os.getcwd()}/out-{server_uid}-1")


@pytest.mark.parametrize("channel", ("stdout", "stderr"))
def test_warning_missing_placeholder_in_output(hq_env: HqEnv, channel: str):
    hq_env.start_server()
    output = hq_env.command(["submit", "--array=1-4", f"--{channel}=foo", "/bin/hostname"])
    assert (
        f"You have submitted an array job, but the `{channel}` " + "path does not contain the task ID placeholder."
        in output
    )
    assert f"Consider adding `%{{TASK_ID}}` to the `--{channel}` value."


@pytest.mark.parametrize("channel", ("stdout", "stderr"))
def test_missing_placeholder_in_output_present_in_cwd(hq_env: HqEnv, channel: str):
    hq_env.start_server()
    output = hq_env.command(
        [
            "submit",
            "--array=1-4",
            "--cwd",
            "task-%{TASK_ID}",
            f"--{channel}",
            "%{CWD}/foo",
            "/bin/hostname",
        ]
    )
    assert "path does not contain the task ID placeholder." not in output


def test_unknown_placeholder(hq_env: HqEnv):
    hq_env.start_server()
    output = hq_env.command(
        [
            "submit",
            "--stream",
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

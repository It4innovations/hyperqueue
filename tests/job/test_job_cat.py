from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.cmd import bash, python


def test_job_cat_stdout(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--array", "1-2", "--", *bash("echo $HQ_TASK_ID")])

    wait_for_job_state(hq_env, 1, "FINISHED")
    output = hq_env.command(["job", "cat", "1", "stdout"])
    assert output.splitlines() == ["1", "2"]

    output = hq_env.command(["job", "cat", "--tasks", "2", "1", "stdout"])
    assert output.strip() == "2"

    output = hq_env.command(["job", "cat", "--tasks", "0", "1", "stdout"])
    assert "Task 0 not found" in output.strip()


def test_job_cat_stderr(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--array",
            "1-2",
            "--",
            *python(
                """
import sys
import os
print(os.environ['HQ_TASK_ID'], file=sys.stderr)
"""
            ),
        ]
    )

    wait_for_job_state(hq_env, 1, "FINISHED")
    output = hq_env.command(["job", "cat", "1", "stderr"])
    assert output.splitlines() == ["1", "2"]

    output = hq_env.command(["job", "cat", "--tasks", "3", "1", "stdout"])
    assert "Task 3 not found" in output.strip()


def test_job_cat_no_output(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(["submit", "--stdout=none", "--stderr=none", "--", "echo", "hello"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    output = hq_env.command(["job", "cat", "1", "stdout"])
    assert "Task 0 has no `stdout` stream associated with it" in output.strip()

    output = hq_env.command(["job", "cat", "1", "stderr"])
    assert "Task 0 has no `stderr` stream associated with it" in output.strip()


def test_job_cat_header(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--array",
            "1-3",
            "--",
            "python3",
            "-c",
            """
import sys
import os
print(os.environ['HQ_TASK_ID'], file=sys.stdout)
print("out1", file=sys.stdout)
print("out2", file=sys.stdout)

print(os.environ['HQ_TASK_ID'], file=sys.stderr)
print("err1", file=sys.stderr)
""",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    output = hq_env.command(["job", "cat", "1", "stdout", "--print-task-header"])
    assert (
        output
        == """
# Task 1
1
out1
out2
# Task 2
2
out1
out2
# Task 3
3
out1
out2
""".lstrip()
    )

    output = hq_env.command(["job", "cat", "1", "stderr", "--print-task-header"])
    assert (
        output
        == """
# Task 1
1
err1
# Task 2
2
err1
# Task 3
3
err1
""".lstrip()
    )


def test_job_cat_status(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--array=3-9",
            "--",
            "python3",
            "-c",
            """
import sys
import os
print(os.environ['HQ_TASK_ID'], file=sys.stdout)
print("out", file=sys.stdout)

assert os.environ['HQ_TASK_ID'] not in ['4', '5', '6', '8']
""",
        ]
    )
    wait_for_job_state(hq_env, 1, "FAILED")

    output = hq_env.command(["job", "cat", "--task-status=finished", "1", "stdout", "--print-task-header"])
    assert (
        output
        == """
# Task 3
3
out
# Task 7
7
out
# Task 9
9
out
""".lstrip()
    )

    output = hq_env.command(["job", "cat", "--task-status=failed", "--tasks", "3-7", "1", "stdout"])
    assert (
        output
        == """
4
out
5
out
6
out
""".lstrip()
    )

    output_selected = hq_env.command(["job", "cat", "--task-status", "finished,failed", "1", "stdout"])
    output_default = hq_env.command(["job", "cat", "1", "stdout"])
    assert (
        output_selected
        == output_default
        == """
3
out
4
out
5
out
6
out
7
out
8
out
9
out
""".lstrip()
    )


def test_job_cat_last(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    output = hq_env.command(["job", "cat", "last", "stdout"])
    assert "No jobs were found" in output

    hq_env.command(["submit", "--", "bash", "-c", "echo '1'"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    hq_env.command(["submit", "--", "bash", "-c", "echo '2'"])
    wait_for_job_state(hq_env, 2, "FINISHED")

    output = hq_env.command(["job", "cat", "last", "stdout"])
    assert output.rstrip() == "2"

    output = hq_env.command(["job", "cat", "last", "stderr"])
    assert output == ""

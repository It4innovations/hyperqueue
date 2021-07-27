import os

from .conftest import HqEnv
from .utils import wait_for_job_state


def test_entries_no_newline(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=2)

    with open("input", "w") as f:
        f.write("One\nTwo\nThree\nFour")

    hq_env.command(
        [
            "submit",
            "--each-line=input",
            "--",
            "bash",
            "-c",
            "echo $HQ_ENTRY",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i, test in enumerate(["One\n", "Two\n", "Three\n", "Four\n"]):
        with open(f"stdout.1.{i}") as f:
            line = f.read()
        assert line == test
    assert not os.path.isfile("stdout.0.4")

    table = hq_env.command(["job", "1"], as_table=True)
    assert table.get_row_value("State").split("\n")[-1] == "FINISHED (4)"


def test_entries_with_newline(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=2)

    with open("input", "w") as f:
        f.write("One\nTwo\nThree\nFour\n")

    hq_env.command(
        [
            "submit",
            "--each-line=input",
            "--",
            "bash",
            "-c",
            "echo $HQ_ENTRY",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i, test in enumerate(["One\n", "Two\n", "Three\n", "Four\n"]):
        with open(f"stdout.1.{i}") as f:
            line = f.read()
        assert line == test
    assert not os.path.isfile("stdout.0.4")

    table = hq_env.command(["job", "1"], as_table=True)
    assert table.get_row_value("State").split("\n")[-1] == "FINISHED (4)"

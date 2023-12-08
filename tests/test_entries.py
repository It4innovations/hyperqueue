import os

import pytest

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.io import check_file_contents
from .utils.job import default_task_output


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
        check_file_contents(default_task_output(job_id=1, task_id=i), test)
    assert not os.path.isfile(default_task_output(job_id=1, task_id=4))

    table = hq_env.command(["job", "info", "1"], as_table=True)
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
        check_file_contents(default_task_output(job_id=1, task_id=i), test)
    assert not os.path.isfile(default_task_output(task_id=4))

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("State").split("\n")[-1] == "FINISHED (4)"


def test_entries_from_json_entry(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=2)

    with open("input", "w") as f:
        f.write('[123, {"x":\n[1,2,3]}, 2.5]')

    hq_env.command(
        [
            "submit",
            "--from-json=input",
            "--",
            "bash",
            "-c",
            "echo $HQ_ENTRY",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i, test in enumerate(["123\n", '{"x":[1,2,3]}\n', "2.5\n"]):
        check_file_contents(default_task_output(job_id=1, task_id=i), test)
    assert not os.path.isfile(default_task_output(task_id=3))

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("State").split("\n")[-1] == "FINISHED (3)"


def test_entries_invalid_from_json_entry(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=2)

    with open("input", "w") as f:
        f.write('{"x":\n[1,2,3]}')

    with pytest.raises(
        Exception,
        match="The top element of the provided JSON file has to be an array",
    ):
        hq_env.command(
            [
                "submit",
                "--from-json=input",
                "--",
                "bash",
                "-c",
                "echo $HQ_ENTRY",
            ]
        )


def test_each_line_with_array(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=2)

    with open("input", "w") as f:
        f.write("One\nTwo\nThree\nFour\nFive\nSix\nSeven")

    hq_env.command(
        [
            "submit",
            "--each-line=input",
            "--array", "2-4, 6",
            "--",
            "bash",
            "-c",
            "echo $HQ_ENTRY,$HQ_TASK_ID",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i, test in enumerate([None, None, "Three,2\n", "Four,3\n", "Five,4\n", None, "Seven,6\n"]):
        filename = default_task_output(job_id=1, task_id=i)
        if test is None:
            assert not os.path.exists(filename)
        else:
            check_file_contents(filename, test)

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("State").split("\n")[-1] == "FINISHED (4)"


def test_json_with_array(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=2)

    with open("input", "w") as f:
        f.write('["One", "Two", "Three", "Four", "Five", "Six", "Seven"]')

    hq_env.command(
        [
            "submit",
            "--from-json=input",
            "--array", "2-3, 5, 6, 7, 1000",
            "--",
            "bash",
            "-c",
            "echo $HQ_ENTRY,$HQ_TASK_ID",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i, test in enumerate([None, None, '"Three",2\n', '"Four",3\n', None, '"Six",5\n', '"Seven",6\n']):
        filename = default_task_output(job_id=1, task_id=i)
        if test is None:
            assert not os.path.exists(filename)
        else:
            check_file_contents(filename, test)

    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("State").split("\n")[-1] == "FINISHED (4)"

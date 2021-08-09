import os
import subprocess

import pytest

from .conftest import RUNNING_IN_CI, HqEnv
from .utils import print_table, wait_for_job_state


def read_list(filename):
    with open(filename) as f:
        return [int(x) for x in f.read().split(",")]


def test_job_num_of_cpus(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--", "bash", "-c", "echo $HQ_CPUS"])
    hq_env.command(
        ["submit", "--cpus", "2 scatter", "--", "bash", "-c", "echo $HQ_CPUS"]
    )
    hq_env.command(
        ["submit", "--cpus", "5 scatter", "--", "bash", "-c", "echo $HQ_CPUS"]
    )

    hq_env.command(
        ["submit", "--cpus", "4 compact!", "--", "bash", "-c", "echo $HQ_CPUS"]
    )

    hq_env.command(
        ["submit", "--cpus", "5 compact!", "--", "bash", "-c", "echo $HQ_CPUS"]
    )

    hq_env.command(["submit", "--cpus", "all", "--", "bash", "-c", "echo $HQ_CPUS"])

    hq_env.start_worker(cpus="3x4")

    wait_for_job_state(hq_env, [1, 2, 4, 5, 6], "FINISHED")

    table = hq_env.command(["job", "1"], as_table=True)
    table.check_value_row("Resources", "1 compact")
    assert len(read_list("stdout.1.0")) == 1

    table = hq_env.command(["job", "2"], as_table=True)
    table.check_value_row("Resources", "2 scatter")
    assert len(set(x // 4 for x in read_list("stdout.2.0"))) == 2

    table = hq_env.command(["job", "4"], as_table=True)
    table.check_value_row("Resources", "4 compact!")
    lst = read_list("stdout.4.0")
    assert len(set(x // 4 for x in lst)) == 1
    assert len(lst) == 4

    table = hq_env.command(["job", "5"], as_table=True)
    table.check_value_row("Resources", "5 compact!")
    lst = read_list("stdout.5.0")
    assert len(set(x // 4 for x in lst)) == 2
    assert len(lst) == 5

    table = hq_env.command(["job", "6"], as_table=True)
    table.check_value_row("Resources", "all")
    lst = read_list("stdout.6.0")
    assert list(range(12)) == lst


@pytest.mark.skipif(RUNNING_IN_CI, reason="Processes in CI are already pre-pinned")
def test_manual_taskset(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--cpus",
            "2 compact",
            "--",
            "bash",
            "-c",
            "taskset -c $HQ_CPUS sleep 1",
        ]
    )
    hq_env.start_worker(cpus=4)

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "1"], as_table=True)
    table.check_value_row("State", "FINISHED")


def test_job_no_pin(hq_env: HqEnv):
    pid = os.getpid()

    process = subprocess.Popen(["taskset", "-p", str(pid)], stdout=subprocess.PIPE)
    (output, _) = process.communicate()
    exit_code = process.wait()
    assert exit_code == 0

    output = output.split()
    del output[1]  # Remove actual PID

    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--cpus",
            "2 compact!",
            "--",
            "bash",
            "-c",
            "echo $HQ_CPUS; taskset -p $$;",
        ]
    )
    hq_env.start_worker(cpus=2)

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "1"], as_table=True)
    print_table(table)
    table.check_value_row("State", "FINISHED")
    table.check_value_row("Resources", "2 compact!")

    with open("stdout.1.0", "rb") as f:
        f.readline()  # skip line
        line = f.readline().split()
        del line[1]  # Remove actual PID
        assert output == line


def test_job_pin(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--pin",
            "--cpus",
            "2 compact!",
            "--",
            "bash",
            "-c",
            "echo $HQ_CPUS; taskset -c -p $$;",
        ]
    )
    hq_env.start_worker(cpus=2)

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "1"], as_table=True)
    print_table(table)
    table.check_value_row("State", "FINISHED")
    table.check_value_row("Resources", "2 compact! [pin]")

    with open("stdout.1.0") as f:
        hq_cpus = f.readline().rstrip()
        assert hq_cpus == f.readline().rstrip().split(" ")[5]

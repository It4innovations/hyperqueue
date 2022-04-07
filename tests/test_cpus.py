import os
import subprocess
from typing import List

import pytest

from .conftest import RUNNING_IN_CI, HqEnv
from .utils import wait_for_job_state
from .utils.job import default_task_output


def read_list(filename) -> List[int]:
    with open(filename) as f:
        return split_numbers(f.read())


def split_numbers(items) -> List[int]:
    return [int(x) for x in items.split(",")]


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

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Resources", "cpus: 1 compact")
    assert len(read_list(default_task_output(job_id=1))) == 1

    table = hq_env.command(["job", "info", "2"], as_table=True)
    table.check_row_value("Resources", "cpus: 2 scatter")
    assert len(set(x // 4 for x in read_list(default_task_output(job_id=2)))) == 2

    table = hq_env.command(["job", "info", "4"], as_table=True)
    table.check_row_value("Resources", "cpus: 4 compact!")
    lst = read_list(default_task_output(job_id=4))
    assert len(set(x // 4 for x in lst)) == 1
    assert len(lst) == 4

    table = hq_env.command(["job", "info", "5"], as_table=True)
    table.check_row_value("Resources", "cpus: 5 compact!")
    lst = read_list(default_task_output(job_id=5))
    assert len(set(x // 4 for x in lst)) == 2
    assert len(lst) == 5

    table = hq_env.command(["job", "info", "6"], as_table=True)
    table.check_row_value("Resources", "cpus: all")
    lst = read_list(default_task_output(job_id=6))
    assert list(range(12)) == lst


def test_set_omp_num_threads(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="4")

    hq_env.command(
        [
            "submit",
            "--cpus",
            "4",
            "--",
            "bash",
            "-c",
            "echo $OMP_NUM_THREADS",
        ]
    )

    wait_for_job_state(hq_env, 1, "FINISHED")

    with open(default_task_output()) as f:
        assert int(f.read()) == 4


def test_do_not_override_set_omp_num_threads(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="4")

    hq_env.command(
        [
            "submit",
            "--cpus",
            "4",
            "--env",
            "OMP_NUM_THREADS=100",
            "--",
            "bash",
            "-c",
            "echo $OMP_NUM_THREADS",
        ]
    )

    wait_for_job_state(hq_env, 1, "FINISHED")

    with open(default_task_output()) as f:
        assert int(f.read()) == 100


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
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("State", "FINISHED")


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
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.print()
    table.check_row_value("State", "FINISHED")
    table.check_row_value("Resources", "cpus: 2 compact!")

    with open(default_task_output(), "rb") as f:
        f.readline()  # skip line
        line = f.readline().split()
        del line[1]  # Remove actual PID
        assert output == line


def test_job_show_pin_in_table(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        ["submit", "--pin", "taskset", "--cpus", "2 compact!", "--", "hostname"]
    )
    hq_env.start_worker(cpus=2)

    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Resources", "cpus: 2 compact! [pin]")


def test_job_pin_taskset(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=2)

    hq_env.command(
        [
            "submit",
            "--pin",
            "taskset",
            "--cpus",
            "2 compact!",
            "--",
            "bash",
            "-c",
            # `taskset -c -p $$` prints the affinity of the current process
            "echo $HQ_CPUS; taskset -c -p $$;",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    with open(default_task_output()) as f:
        hq_cpus = sorted(split_numbers(f.readline()))
        taskset_cpus = sorted(split_numbers(f.readline().rstrip().split(" ")[-1]))
        assert hq_cpus == taskset_cpus


def test_job_pin_openmp(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=2)

    hq_env.command(
        [
            "submit",
            "--pin",
            "omp",
            "--cpus",
            "2 compact!",
            "--",
            "bash",
            "-c",
            "echo $OMP_PLACES; echo $OMP_PROC_BIND",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    with open(default_task_output()) as f:
        places = f.readline().strip()
        assert places.startswith("{")
        assert places.endswith("}")
        assert sorted(split_numbers(places[1:-1])) == [0, 1]
        bind = f.readline().strip()
        assert bind == "close"

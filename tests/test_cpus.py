import time

from .conftest import HqEnv, print_table


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
    time.sleep(0.5)

    table = hq_env.command(["job", "1"], as_table=True)
    assert table[4][0] == "Resources"
    assert table[4][1] == "1 compact"
    assert len(read_list("stdout.1.0")) == 1

    table = hq_env.command(["job", "2"], as_table=True)
    assert table[4][1] == "2 scatter"
    assert len(set(x // 4 for x in read_list("stdout.2.0"))) == 2

    table = hq_env.command(["job", "4"], as_table=True)
    assert table[4][1] == "4 compact!"
    lst = read_list("stdout.4.0")
    assert len(set(x // 4 for x in lst)) == 1
    assert len(lst) == 4

    table = hq_env.command(["job", "5"], as_table=True)
    assert table[4][1] == "5 compact!"
    lst = read_list("stdout.5.0")
    assert len(set(x // 4 for x in lst)) == 2
    assert len(lst) == 5

    table = hq_env.command(["job", "6"], as_table=True)
    assert table[4][1] == "all"
    lst = read_list("stdout.6.0")
    assert list(range(12)) == lst


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
    time.sleep(1.5)

    table = hq_env.command(["job", "1"], as_table=True)
    assert table[2][1] == "FINISHED"


def test_job_no_pin(hq_env: HqEnv):

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
    time.sleep(0.4)

    table = hq_env.command(["job", "1"], as_table=True)
    print_table(table)
    assert table[2][1] == "FINISHED"
    assert table[4][1] == "2 compact!"

    with open("stdout.1.0") as f:
        f.readline()
        assert "f" == f.readline().rstrip().split(" ")[5]


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
    time.sleep(0.4)

    table = hq_env.command(["job", "1"], as_table=True)
    print_table(table)
    assert table[2][1] == "FINISHED"
    assert table[4][1] == "2 compact! [pin]"

    with open("stdout.1.0") as f:
        hq_cpus = f.readline().rstrip()
        assert hq_cpus == f.readline().rstrip().split(" ")[5]

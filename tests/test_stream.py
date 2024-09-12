import time
import os

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.io import check_file_contents
from .utils.wait import wait_until


def test_stream_collision(hq_env: HqEnv):
    os.mkdir("mylog")
    for i in range(2):
        print(f"Run {i}")
        hq_env.start_server()
        hq_env.command(
            [
                "submit",
                "--stream",
                "mylog",
                "--array=1-20",
                "--",
                "bash",
                "-c",
                "echo Hello from ${HQ_TASK_ID}",
            ]
        )
        hq_env.start_workers(2, cpus="2")
        wait_for_job_state(hq_env, 1, "FINISHED")
        hq_env.stop_server()
        time.sleep(1)

    print(hq_env.command(["read", "mylog", "show"], expect_fail="Found streams from multiple server instances"))


def test_stream_submit(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--array=1-20",
            "--",
            "bash",
            "-c",
            "echo Hello from ${HQ_TASK_ID}",
        ]
    )
    hq_env.start_workers(2, cpus="2")
    wait_for_job_state(hq_env, 1, "FINISHED")

    lines = set(hq_env.command(["read", "mylog", "show"], as_lines=True))
    for i in range(1, 21):
        assert "1.{0:02}:0> Hello from {0}".format(i) in lines
    assert len(lines) == 20

    result = hq_env.command(["read", "mylog", "show", "--channel=stderr"])
    assert result == ""

    table = hq_env.command(["read", "mylog", "summary"], as_table=True)
    table.check_row_value("Tasks", "20")
    table.check_row_value("Opened streams", "0")
    table.check_row_value("Stdout/stderr size", "271 B / 0 B")
    table.check_row_value("Superseded streams", "0")
    table.check_row_value("Superseded stdout/stderr size", "0 B / 0 B")

    result = hq_env.command(["read", "mylog", "cat", "1", "stdout"])
    assert result == "".join(["Hello from {}\n".format(i) for i in range(1, 21)])

    result = hq_env.command(["read", "mylog", "cat", "1", "stdout", "--task=3-4,2"]).splitlines()
    assert result[0] == "Hello from 3"
    assert result[1] == "Hello from 4"
    assert result[2] == "Hello from 2"

    result = hq_env.command(["read", "mylog", "export", "1", "--task=3-4,2"], as_json=True)
    assert result == [
        {"finished": True, "id": 3, "stdout": "Hello from 3\n"},
        {"finished": True, "id": 4, "stdout": "Hello from 4\n"},
        {"finished": True, "id": 2, "stdout": "Hello from 2\n"},
    ]


def test_stream_overlap(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--array=1-2",
            "--",
            "bash",
            "-c",
            "echo A; sleep 1; echo B; sleep 1",
        ]
    )
    hq_env.start_workers(2)
    wait_for_job_state(hq_env, 1, "FINISHED")
    result = hq_env.command(["read", "mylog", "show"], as_lines=True)

    chunks = [set(result[i * 2 : i * 2 + 2]) for i in range(3)]
    assert chunks[0] == {"1.1:0> A", "1.2:0> A"}
    assert chunks[1] == {"1.2:0> B", "1.1:0> B"}

    result = hq_env.command(["read", "mylog", "cat", "1", "stdout"])
    assert result == "A\nB\nA\nB\n"


def test_stream_big_output(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--array=1-2",
            "--",
            "python3",
            "-c",
            "print('1234567890' * 80_000)",
        ]
    )
    hq_env.start_workers(2)
    wait_for_job_state(hq_env, 1, "FINISHED")

    result = hq_env.command(["read", "mylog", "show"], as_lines=True)
    # print(result)
    first = []
    second = []
    for line in result:
        if line.startswith("1.1:0> "):
            first.append(line[7:])
        if line.startswith("1.2:0> "):
            second.append(line[7:])

    expected = "1234567890" * 80_000
    assert "".join(first) == expected
    assert "".join(second) == expected

    result = hq_env.command(["read", "mylog", "cat", "1", "stdout"])
    assert result == 2 * (expected + "\n")


def test_stream_stderr(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--",
            "python3",
            "-c",
            "import sys; print('Ok'); print('Error', file=sys.stderr)",
        ]
    )
    hq_env.start_workers(2)
    wait_for_job_state(hq_env, 1, "FINISHED")

    result = hq_env.command(["read", "mylog", "show"], as_lines=True)
    assert set(result[0:2]) == {"1.0:0> Ok", "1.0:1> Error"}

    result = hq_env.command(["read", "mylog", "show", "--channel=stdout"], as_lines=True)
    assert result[0] == "1.0:0> Ok"

    result = hq_env.command(["read", "mylog", "show", "--channel=stderr"], as_lines=True)
    assert result[0] == "1.0:1> Error"

    result = hq_env.command(["read", "mylog", "cat", "1", "stdout"])
    assert result == "Ok\n"

    result = hq_env.command(["read", "mylog", "cat", "1", "stderr"])
    assert result == "Error\n"


def test_stream_restart(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.start_workers(1)

    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--",
            "bash",
            "-c",
            "echo Start; sleep 2; echo End ${HQ_INSTANCE_ID}",
        ]
    )
    time.sleep(1.0)

    hq_env.kill_worker(1)
    hq_env.start_workers(1)

    wait_for_job_state(hq_env, 1, "FINISHED")

    result = hq_env.command(["read", "mylog", "cat", "1", "stdout"])
    assert result == "Start\nEnd 1\n"

    table = hq_env.command(["read", "mylog", "summary"], as_table=True)
    print(table)
    assert table[1] == ["Files", "2"]
    assert table[2] == ["Jobs", "1"]
    assert table[3] == ["Tasks", "1"]
    assert table[4] == ["Opened streams", "0"]
    assert table[5] == ["Stdout/stderr size", "12 B / 0 B"]
    assert table[6] == ["Superseded streams", "1"]
    assert table[7] == ["Superseded stdout/stderr size", "6 B / 0 B"]

    result = hq_env.command(["read", "mylog", "show"])
    assert result == "1.0:0> Start\n1.0:0> End 1\n"


def test_stream_partial(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--stdout=task-%{JOB_ID}-%{TASK_ID}-%{INSTANCE_ID}.out",
            "--",
            "python3",
            "-c",
            "import sys; print('Ok'); print('Error', file=sys.stderr)",
        ]
    )
    hq_env.start_workers(2)
    wait_for_job_state(hq_env, 1, "FINISHED")

    result = hq_env.command(["read", "mylog", "show"], as_lines=True)
    assert result[0] == "1.0:1> Error"

    check_file_contents("task-1-0-0.out", "Ok\n")


def test_stream_unfinished_small(hq_env: HqEnv):
    # Test if valid header is created even the rest of log is still in buffer
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--",
            "bash",
            "-c",
            "echo Start; sleep 2; echo End",
        ]
    )
    hq_env.start_workers(2)
    wait_for_job_state(hq_env, 1, "RUNNING")
    hq_env.command(["read", "mylog", "cat", "1", "stdout", "--allow-unfinished"], expect_fail="Job 1 not found")
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_stream_unfinished_large(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--",
            "python3",
            "-c",
            (
                "import time; import sys; sys.stdout.write('ab' * (16 * 1024 + 3));"
                "sys.stdout.flush(); time.sleep(2); print('end')"
            ),
        ]
    )
    hq_env.start_workers(1)
    wait_for_job_state(hq_env, 1, "RUNNING")
    time.sleep(1.0)
    hq_env.command(
        ["read", "mylog", "cat", "1", "stdout"],
        expect_fail="Stream for task 0 is not finished",
    )
    result = hq_env.command(["read", "mylog", "cat", "1", "stdout", "--allow-unfinished"])
    assert ("ab" * (16 * 1024 + 3)).startswith(result)
    assert len(result) >= 8 * 1024

    wait_for_job_state(hq_env, 1, "FINISHED")
    result = hq_env.command(["read", "mylog", "cat", "1", "stdout"])
    assert "ab" * (16 * 1024 + 3) + "end\n" == result


def test_stream_task_fail(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.start_workers(1)

    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--",
            "bash",
            "-c",
            "echo Start; exit 1",
        ]
    )

    wait_for_job_state(hq_env, 1, "FAILED")
    wait_until(lambda: hq_env.command(["read", "mylog", "show"]) == "1.0:0> Start\n")


def test_stream_task_cancel(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.start_workers(1)

    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--",
            "bash",
            "-c",
            "echo Start; sleep 2; echo End",
        ]
    )

    wait_for_job_state(hq_env, 1, "RUNNING")
    # Allow the program to write "Start"
    time.sleep(0.5)

    hq_env.command(["job", "cancel", "1"])
    wait_for_job_state(hq_env, 1, "CANCELED")
    wait_until(lambda: hq_env.command(["read", "mylog", "show"]) == "1.0:0> Start\n")


def test_stream_timeout(hq_env: HqEnv):
    hq_env.start_server()
    os.mkdir("mylog")
    hq_env.start_workers(1, args=["--heartbeat", "500ms"])

    hq_env.command(
        [
            "submit",
            "--stream",
            "mylog",
            "--time-limit=1s",
            "--",
            "bash",
            "-c",
            "echo Start; sleep 4; echo End",
        ]
    )

    wait_for_job_state(hq_env, 1, "FAILED")

    wait_until(lambda: hq_env.command(["read", "mylog", "show"]) == "1.0:0> Start\n")

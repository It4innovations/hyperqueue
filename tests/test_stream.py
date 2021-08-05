import time

from .conftest import HqEnv
from .utils import wait_for_job_state


def check_no_stream_connections(hq_env: HqEnv):
    table = hq_env.command(["server", "info", "--stats"], as_table=True)
    table.check_value_row("Stream connections", "")
    table.check_value_row("Stream registrations", "")


def test_stream_submit(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--log",
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

    lines = set(hq_env.command(["log", "mylog", "show"], as_lines=True))
    for i in range(1, 21):
        assert "{0:02}:0> Hello from {0}".format(i) in lines
        assert "{0:02}: > stream closed".format(i) in lines
    assert len(lines) == 40

    result = hq_env.command(["log", "mylog", "show", "--channel=stderr"])
    assert result == ""

    lines = set(
        hq_env.command(
            ["log", "mylog", "show", "--channel=stderr", "--show-empty"], as_lines=True
        )
    )
    for i in range(1, 21):
        assert "{0:02}: > stream closed".format(i) in lines

    table = hq_env.command(["log", "mylog", "summary"], as_table=True)
    table.check_value_row("Tasks", "20")
    table.check_value_row("Opened streams", "0")
    table.check_value_row("Stdout/stderr size", "271 B / 0 B")
    table.check_value_row("Superseded streams", "0")
    table.check_value_row("Superseded stdout/stderr size", "0 B / 0 B")

    result = hq_env.command(["log", "mylog", "cat", "stdout"])
    assert result == "".join(["Hello from {}\n".format(i) for i in range(1, 21)])

    result = hq_env.command(["log", "mylog", "cat", "stdout", "--task=3"])
    assert result == "Hello from 3\n"

    check_no_stream_connections(hq_env)


def test_stream_overlap(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--log",
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
    result = hq_env.command(["log", "mylog", "show"], as_lines=True)

    chunks = [set(result[i * 2 : i * 2 + 2]) for i in range(3)]
    assert chunks[0] == {"1:0> A", "2:0> A"}
    assert chunks[1] == {"2:0> B", "1:0> B"}
    assert chunks[2] == {"1: > stream closed", "2: > stream closed"}

    result = hq_env.command(["log", "mylog", "cat", "stdout"])
    assert result == "A\nB\nA\nB\n"
    check_no_stream_connections(hq_env)


def test_stream_big_output(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--log",
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

    result = hq_env.command(["log", "mylog", "show"], as_lines=True)
    # print(result)
    first = []
    second = []
    for line in result:
        if line.startswith("1:0> "):
            first.append(line[5:])
        if line.startswith("1:0> "):
            second.append(line[5:])

    expected = "1234567890" * 80_000
    assert "".join(first) == expected
    assert "".join(second) == expected

    result = hq_env.command(["log", "mylog", "cat", "stdout"])
    assert result == 2 * (expected + "\n")
    check_no_stream_connections(hq_env)


def test_stream_stderr(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--log",
            "mylog",
            "--",
            "python3",
            "-c",
            "import sys; print('Ok'); print('Error', file=sys.stderr)",
        ]
    )
    hq_env.start_workers(2)
    wait_for_job_state(hq_env, 1, "FINISHED")

    result = hq_env.command(["log", "mylog", "show"], as_lines=True)
    assert result[0] == "0:0> Ok"
    assert result[1] == "0:1> Error"

    result = hq_env.command(["log", "mylog", "show", "--channel=stdout"], as_lines=True)
    assert result[0] == "0:0> Ok"
    assert result[1] == "0: > stream closed"

    result = hq_env.command(["log", "mylog", "show", "--channel=stderr"], as_lines=True)
    assert result[0] == "0:1> Error"
    assert result[1] == "0: > stream closed"

    result = hq_env.command(["log", "mylog", "cat", "stdout"])
    assert result == "Ok\n"

    result = hq_env.command(["log", "mylog", "cat", "stderr"])
    assert result == "Error\n"
    check_no_stream_connections(hq_env)


def test_stream_stats(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--log", "mylog", "--array=1-4", "--", "sleep", "2"])

    table = hq_env.command(["server", "info", "--stats"], as_table=True)
    table.check_value_row("Stream connections", "")
    assert table.get_row_value("Stream registrations").startswith("1:")

    hq_env.start_workers(2, cpus="2")
    time.sleep(1)

    table = hq_env.command(["server", "info", "--stats"], as_table=True)
    assert table.get_row_value("Stream registrations").startswith("1:")
    assert 2 == len(table.get_row_value("Stream connections").split("\n"))
    time.sleep(1.2)
    check_no_stream_connections(hq_env)


def test_stream_restart(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_workers(1)

    hq_env.command(
        [
            "submit",
            "--log",
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

    result = hq_env.command(["log", "mylog", "cat", "stdout"])
    assert result == "Start\nEnd 1\n"

    table = hq_env.command(["log", "mylog", "summary"], as_table=True)
    assert table[1] == ["Tasks", "1"]
    assert table[2] == ["Opened streams", "0"]
    assert table[3] == ["Stdout/stderr size", "12 B / 0 B"]
    assert table[4] == ["Superseded streams", "1"]
    assert table[5] == ["Superseded stdout/stderr size", "6 B / 0 B"]

    result = hq_env.command(["log", "mylog", "show"])
    assert result == "0:0> Start\n0:0> End 1\n0: > stream closed\n"

    check_no_stream_connections(hq_env)


def test_stream_partial(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--log",
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

    result = hq_env.command(["log", "mylog", "show"], as_lines=True)
    assert result[0] == "0:1> Error"
    assert result[1] == "0: > stream closed"

    check_no_stream_connections(hq_env)

    with open("task-1-0-0.out") as f:
        assert f.read() == "Ok\n"


def test_stream_unfinished_small(hq_env: HqEnv):
    # Test if valid header is created even the rest of log is still in buffer
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--log",
            "mylog",
            "--",
            "bash",
            "-c",
            "echo Start; sleep 2; echo End",
        ]
    )
    hq_env.start_workers(2)
    wait_for_job_state(hq_env, 1, "RUNNING")
    hq_env.command(["log", "mylog", "cat", "stdout", "--allow-unfinished"])
    wait_for_job_state(hq_env, 1, "FINISHED")
    check_no_stream_connections(hq_env)


def test_stream_unfinished_large(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--log",
            "mylog",
            "--",
            "python3",
            "-c",
            "import time; import sys; sys.stdout.write('ab' * (16 * 1024 + 3)); sys.stdout.flush(); time.sleep(2); print('end')",
        ]
    )
    hq_env.start_workers(1)
    wait_for_job_state(hq_env, 1, "RUNNING")
    time.sleep(1.0)
    hq_env.command(
        ["log", "mylog", "cat", "stdout"],
        expect_fail="Stream for task 0 is not finished",
    )
    result = hq_env.command(["log", "mylog", "cat", "stdout", "--allow-unfinished"])
    assert ("ab" * (16 * 1024 + 3)).startswith(result)
    assert len(result) >= 8 * 1024

    wait_for_job_state(hq_env, 1, "FINISHED")
    check_no_stream_connections(hq_env)
    result = hq_env.command(["log", "mylog", "cat", "stdout"])
    assert "ab" * (16 * 1024 + 3) + "end\n" == result

import os
import time

import pytest
from tako.client import TaskFailed
from tako.client.program import ProgramDefinition
from tako.client.task import make_program_task


def test_server_without_workers(tako_env):
    session = tako_env.start()
    assert len(session.overview()["worker_overviews"]) == 0


def test_submit_simple_task_ok(tako_env):
    session = tako_env.start(workers=[1])

    stdout = os.path.join(tako_env.work_dir, "test.out")
    stderr = os.path.join(tako_env.work_dir, "test.err")

    t1 = make_program_task(ProgramDefinition(["/bin/hostname"]), keep=True)
    t2 = make_program_task(ProgramDefinition(["/bin/hostname"]))
    t3 = make_program_task(
        ProgramDefinition(["bash", "-c", "echo 'hello'"], stdout=stdout, stderr=stderr),
        keep=True,
    )

    assert t1._id is None
    assert t2._id is None
    assert t3._id is None

    session.submit([t1, t2, t3])

    assert isinstance(t1._id, int)
    assert isinstance(t2._id, int)
    assert isinstance(t3._id, int)

    session.wait_all([t1, t2, t3])

    assert os.path.isfile(stdout)
    assert os.path.isfile(stderr)

    with open(stdout) as f:
        assert f.read() == "hello\n"

    session.wait(t1)
    session.wait(t1)

    session.wait(t2)

    print(session.overview())


def test_submit_simple_task_fail(tako_env):
    session = tako_env.start(workers=[1])

    t1 = make_program_task(ProgramDefinition(["/usr/bin/nonsense"]))
    session.submit([t1])
    with pytest.raises(TaskFailed):
        session.wait(t1)

    # TODO: Bash that aborts
    t1 = make_program_task(ProgramDefinition(["bash", "-c" "'exit 3'"]))
    session.submit([t1])
    with pytest.raises(TaskFailed):
        session.wait(t1)

    # Make sure that program may still run
    t2 = make_program_task(ProgramDefinition(["/bin/hostname"]))
    session.submit([t2])
    session.wait(t2)


def test_cancel_immediately(tako_env):
    session = tako_env.start(workers=[1])
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]))
    session.submit([t1])
    start = time.time()
    session.cancel(t1)
    with pytest.raises(TaskFailed):
        session.wait(t1)
    end = time.time()
    assert (end - start) <= 0.2
    time.sleep(0.3)  # Give some time to worker to fail


def test_cancel_prev(tako_env):
    session = tako_env.start(workers=[1])
    ts = [make_program_task(ProgramDefinition(["sleep", "1"])) for _ in range(100)]
    session.submit(ts)
    session.cancel_all(ts[:72] + ts[73:])

    start = time.time()
    session.wait(ts[72])
    end = time.time()
    assert 1.0 <= (end - start) <= 1.2
    time.sleep(0.2)  # Give some time to worker to fail


def test_cancel_error_task(tako_env):
    session = tako_env.start(workers=[1])
    t1 = make_program_task(ProgramDefinition(["/nonsense", "1"]))
    session.submit([t1])
    time.sleep(0.3)
    session.cancel(t1)
    with pytest.raises(TaskFailed):
        session.wait(t1)


def test_task_time_limit_fail(tako_env):
    session = tako_env.start(workers=[1])
    t1 = make_program_task(ProgramDefinition(["sleep", "2"]), time_limit=0.6)
    session.submit([t1])
    with pytest.raises(TaskFailed, match="Time limit reached"):
        session.wait(t1)


def test_task_time_limit_pass(tako_env):
    session = tako_env.start(workers=[1])
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]), time_limit=1.6)
    session.submit([t1])
    session.wait(t1)
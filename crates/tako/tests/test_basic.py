from tako.client.program import ProgramDefinition
from tako.client.task import make_program_task
from tako.client import TaskFailed


import os
import pytest
import time


def test_server_without_workers(tako_env):
    tako_env.start()


def test_submit_simple_task_ok(tako_env):
    session = tako_env.start(workers=[1])

    stdout = os.path.join(tako_env.work_dir, "test.out")
    stderr = os.path.join(tako_env.work_dir, "test.err")

    t1 = make_program_task(ProgramDefinition(["/usr/bin/hostname"]), keep=True)
    t2 = make_program_task(ProgramDefinition(["/usr/bin/hostname"]))
    t3 = make_program_task(ProgramDefinition(["bash", "-c", "echo 'hello'"], stdout=stdout, stderr=stderr), keep=True)

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
    t2 = make_program_task(ProgramDefinition(["/usr/bin/hostname"]))
    session.submit([t2])
    session.wait(t2)


def test_submit_2_sleeps_on_1(tako_env):
    session = tako_env.start(workers=[1])
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]))
    t2 = make_program_task(ProgramDefinition(["sleep", "1"]))

    start = time.time()
    session.submit([t1, t2])
    session.wait_all([t1, t2])
    end = time.time()
    assert 2.0 <= (end - start) <= 2.3


def test_submit_2_sleeps_on_2(tako_env):
    session = tako_env.start(workers=[2])
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]))
    t2 = make_program_task(ProgramDefinition(["sleep", "1"]))

    start = time.time()
    session.submit([t1, t2])
    session.wait_all([t1, t2])
    end = time.time()
    assert 1.0 <= (end - start) <= 1.3

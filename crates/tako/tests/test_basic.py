import os
import time

import pytest
from tako.client import TaskFailed
from tako.client.program import ProgramDefinition
from tako.client.task import make_program_task


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

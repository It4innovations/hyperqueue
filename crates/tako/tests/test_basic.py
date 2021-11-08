import pytest
from tako.client import TaskFailed
from tako.client.program import ProgramDefinition
from tako.client.task import make_program_task


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

from tako.client.program import ProgramDefinition
from tako.client.task import make_program_task
from tako.client import TaskFailed


import os
import pytest
import time


def test_server_without_workers(tako_env):
    session = tako_env.start()
    assert len(session.overview()["workers"]) == 0


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
    t2 = make_program_task(ProgramDefinition(["/usr/bin/hostname"]))
    session.submit([t2])
    session.wait(t2)


def test_submit_2_sleeps_on_1(tako_env):
    session = tako_env.start(workers=[1])
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]))
    t2 = make_program_task(ProgramDefinition(["sleep", "1"]))
    start = time.time()
    session.submit([t1, t2])

    time.sleep(0.2)
    overview1 = session.overview()
    assert len(overview1["workers"][0]["running_tasks"]) == 1

    time.sleep(0.5)
    overview2 = session.overview()
    assert len(overview2["workers"][0]["running_tasks"]) == 1
    assert overview1["workers"][0]["running_tasks"] == overview2["workers"][0]["running_tasks"]

    time.sleep(0.5)
    overview2 = session.overview()
    assert len(overview2["workers"][0]["running_tasks"]) == 1
    assert overview1["workers"][0]["running_tasks"] != overview2["workers"][0]["running_tasks"]

    session.wait_all([t1, t2])
    end = time.time()
    assert 2.0 <= (end - start) <= 2.3


def test_submit_2_sleeps_on_2(tako_env):
    session = tako_env.start(workers=[2])
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]))
    t2 = make_program_task(ProgramDefinition(["sleep", "1"]))

    start = time.time()
    session.submit([t1, t2])

    time.sleep(0.2)
    overview = session.overview()
    assert len(overview["workers"][0]["running_tasks"]) == 2

    session.wait_all([t1, t2])
    end = time.time()
    assert 1.0 <= (end - start) <= 1.3


def test_submit_2_sleeps_on_separated_2(tako_env):
    session = tako_env.start(workers=[1, 1, 1])
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]))
    t2 = make_program_task(ProgramDefinition(["sleep", "1"]))

    start = time.time()
    session.submit([t1, t2])

    time.sleep(0.2)
    overview = session.overview()

    empty = None

    for w in overview["workers"]:
        if len(w["running_tasks"]) == 0:
            empty = w["id"]
            break
    else:
        assert 0

    for w in overview["workers"]:
        if w["id"] != empty:
            assert len(w["running_tasks"]) == 1

    session.wait_all([t1, t2])
    end = time.time()
    assert 1.0 <= (end - start) <= 1.3

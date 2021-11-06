import time

from tako.client.program import ProgramDefinition
from tako.client.task import make_program_task


def test_lost_worker_with_tasks_continue(tako_env):
    # We use delay to ensure that worker_id matches to kill the right one
    session = tako_env.start(
        workers=[1, 1], worker_start_delay=0.3, panic_on_worker_lost=False
    )

    t1 = make_program_task(ProgramDefinition(["sleep", "1"]))

    start = time.time()
    session.submit([t1])
    time.sleep(0.4)
    overview = session.overview()
    for w in overview["worker_overviews"]:
        if len(w["running_tasks"]) == 1:
            break
    else:
        assert 0
    tako_env.kill_worker(w["id"] - 1)

    session.wait(t1)

    overview = session.overview()
    assert len(overview["worker_overviews"]) == 1

    end = time.time()
    assert 1.4 <= end - start <= 1.8


def test_lost_worker_with_tasks_restarts(tako_env):
    # We use delay to ensure that worker_id matches to kill the right one
    session = tako_env.start(workers=[], panic_on_worker_lost=False)

    t1 = make_program_task(ProgramDefinition(["sleep", "1"]))

    session.submit([t1])

    for i in range(5):
        tako_env.start_worker(ncpus=1)
        time.sleep(0.3)
        tako_env.kill_worker(i)

    tako_env.start_worker(ncpus=1)

    start = time.time()
    session.wait(t1)
    end = time.time()

    assert 1.0 <= end - start <= 1.2

    overview = session.overview()
    assert len(overview["worker_overviews"]) == 1
    assert overview["worker_overviews"][0]["id"] == 6


def test_frozen_worker1(tako_env):
    session = tako_env.start(
        workers=[1], worker_start_delay=0.4, panic_on_worker_lost=False, heartbeat=500
    )
    time.sleep(0.5)

    overview = session.overview()
    assert len(overview["worker_overviews"]) == 1

    tako_env.pause_worker(0)

    time.sleep(1.5)

    overview = session.overview()
    assert len(overview["worker_overviews"]) == 0


def test_frozen_worker2(tako_env):
    session = tako_env.start(
        workers=[1], worker_start_delay=0.4, panic_on_worker_lost=False, heartbeat=500
    )
    overview = session.overview()
    assert len(overview["worker_overviews"]) == 1

    tako_env.pause_worker(0)
    start = time.time()
    overview = session.overview()
    assert len(overview["worker_overviews"]) == 0
    end = time.time()
    assert 0.5 < (end - start) < 1.5


def test_worker_idle_timeout_no_tasks(tako_env):
    session = tako_env.start(
        workers=[1],
        worker_start_delay=0.4,
        panic_on_worker_lost=False,
        heartbeat=500,
        idle_timeout=1,
    )
    overview = session.overview()
    assert len(overview["worker_overviews"]) == 1
    time.sleep(2)
    overview = session.overview()
    assert len(overview["worker_overviews"]) == 0

    tako_env.expect_worker_exit(0)


def test_worker_idle_timeout_tasks(tako_env):
    session = tako_env.start(
        workers=[1],
        worker_start_delay=0.4,
        panic_on_worker_lost=False,
        heartbeat=500,
        idle_timeout=1,
    )
    t1 = make_program_task(ProgramDefinition(["sleep", "2"]))
    session.submit([t1])

    overview = session.overview()
    assert len(overview["worker_overviews"]) == 1
    time.sleep(2)
    overview = session.overview()
    assert len(overview["worker_overviews"]) == 1
    time.sleep(2.5)
    overview = session.overview()
    assert len(overview["worker_overviews"]) == 0
    tako_env.expect_worker_exit(0)

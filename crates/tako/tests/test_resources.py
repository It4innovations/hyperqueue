import time

from tako.client.program import ProgramDefinition
from tako.client.resources import ResourceRequest, AllocationPolicy
from tako.client.task import make_program_task


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
    assert (
        overview1["workers"][0]["running_tasks"]
        == overview2["workers"][0]["running_tasks"]
    )

    time.sleep(0.5)
    overview2 = session.overview()
    assert len(overview2["workers"][0]["running_tasks"]) == 1
    assert (
        overview1["workers"][0]["running_tasks"]
        != overview2["workers"][0]["running_tasks"]
    )

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


def test_submit_sleeps_more_cpus1(tako_env):
    session = tako_env.start(workers=[4, 4])
    rq1 = ResourceRequest(cpus=3)
    rq2 = ResourceRequest(cpus=2)
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq1)
    t2 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq2)
    t3 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq2)
    # t4 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq1)

    start = time.time()
    session.submit([t1, t2, t3])

    time.sleep(0.2)
    overview = session.overview()
    print([overview["workers"]])
    # print([w["running_tasks"] for w in overview["workers"]])
    rts = [len(w["running_tasks"]) for w in overview["workers"]]
    rts.sort()
    assert rts == [1, 2]
    session.wait_all([t1, t2, t3])
    end = time.time()
    assert 1.0 <= (end - start) <= 1.3


def test_submit_sleeps_more_cpus2(tako_env):
    session = tako_env.start(workers=[4, 4])
    rq1 = ResourceRequest(cpus=3)
    rq2 = ResourceRequest(cpus=2)
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq1)
    t2 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq2)
    t3 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq2)
    t4 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq1)

    start = time.time()
    session.submit([t1, t2, t3, t4])
    session.wait_all([t1, t2, t3, t4])
    end = time.time()
    assert 2.0 <= (end - start) <= 2.3


def test_submit_sleeps_more_cpus3(tako_env):
    session = tako_env.start()
    rq1 = ResourceRequest(cpus=3)
    rq2 = ResourceRequest(cpus=2)
    t1 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq1)
    t2 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq2)
    t3 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq2)
    t4 = make_program_task(ProgramDefinition(["sleep", "1"]), resources=rq1)

    tako_env.start_worker(5)
    tako_env.start_worker(5)

    start = time.time()
    session.submit([t1, t2, t3, t4])
    session.wait_all([t1, t2, t3, t4])
    end = time.time()
    assert 1.0 <= (end - start) <= 1.3


def test_force_compact(tako_env):
    session = tako_env.start()
    tako_env.start_worker(ncpus=2, nsockets=2)
    rq1 = ResourceRequest(cpus=4, policy=AllocationPolicy.ForceCompact)
    t1 = make_program_task(ProgramDefinition(["hostname"]), resources=rq1)
    session.submit([t1])
    session.wait_all([t1])
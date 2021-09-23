import time


def test_worker_timeout(tako_env):

    session = tako_env.start(workers=[1], time_limit=1, panic_on_worker_lost=False)
    ov = session.overview()
    assert len(ov["worker_overviews"]) == 1
    time.sleep(1.3)
    ov = session.overview()
    tako_env.expect_worker_exit(0)
    assert len(ov["worker_overviews"]) == 0

import multiprocessing
import time


def test_hw_monitoring(tako_env):
    session = tako_env.start(
        workers=[1],
        worker_start_delay=0.4,
        panic_on_worker_lost=False,
        heartbeat=500,
        hw_interval=100,
    )
    time.sleep(0.2)

    overview = session.overview()
    cpu_percent_vec = overview["worker_overviews"][0]["hw_state"]["state"][
        "worker_cpu_usage"
    ]["cpu_per_core_percent_usage"]

    cpu_count = multiprocessing.cpu_count()
    assert cpu_count == len(cpu_percent_vec)

    for cpu_percent in cpu_percent_vec:
        assert 0.00 <= cpu_percent <= 100.00

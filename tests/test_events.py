import json
import time
from typing import List

from schema import Schema
from utils import wait_for_worker_state

from .conftest import HqEnv


def test_worker_connected_event(hq_env: HqEnv):
    def body():
        hq_env.start_worker()

    events = get_events(hq_env, body)
    assert find_events(events, "worker-connected")[0]["id"] == 1


def test_worker_lost_event(hq_env: HqEnv):
    def body():
        hq_env.start_worker()
        hq_env.command(["worker", "stop", "1"])

    events = get_events(hq_env, body)
    event = find_events(events, "worker-lost")[0]
    assert event["id"] == 1
    assert event["reason"] == "Stopped"


def test_worker_send_overview(hq_env: HqEnv):
    def body():
        hq_env.start_worker(args=["--overview-interval", "10ms"])
        wait_for_worker_state(hq_env, 1, "RUNNING")
        time.sleep(0.2)
        hq_env.command(["worker", "stop", "1"])

    events = get_events(hq_env, body)
    event = find_events(events, "worker-overview")[0]
    hw_state = event["hw-state"]["state"]
    schema = Schema(
        {
            "timestamp": int,
            "cpu_usage": {"cpu_per_core_percent_usage": [float]},
            "memory_usage": {"free": int, "total": int},
            "network_usage": {
                "rx_bytes": int,
                "rx_errors": int,
                "rx_packets": int,
                "tx_bytes": int,
                "tx_errors": int,
                "tx_packets": int,
            },
        }
    )
    schema.validate(hw_state)


def test_worker_disable_overview(hq_env: HqEnv):
    def body():
        hq_env.start_worker(args=["--overview-interval", "0s"])
        wait_for_worker_state(hq_env, 1, "RUNNING")
        time.sleep(0.2)
        hq_env.command(["worker", "stop", "1"])

    events = get_events(hq_env, body)
    events = find_events(events, "worker-overview")
    assert len(events) == 0


def find_events(events, type: str) -> List:
    return [e["event"] for e in events if e["event"]["type"] == type]


def get_events(hq_env: HqEnv, callback):
    log_path = "events.log"
    process = hq_env.start_server(args=["--event-log-path", log_path])
    callback()
    hq_env.command(["server", "stop"])
    process.wait(timeout=5)
    hq_env.processes.clear()

    output = hq_env.command(["event-log", "export", log_path], ignore_stderr=True)
    events = []
    for line in output.splitlines(keepends=False):
        events.append(json.loads(line))
    return events

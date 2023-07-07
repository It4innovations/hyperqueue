import json
import time
from typing import List

from schema import Schema

from .conftest import HqEnv
from .utils import wait_for_worker_state


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
        },
        ignore_extra_keys=True,
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


def test_worker_capture_nvidia_gpu_state(hq_env: HqEnv):
    def body():
        with hq_env.mock.mock_program_with_code(
            "nvidia-smi",
            """
print("BUS1, 10.0 %, 100 MiB, 200 MiB")
""",
        ):
            hq_env.start_worker(args=["--overview-interval", "10ms", "--resource", "gpus/nvidia=[0]"])
            wait_for_worker_state(hq_env, 1, "RUNNING")
            time.sleep(0.2)
            hq_env.command(["worker", "stop", "1"])

    events = get_events(hq_env, body)
    event = find_events(events, "worker-overview")[0]
    event = event["hw-state"]["state"]["nvidia_gpus"]
    schema = Schema(
        {
            "gpus": [
                {
                    "id": "BUS1",
                    "processor_usage": 10.0,
                    "mem_usage": 50.0,
                }
            ]
        }
    )
    schema.validate(event)


def test_worker_capture_amd_gpu_state(hq_env: HqEnv):
    def body():
        with hq_env.mock.mock_program_with_code(
            "rocm-smi",
            """
import json

data = {
    "card0": {
        "GPU use (%)": "1.5",
        "GPU memory use (%)": "12.5",
        "PCI Bus": "FOOBAR1"
    },
    "card1": {
        "GPU use (%)": "12.5",
        "GPU memory use (%)": "64.0",
        "PCI Bus": "FOOBAR2"
    }
}
print(json.dumps(data))
""",
        ):
            hq_env.start_worker(args=["--overview-interval", "10ms", "--resource", "gpus/amd=[0]"])
            wait_for_worker_state(hq_env, 1, "RUNNING")
            time.sleep(0.2)
            hq_env.command(["worker", "stop", "1"])

    events = get_events(hq_env, body)
    event = find_events(events, "worker-overview")[0]
    event = event["hw-state"]["state"]["amd_gpus"]
    schema = Schema(
        {
            "gpus": [
                {
                    "id": "FOOBAR1",
                    "processor_usage": 1.5,
                    "mem_usage": 12.5,
                },
                {
                    "id": "FOOBAR2",
                    "processor_usage": 12.5,
                    "mem_usage": 64.0,
                },
            ]
        }
    )
    schema.validate(event)


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

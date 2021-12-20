import datetime
import json
import socket
from typing import List

import iso8601
from schema import Schema

from ..conftest import HqEnv
from ..utils import wait_for_job_state


def parse_json_output(hq_env: HqEnv, command: List[str]):
    return json.loads(hq_env.command(command))


def test_print_worker_list(hq_env: HqEnv):
    hq_env.start_server()

    for i in range(5):
        hq_env.start_worker()

    output = parse_json_output(hq_env, ["--output-mode=json", "worker", "list"])
    assert len(output) == 5


def test_print_worker_info(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    output = parse_json_output(hq_env, ["--output-mode=json", "worker", "info", "1"])

    schema = Schema(
        {
            "configuration": {
                "heartbeat_interval": 8.0,
                "hostname": "worker1",
                "idle_timeout": None,
                "listen_address": str,
                "log_dir": str,
                "resources": {"cpus": [[0]], "generic": []},
                "time_limit": None,
                "work_dir": str,
            },
            "ended": None,
            "id": 1,
        }
    )
    schema.validate(output)


def test_print_server_record(hq_env: HqEnv):
    process = hq_env.start_server()
    output = parse_json_output(hq_env, ["--output-mode=json", "server", "info"])

    schema = Schema(
        {
            "host": socket.gethostname(),
            "hq_port": int,
            "pid": process.pid,
            "server_dir": hq_env.server_dir,
            "start_date": str,
            "version": str,
            "worker_port": int,
        }
    )
    schema.validate(output)

    time = iso8601.parse_date(output["start_date"])
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    duration = now - time
    assert abs(duration).total_seconds() > 0

    assert 0 < int(output["hq_port"]) < 65536
    assert 0 < int(output["worker_port"]) < 65536


def test_print_job_list(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "echo", "tt"])
    wait_for_job_state(hq_env, 1, "FINISHED")
    output = parse_json_output(hq_env, ["--output-mode=json", "jobs"])

    schema = Schema(
        [
            {
                "id": 1,
                "name": "echo",
                "resources": {
                    "cpus": {"cpus": 1, "type": "compact"},
                    "generic": [],
                    "min_time": 0.0,
                },
                "task_count": 1,
                "task_stats": {
                    "canceled": 0,
                    "failed": 0,
                    "finished": 1,
                    "running": 0,
                    "waiting": 0,
                },
            }
        ]
    )
    schema.validate(output)


def test_print_job_detail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "echo", "tt"])
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "1"])

    schema = Schema(
        {
            "info": {
                "id": 1,
                "name": "echo",
                "resources": dict,
                "task_count": 1,
                "task_stats": dict,
            },
            "finished_at": None,
            "max_fails": None,
            "pin": False,
            "priority": 0,
            "program": {
                "args": ["echo", "tt"],
                "env": {},
                "cwd": str,
                "stdout": dict,
                "stderr": dict,
            },
            "started_at": str,
            "time_limit": None,
        }
    )
    schema.validate(output)


def test_print_job_with_tasks(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "echo", "tt", "--array=1-4"])
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "1", "--tasks"])

    schema = Schema([{"id": id, "state": "waiting"} for id in range(1, 5)])
    schema.validate(output["tasks"])


def test_print_hw(hq_env: HqEnv):
    hq_env.start_server()
    output = parse_json_output(hq_env, ["--output-mode=json", "worker", "hwdetect"])

    schema = Schema({"cpus": [[int]], "generic": list})
    schema.validate(output)

import datetime
import os
import socket
from typing import List

import iso8601
from schema import Schema

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.job import default_task_output


def parse_json_output(hq_env: HqEnv, command: List[str]):
    return hq_env.command(command, ignore_stderr=True, as_json=True)


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
                "on_server_lost": "stop",
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
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "list"])

    schema = Schema(
        [
            {
                "id": 1,
                "name": "echo",
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
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "info", "1"])

    schema = Schema(
        {
            "info": {
                "id": 1,
                "name": "echo",
                "task_count": 1,
                "task_stats": dict,
            },
            "resources": dict,
            "finished_at": None,
            "max_fails": None,
            "pin_mode": "None",
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
            "submit_dir": str,
            "tasks": list,
            "task_dir": bool,
        }
    )
    schema.validate(output)


def test_print_job_tasks_in_job_detail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--array=1-4", "echo", "tt"])
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "info", "1"])

    schema = Schema([{"id": id, "state": "waiting"} for id in range(1, 5)])
    schema.validate(output["tasks"])


def test_print_job_tasks(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--array=1-4", "echo", "tt"])
    output = parse_json_output(hq_env, ["--output-mode=json", "task", "list", "1"])

    schema = Schema({"1": [{"id": id, "state": "waiting"} for id in range(1, 5)]})
    schema.validate(output)


def test_print_task_placeholders(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "--array=1-4", "echo", "tt"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    output = parse_json_output(hq_env, ["--output-mode=json", "job", "info", "1"])

    schema = Schema(
        [{"id": id, "state": "finished"} for id in range(1, 5)], ignore_extra_keys=True
    )
    schema.validate(output["tasks"])

    tasks = sorted(output["tasks"], key=lambda t: t["id"])
    for i in range(4):
        task_id = tasks[i]["id"]
        assert tasks[i]["cwd"] == os.getcwd()
        assert tasks[i]["stdout"]["File"] == default_task_output(
            task_id=task_id, type="stdout"
        )
        assert tasks[i]["stderr"]["File"] == default_task_output(
            task_id=task_id, type="stderr"
        )


def test_print_hw(hq_env: HqEnv):
    hq_env.start_server()
    output = parse_json_output(hq_env, ["--output-mode=json", "worker", "hwdetect"])

    schema = Schema({"cpus": [[int]], "generic": list})
    schema.validate(output)

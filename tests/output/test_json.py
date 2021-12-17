import datetime
import json
import socket
from typing import List

import iso8601
from schema import Schema

from ..conftest import HqEnv


def parse_json_output(hq_env: HqEnv, command: List[str]):
    return json.loads(hq_env.command(command))


def test_print_worker_list(hq_env: HqEnv):
    hq_env.start_server()

    for i in range(5):
        hq_env.start_worker()

    output = parse_json_output(hq_env, ["--output-type=json", "worker", "list"])
    assert len(output) == 5


def test_print_worker_info(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    output = parse_json_output(hq_env, ["--output-type=json", "worker", "info", "1"])

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
    output = parse_json_output(hq_env, ["--output-type=json", "server", "info"])

    schema = Schema(
        {
            "host": socket.gethostname(),
            "hq_port": int,
            "pid": process.pid,
            "server_dir": hq_env.server_dir,
            "start_date": str,
            "version": str,
            "worker_port": int
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
    output = parse_json_output(hq_env, ["--output-type=json", "jobs"])
    assert isinstance(output, list)
    assert isinstance(output[0], dict)


def test_print_job_detail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "echo", "tt"])
    tasks, job_detail = parse_json_output(hq_env, ["--output-type=json", "job", "1"])
    assert isinstance(tasks, dict)
    assert isinstance(job_detail, dict)
    assert job_detail["job_detail"]["job_type"] == "Simple"
    assert tasks["tasks_id"] == [0]


def test_print_job_tasks(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "echo", "tt", "--array=1-10"])
    tasks, job_detail = parse_json_output(
        hq_env, ["--output-type=json", "job", "1", "--tasks"]
    )
    assert isinstance(tasks, dict)
    for key in tasks:
        if isinstance(tasks[key], list):
            assert len(tasks[key]) == 10


def test_print_hw(hq_env: HqEnv):
    hq_env.start_server()
    output = parse_json_output(hq_env, ["--output-type=json", "worker", "hwdetect"])

    schema = Schema({
        "cpus": [[int]],
        "generic": list
    })
    schema.validate(output)

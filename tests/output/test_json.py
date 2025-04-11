import datetime
import os
import socket
from typing import List

import iso8601
from schema import Schema

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.job import default_task_output

RESOURCE_DESCRIPTOR_SCHEMA = [
    {"name": str, "kind": "range", "start": int, "end": int},
    {"name": str, "kind": "sum", "size": int},
    {"name": str, "kind": "list", "values": [str]},
    {"name": str, "kind": "groups", "groups": [[str]]},
]


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
                "resources": {"resources": RESOURCE_DESCRIPTOR_SCHEMA},
                "time_limit": None,
                "work_dir": str,
                "group": str,
                "on_server_lost": "stop",
            },
            "allocation": None,
            "started": str,
            "ended": None,
            "runtime_info": {"SingleNodeTasks": {"assigned_tasks": 0, "is_reserved": False, "running_tasks": 0}},
            "last_task_started": None,
            "id": 1,
        }
    )
    schema.validate(output)


def test_print_worker_info_pbs_allocation(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"})
    output = parse_json_output(hq_env, ["--output-mode=json", "worker", "info", "1"])

    schema = Schema(
        {
            "allocation": {"manager": "PBS", "id": "x1234"},
        },
        ignore_extra_keys=True,
    )
    schema.validate(output)


def test_print_server_record(hq_env: HqEnv):
    process = hq_env.start_server()
    output = parse_json_output(hq_env, ["--output-mode=json", "server", "info"])

    schema = Schema(
        {
            "worker_host": socket.gethostname(),
            "client_host": socket.gethostname(),
            "worker_port": int,
            "server_dir": None,
            "version": str,
            "client_port": int,
            "server_uid": str,
            "start_date": str,
            "pid": int,
            "journal_path": None,
        }
    )
    schema.validate(output)

    time = iso8601.parse_date(output["start_date"])
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    duration = now - time
    assert abs(duration).total_seconds() > 0

    assert process.pid == output["pid"]

    assert 0 < int(output["client_port"]) < 65536
    assert 0 < int(output["worker_port"]) < 65536
    assert output["server_uid"].isalnum() and len(output["server_uid"]) == 6


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


ARRAY_JOB_DETAIL_SCHEMA = {
    "info": {
        "id": int,
        "name": "echo",
        "task_count": 1,
        "task_stats": dict,
        "is_open": bool,
    },
    "submits": [
        {
            "array": {
                "resources": list,
                "priority": 0,
                "program": {
                    "args": ["echo", "tt"],
                    "env": {},
                    "cwd": str,
                    "stdout": str,
                    "stderr": str,
                },
                "pin_mode": None,
                "crash_limit": int,
                "task_dir": bool,
                "time_limit": None,
            }
        }
    ],
    "finished_at": None,
    "max_fails": None,
    "started_at": str,
    "tasks": list,
}


def test_print_job_detail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "echo", "tt"])
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "info", "1"])
    schema = Schema([ARRAY_JOB_DETAIL_SCHEMA])
    schema.validate(output)


def test_print_job_detail_resources(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        [
            "submit",
            "--resource",
            "res1=2",
            "--resource",
            "res2=8 compact!",
            "--resource",
            "res3=all",
            "--resource",
            "res4=4 scatter",
            "echo",
            "tt",
        ]
    )
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "info", "1"])

    schema = Schema(
        {
            "resources": [
                {
                    "min_time": 0.0,
                    "n_nodes": 0,
                    "resources": [
                        {"request": {"Compact": 2 * 10_000}, "resource": "res1"},
                        {"request": {"ForceCompact": 8 * 10_000}, "resource": "res2"},
                        {"request": "All", "resource": "res3"},
                        {"request": {"Scatter": 4 * 10_000}, "resource": "res4"},
                        {"request": {"Compact": 1 * 10_000}, "resource": "cpus"},
                    ],
                }
            ]
        },
        ignore_extra_keys=True,
    )
    schema.validate(output[0]["submits"][0]["array"])


def test_print_job_detail_multiple_jobs(hq_env: HqEnv):
    hq_env.start_server()
    for _ in range(2):
        hq_env.command(["submit", "echo", "tt"])
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "info", "1,2"])

    schema = Schema([ARRAY_JOB_DETAIL_SCHEMA])
    schema.validate(output)


def test_print_job_tasks_in_job_detail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "--array=1-4", "echo", "tt"])
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "info", "1"])

    schema = Schema([{"id": id, "state": "waiting"} for id in range(1, 5)])
    schema.validate(output[0]["tasks"])


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

    schema = Schema([{"id": id, "state": "finished"} for id in range(1, 5)], ignore_extra_keys=True)
    schema.validate(output[0]["tasks"])

    tasks = sorted(output[0]["tasks"], key=lambda t: t["id"])
    for i in range(4):
        task_id = tasks[i]["id"]
        assert tasks[i]["cwd"] == os.getcwd()
        assert tasks[i]["stdout"] == default_task_output(task_id=task_id, type="stdout")
        assert tasks[i]["stderr"] == default_task_output(task_id=task_id, type="stderr")


def test_print_hw(hq_env: HqEnv):
    hq_env.start_server()
    output = parse_json_output(hq_env, ["--output-mode=json", "worker", "hwdetect"])

    schema = Schema({"resources": RESOURCE_DESCRIPTOR_SCHEMA})
    schema.validate(output)


def test_print_job_summary(hq_env: HqEnv):
    hq_env.start_server()
    output = parse_json_output(hq_env, ["--output-mode=json", "job", "summary"])
    schema = Schema(
        {
            "Canceled": 0,
            "Failed": 0,
            "Finished": 0,
            "Running": 0,
            "Waiting": 0,
        }
    )
    schema.validate(output)

import datetime
import json
import os
from typing import List

import iso8601

from .conftest import HqEnv
from .utils import wait_for_job_state

"""
Json output tests
"""


def hq_json_wrapper(hq_env: HqEnv, command: List[str]):
    output = hq_env.command(command)
    output = output.split("\n")
    output = [json.loads(i) for i in output if i]
    if len(output) == 1:
        return output[0]
    return output


def test_print_worker_list(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    output = hq_json_wrapper(hq_env, ["--output-type=json", "worker", "list"])
    assert isinstance(output, list)
    assert isinstance(output[0], dict)
    assert len(output) == 1
    assert output[0]["configuration"]["hostname"] == "worker1"

    for i in range(13):
        hq_env.start_worker()

    output = hq_json_wrapper(hq_env, ["--output-type=json", "worker", "list"])
    assert len(output) == 14


def test_print_worker_info(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    output = hq_json_wrapper(hq_env, ["--output-type=json", "worker", "info", "1"])
    assert isinstance(output, dict)
    assert output["id"] == 1
    assert len(output) == 2
    assert output["worker_configuration"]["time_limit"] is None


def test_print_server_record(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    output = hq_json_wrapper(hq_env, ["--output-type=json", "server", "info"])
    assert isinstance(output, dict)
    assert output["host"] == os.uname()[1]
    assert output["server_dir"] == hq_env.server_dir
    time = iso8601.parse_date(output["start_date"])
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    duration = now - time
    assert abs(duration).total_seconds() > 0

    assert 0 < int(output["worker_port"]) < 65536


def test_print_job_list(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(["submit", "echo", "tt"])
    output = hq_json_wrapper(hq_env, ["--output-type=json", "jobs"])
    assert isinstance(output, list)
    assert isinstance(output[0], dict)


def test_print_job_detail(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "echo", "tt"])
    tasks, job_detail = hq_json_wrapper(hq_env, ["--output-type=json", "job", "1"])
    assert isinstance(tasks, dict)
    assert isinstance(job_detail, dict)
    assert job_detail["job_detail"]["job_type"] == "Simple"
    assert tasks["tasks_id"] == [0]


def test_print_job_tasks(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["submit", "echo", "tt", "--array=1-10"])
    tasks, job_detail = hq_json_wrapper(
        hq_env, ["--output-type=json", "job", "1", "--tasks"]
    )
    assert isinstance(tasks, dict)
    for key in tasks:
        if isinstance(tasks[key], list):
            assert len(tasks[key]) == 10


def test_print_hw(hq_env: HqEnv):
    hq_env.start_server()
    output = hq_json_wrapper(hq_env, ["--output-type=json", "worker", "hwdetect"])
    assert isinstance(output, dict)
    assert "cpus" in output.keys()


"""
Quiet flag tests
"""


def test_print_worker_list_quiet(hq_env: HqEnv):
    hq_env.start_server()
    for i in range(9):
        hq_env.start_worker()
    output = hq_env.command(["--output-type=quiet", "worker", "list"])
    output = output.splitlines(keepends=False)
    assert output == [f"{id + 1} RUNNING" for id in range(9)]


def test_print_job_list_quiet(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    for i in range(9):
        hq_env.command(["submit", "echo", "tt"])

    wait_for_job_state(hq_env, list(range(1, 10)), "FINISHED")

    output = hq_env.command(["--output-type=quiet", "jobs"])
    output = output.splitlines(keepends=False)
    assert output == [f"{id + 1} FINISHED" for id in range(9)]


def test_submit_quiet(hq_env: HqEnv):
    hq_env.start_server()
    output = hq_env.command(["--output-type=quiet", "submit", "echo", "tt"])
    assert output == "1\n"

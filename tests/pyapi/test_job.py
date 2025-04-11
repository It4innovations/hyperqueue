import datetime
import os.path
import random
import sys
import time
from pathlib import Path

import iso8601
import pytest
from hyperqueue.client import FailedJobsException
from hyperqueue.ffi.protocol import ResourceRequest
from hyperqueue.job import Job
from hyperqueue.output import StdioDef

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.io import check_file_contents
from ..utils.wait import wait_for_job_list_count
from ..utils.cmd import bash
from . import prepare_job_client


def test_submit_empty_job(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    with pytest.raises(Exception, match="Submitted job must have at least a single task"):
        client.submit(job)


def test_submit_simple(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(args=["uname"])
    submitted_job = client.submit(job)

    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")


def test_submit_cwd(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    cwd = Path(hq_env.server_dir) / "workdir"
    cwd.mkdir()

    job.program(args=["uname"], cwd=str(cwd))
    submitted_job = client.submit(job)

    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")

    table = hq_env.command(["task", "info", str(submitted_job.id), "0"], as_table=True)
    assert table.get_row_value("Workdir") == str(cwd)


def test_submit_env(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    job.program(
        args=bash("echo $FOO > out.txt; echo $BAZ >> out.txt"),
        env={"FOO": "BAR", "BAZ": "123"},
    )
    submitted_job = client.submit(job)

    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")
    check_file_contents("out.txt", "BAR\n123\n")


def test_submit_stdio(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    job.program(
        args=bash("echo Test1; cat -; >&2 echo Test2"),
        stdout="out",
        stderr="err",
        stdin=b"Hello\n",
    )
    submitted_job = client.submit(job)
    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")
    check_file_contents("out", "Test1\nHello\n")
    check_file_contents("err", "Test2\n")


def test_empty_stdio(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    def foo():
        print("Hello")
        print("Hello", file=sys.stderr)

    job.function(
        foo,
        stdout=None,
        stderr=None,
    )
    submitted_job = client.submit(job)
    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")
    assert not os.path.isfile("0.stdout")
    assert not os.path.isfile("0.stderr")


def test_stdio_rm_if_finished(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    job.program(
        args="uname",
        stdout=StdioDef.remove_if_finished(path="out"),
        stderr=StdioDef.remove_if_finished(path="err"),
    )
    submitted_job = client.submit(job)
    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")
    assert not Path("out").is_file()
    assert not Path("err").is_file()


def test_wait_for_job(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(
        args=bash("exit 1"),
    )
    job_id = client.submit(job)
    with pytest.raises(FailedJobsException):
        client.wait_for_jobs([job_id])

    job = Job()
    job.program(
        args=bash("echo Test1 > output"),
    )
    job_id = client.submit(job)
    client.wait_for_jobs([job_id])
    check_file_contents("output", "Test1\n")


def test_get_failed_tasks(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(
        args=bash("echo a"),
    )
    job.program(
        args=bash("exit 1"),
    )
    job.program(
        args=bash("echo b"),
    )
    job_id = client.submit(job)
    assert not client.wait_for_jobs([job_id], raise_on_error=False)

    errors = client.get_failed_tasks(job_id)
    assert len(errors) == 1
    assert errors[1].error == "Error: Program terminated with exit code 1."


def test_job_cpus_resources(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(args=bash("echo Hello"), resources=ResourceRequest(cpus="1"))
    job.program(args=bash("echo Hello"), resources=ResourceRequest(cpus="2"))
    job.program(args=bash("echo Hello"), resources=ResourceRequest(cpus="all"))
    submitted_job = client.submit(job)
    time.sleep(1.0)

    table = hq_env.command(["task", "list", str(submitted_job.id)], as_table=True)
    assert table.get_column_value("State") == ["FINISHED", "WAITING", "FINISHED"]


def test_job_generic_resources(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    hq_env.start_worker(
        args=[
            "--resource",
            "gpus=range(1-2)",
            "--resource",
            "fairy=sum(1000)",
        ]
    )
    t1 = job.program(args=bash("echo Hello"), resources=ResourceRequest(resources={"gpus": 1}))
    job.program(args=bash("echo Hello"), resources=ResourceRequest(resources={"gpus": 4}))
    job.program(
        args=bash("echo Hello"),
        resources=ResourceRequest(resources={"gpus": 2}),
        deps=[t1],
    )
    job.program(
        args=bash("echo Hello"),
        resources=ResourceRequest(resources={"gpus": 2, "fairy": 2000}),
    )

    submitted_job = client.submit(job)
    time.sleep(1.0)
    table = hq_env.command(["task", "list", str(submitted_job.id)], as_table=True)
    assert table.get_column_value("State") == [
        "FINISHED",
        "WAITING",
        "FINISHED",
        "WAITING",
    ]


def test_task_priorities(hq_env: HqEnv):
    """Submits tasks with different randomly shuffled priorities and
    checks that tasks are executed in this order
    """
    (job, client) = prepare_job_client(hq_env)

    priorities = list(range(-20, 20))
    random.seed(123123)
    random.shuffle(priorities)
    for p in priorities:
        if random.randint(0, 1) == 0:
            job.program(bash("echo Hello"), priority=p)
        else:
            job.function(lambda: 0, priority=p)
    job_id = client.submit(job)
    client.wait_for_jobs([job_id])
    data = hq_env.command(["--output-mode=json", "task", "list", "1"], as_json=True)["1"]

    starts1 = [(props["id"], iso8601.parse_date(props["started_at"])) for props in data]
    starts2 = starts1[:]

    starts1.sort(key=lambda x: -priorities[x[0]])
    starts2.sort(key=lambda x: x[1])
    assert starts1 == starts2


def test_resource_uniqueness_priorities(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env, with_worker=False)
    hq_env.start_worker()
    hq_env.start_worker(
        cpus=1,
        args=[
            "--resource",
            "res0=range(1-1)",
        ],
    )

    ts0 = [job.program(bash("sleep 1")) for _ in range(5)]
    res = ResourceRequest(cpus="1", resources={"res0": 1})
    for t in ts0:
        job.program(bash("sleep 1"), deps=[t], resources=res)

    job_id = client.submit(job)
    client.wait_for_jobs([job_id])

    data = hq_env.command(["--output-mode=json", "task", "list", "1"], as_json=True)["1"]
    print(data)
    tasks_on_worker_2 = [t["id"] for t in data if t["worker"] == 1]
    print(tasks_on_worker_2)
    tasks_on_worker_2 = [t["id"] for t in data if t["worker"] == 2]
    print(tasks_on_worker_2)


def test_resources_task(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(
        args=bash("echo Hello"),
        resources=ResourceRequest(cpus="2", resources={"res0": 1, "res1": 105.1}),
    )
    submitted_job = client.submit(job)
    wait_for_job_state(hq_env, 1, "WAITING")

    table = hq_env.command(["task", "info", str(submitted_job.id), "0"], as_table=True)
    table.check_row_value("Resources", "cpus: 2 compact\nres0: 1 compact\nres1: 105.1 compact")


def test_priority_task(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(
        args=bash("echo Hello"),
        priority=2,
    )
    submitted_job = client.submit(job)
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["task", "info", str(submitted_job.id), "0"], as_table=True)
    table.check_row_value("Priority", "2")


def test_resource_variants(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(
        args=bash("echo Hello"),
        resources=[
            ResourceRequest(cpus=2),
            ResourceRequest(cpus=1, resources={"gpus": 2}),
        ],
    )
    submitted_job = client.submit(job)

    table = hq_env.command(["task", "info", str(submitted_job.id), "0"], as_table=True)
    table.check_row_value(
        "Resources",
        "# Variant 1\ncpus: 2 compact\n# Variant 2\ncpus: 1 compact\ngpus: 2 compact",
    )


def test_job_forget(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(args=bash("uname"))
    submitted_job = client.submit(job)
    wait_for_job_state(hq_env, 1, "FINISHED")

    client.forget(submitted_job)

    wait_for_job_list_count(hq_env, 0)


def test_job_forget_running(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(args=bash("sleep 100"))
    submitted_job = client.submit(job)
    wait_for_job_state(hq_env, 1, "RUNNING")

    with pytest.raises(Exception, match="Cannot forget job 1"):
        client.forget(submitted_job)


def test_resource_min_time(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(
        args=bash("echo Hello"),
        resources=[
            ResourceRequest(min_time=datetime.timedelta(seconds=42)),
        ],
    )
    client.submit(job)

    data = hq_env.command(["--output-mode=json", "job", "info", "last"], as_json=True)[0]
    assert data["submits"][0]["graph"][0]["resources"][0]["min_time"] == 42.0


def test_job_name(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env, name="Foo")
    job.program("ls")
    client.submit(job)
    data = hq_env.command(["job", "info", "last", "--output-mode", "json"], as_json=True)
    assert data[0]["info"]["name"] == "Foo"

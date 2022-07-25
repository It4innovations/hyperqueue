import random
import time
from pathlib import Path

import iso8601
import pytest
from hyperqueue.client import FailedJobsException
from hyperqueue.ffi.protocol import ResourceRequest
from hyperqueue.job import Job

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.io import check_file_contents
from ..utils.table import parse_multiline_cell
from . import bash, prepare_job_client


def test_submit_empty_job(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    with pytest.raises(
        Exception, match="Submitted job must have at least a single task"
    ):
        client.submit(job)


def test_submit_simple(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(args=["hostname"])
    submitted_job = client.submit(job)

    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")


def test_submit_cwd(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    cwd = Path(hq_env.server_dir) / "workdir"
    cwd.mkdir()

    job.program(args=["hostname"], cwd=str(cwd))
    submitted_job = client.submit(job)

    wait_for_job_state(hq_env, submitted_job.id, "FINISHED")

    table = hq_env.command(["task", "list", str(submitted_job.id)], as_table=True)
    cell = table.get_column_value("Paths")[0]
    assert parse_multiline_cell(cell)["Workdir"] == str(cwd)


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
    assert errors[1].error == "Error: Program terminated with exit code 1"


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
    t1 = job.program(
        args=bash("echo Hello"), resources=ResourceRequest(generic={"gpus": 1})
    )
    job.program(args=bash("echo Hello"), resources=ResourceRequest(generic={"gpus": 4}))
    job.program(
        args=bash("echo Hello"),
        resources=ResourceRequest(generic={"gpus": 2}),
        deps=[t1],
    )
    job.program(
        args=bash("echo Hello"),
        resources=ResourceRequest(generic={"gpus": 2, "fairy": 2000}),
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
    data = hq_env.command(["--output-mode=json", "task", "list", "1"], as_json=True)[
        "1"
    ]

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
    res = ResourceRequest(cpus="1", generic={"res0": 1})
    for t in ts0:
        job.program(bash("sleep 1"), deps=[t], resources=res)

    job_id = client.submit(job)
    client.wait_for_jobs([job_id])

    data = hq_env.command(["--output-mode=json", "task", "list", "1"], as_json=True)[
        "1"
    ]
    print(data)
    tasks_on_worker_2 = [t["id"] for t in data if t["worker"] == 1]
    print(tasks_on_worker_2)
    tasks_on_worker_2 = [t["id"] for t in data if t["worker"] == 2]
    print(tasks_on_worker_2)

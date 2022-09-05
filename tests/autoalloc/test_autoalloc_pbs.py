import json
import os
import time
from os.path import dirname, join
from pathlib import Path
from subprocess import Popen
from typing import List

import pytest

from ..conftest import HqEnv, get_hq_binary
from ..utils.io import check_file_contents
from ..utils.wait import (
    DEFAULT_TIMEOUT,
    TimeoutException,
    wait_for_job_state,
    wait_for_worker_state,
    wait_until,
)
from .conftest import PBS_AVAILABLE, PBS_TIMEOUT, pbs_test
from .pbs_mock import JobState, NewJobFailed, NewJobId, PbsMock
from .utils import (
    add_queue,
    extract_script_args,
    extract_script_commands,
    prepare_tasks,
    program_code_store_args_json,
    program_code_store_cwd_json,
    remove_queue,
)


def test_add_pbs_queue(hq_env: HqEnv):
    hq_env.start_server()
    output = add_queue(
        hq_env,
        manager="pbs",
        name="foo",
        backlog=5,
        workers_per_alloc=2,
    )
    assert "Allocation queue 1 successfully created" in output

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("ID", 0, "1")


def test_pbs_queue_qsub_args(hq_env: HqEnv):
    path = join(hq_env.work_path, "qsub.out")
    qsub_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("qsub", qsub_code):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, time_limit="3m", additional_args="--foo=bar a b --baz 42")
        wait_until(lambda: os.path.exists(path))

        with open(path) as f:
            args = json.loads(f.read())
            qsub_script_path = args[1]
        with open(qsub_script_path) as f:
            data = f.read()
            pbs_args = extract_script_args(data, "#PBS")
            assert pbs_args == [
                "-l select=1",
                "-N hq-alloc-1",
                f"-o {join(dirname(qsub_script_path), 'stdout')}",
                f"-e {join(dirname(qsub_script_path), 'stderr')}",
                "-l walltime=00:03:00",
                "--foo=bar a b --baz 42",
            ]


def test_pbs_queue_qsub_success(hq_env: HqEnv):
    mock = PbsMock(hq_env, new_job_responses=[NewJobId(id="123.job")])

    with mock.activate():
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env)
        wait_for_alloc(hq_env, "QUEUED", "123.job")


def test_pbs_qsub_command_multinode_allocation(hq_env: HqEnv):
    path = join(hq_env.work_path, "qsub.out")
    qsub_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("qsub", qsub_code):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, workers_per_alloc=2)
        wait_until(lambda: os.path.exists(path))

        with open(path) as f:
            args = json.loads(f.read())
            qsub_script_path = args[1]
        with open(qsub_script_path) as f:
            commands = extract_script_commands(f.read())
            assert (
                commands
                == f"pbsdsh -- bash -l -c '{get_hq_binary()} worker start --idle-timeout 5m \
--manager pbs --server-dir {hq_env.server_dir}/001 --on-server-lost=finish-running'"
            )


def test_pbs_allocations_job_lifecycle(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    job_id = mock.job_id(0)

    with mock.activate():
        hq_env.start_server()
        prepare_tasks(hq_env)

        mock.update_job_state(job_id, JobState.queued())
        add_queue(hq_env)

        # Queued
        wait_for_alloc(hq_env, "QUEUED", job_id)

        # Started
        worker = add_worker(hq_env, job_id)
        wait_for_alloc(hq_env, "RUNNING", job_id)

        # Finished
        worker.kill()
        worker.wait()
        hq_env.check_process_exited(worker, expected_code="error")
        wait_for_alloc(hq_env, "FAILED", job_id)


def test_pbs_check_working_directory(hq_env: HqEnv):
    """Check that qsub and qstat are invoked from an autoalloc working directory"""
    qsub_path = join(hq_env.work_path, "qsub.out")
    qsub_code = f"""
{program_code_store_cwd_json(qsub_path)}
print("1")
"""

    with hq_env.mock.mock_program("qsub", qsub_code):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo")
        wait_until(lambda: os.path.isfile(qsub_path))

        check_file_contents(
            qsub_path, Path(hq_env.server_dir) / "001/autoalloc/1-foo/001"
        )


def test_pbs_cancel_active_jobs_on_server_stop(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    with mock.activate():
        process = hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo", backlog=2, workers_per_alloc=1)
        w1 = add_worker(hq_env, mock.job_id(0))
        w2 = add_worker(hq_env, mock.job_id(1))

        def wait_until_fixpoint():
            jobs = hq_env.command(["alloc", "info", "1"], as_table=True)
            # 2 running + 2 queued
            return len(jobs) == 4

        wait_until(lambda: wait_until_fixpoint())

        hq_env.command(["server", "stop"])
        process.wait()
        hq_env.check_process_exited(process)

        w1.wait()
        hq_env.check_process_exited(w1, expected_code="error")
        w2.wait()
        hq_env.check_process_exited(w2, expected_code="error")

        expected_job_ids = set(mock.job_id(index) for index in range(4))
        wait_until(lambda: expected_job_ids == set(mock.deleted_jobs()))


def test_pbs_cancel_queued_jobs_on_remove_queue(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    for index in range(2):
        mock.update_job_state(
            mock.job_id(index),
            JobState.queued(),
        )

    with mock.activate():
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo", backlog=2, workers_per_alloc=1)

        def wait_until_fixpoint():
            jobs = hq_env.command(["alloc", "info", "1"], as_table=True)
            return len(jobs) == 2

        wait_until(lambda: wait_until_fixpoint())

        remove_queue(hq_env, 1)
        wait_until(lambda: len(hq_env.command(["alloc", "list"], as_table=True)) == 0)

        expected_job_ids = set(mock.job_id(index) for index in range(2))
        wait_until(lambda: expected_job_ids == set(mock.deleted_jobs()))


def test_fail_on_remove_queue_with_running_jobs(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    with mock.activate():
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo", backlog=2, workers_per_alloc=1)
        job_id = mock.job_id(0)

        add_worker(hq_env, job_id)
        wait_for_alloc(hq_env, "RUNNING", job_id)

        remove_queue(
            hq_env,
            1,
            expect_fail="Allocation queue has running jobs, so it will not be removed. "
            "Use `--force` if you want to remove the queue anyway",
        )
        wait_for_alloc(hq_env, "RUNNING", job_id)


def test_pbs_cancel_active_jobs_on_forced_remove_queue(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    with mock.activate():
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo", backlog=2, workers_per_alloc=1)

        job_a = mock.job_id(0)
        job_b = mock.job_id(1)

        add_worker(hq_env, job_a)
        add_worker(hq_env, job_b)

        def wait_until_fixpoint():
            jobs = hq_env.command(["alloc", "info", "1"], as_table=True)
            # 2 running + 2 queued
            return len(jobs) == 4

        wait_until(lambda: wait_until_fixpoint())

        remove_queue(hq_env, 1, force=True)
        wait_until(lambda: len(hq_env.command(["alloc", "list"], as_table=True)) == 0)

        expected_job_ids = set(mock.job_id(index) for index in range(4))
        wait_until(lambda: expected_job_ids == set(mock.deleted_jobs()))


def test_pbs_refresh_allocation_remove_queued_job(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    with mock.activate():
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo")

        mock.update_job_state(
            mock.job_id(0),
            JobState.queued(),
        )
        wait_for_alloc(hq_env, "QUEUED", mock.job_id(0))

        mock.update_job_state(mock.job_id(0), None)
        wait_for_alloc(hq_env, "FAILED", mock.job_id(0))


def test_pbs_refresh_allocation_finish_queued_job(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    with mock.activate():
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo")
        wait_for_alloc(hq_env, "QUEUED", mock.job_id(0))
        mock.update_job_state(
            mock.job_id(0),
            JobState.finished(),
        )
        wait_for_alloc(hq_env, "FINISHED", mock.job_id(0))


def test_pbs_refresh_allocation_fail_queued_job(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    with mock.activate():
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo")
        wait_for_alloc(hq_env, "QUEUED", mock.job_id(0))
        mock.update_job_state(
            mock.job_id(0),
            JobState.failed(),
        )
        wait_for_alloc(hq_env, "FAILED", mock.job_id(0))


def dry_run_cmd() -> List[str]:
    return ["alloc", "dry-run", "pbs", "--time-limit", "1h"]


@pytest.mark.skipif(
    PBS_AVAILABLE, reason="This test will not work properly if `qsub` is available"
)
def test_pbs_dry_run_missing_qsub(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        dry_run_cmd(),
        expect_fail="Could not submit allocation: qsub start failed",
    )


def test_pbs_dry_run_submit_error(hq_env: HqEnv):
    mock = PbsMock(hq_env, new_job_responses=[NewJobFailed(message="FOOBAR")])

    with mock.activate():
        hq_env.start_server()
        hq_env.command(dry_run_cmd(), expect_fail="Stderr: FOOBAR")


def test_pbs_dry_run_cancel_error(hq_env: HqEnv):
    mock = PbsMock(hq_env, new_job_responses=[NewJobId(id="job1")], qdel_code="exit(1)")

    with mock.activate():
        hq_env.start_server()
        hq_env.command(dry_run_cmd(), expect_fail="Could not cancel allocation job1")


def test_pbs_dry_run_success(hq_env: HqEnv):
    mock = PbsMock(hq_env, new_job_responses=[NewJobId(id="job1")])

    with mock.activate():
        hq_env.start_server()
        hq_env.command(dry_run_cmd())


def test_pbs_add_queue_dry_run_fail(hq_env: HqEnv):
    hq_env.start_server()

    mock = PbsMock(hq_env, [NewJobFailed("failed to allocate")])

    with mock.activate():
        add_queue(
            hq_env,
            dry_run=True,
            expect_fail="Could not submit allocation: qsub execution failed",
        )


def test_pbs_too_high_time_request(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    with mock.activate():
        hq_env.start_server()
        hq_env.command(["submit", "--time-request", "1h", "sleep", "1"])

        add_queue(hq_env, name="foo", time_limit="30m")
        time.sleep(1)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        assert len(table) == 0


def get_worker_args(path: str):
    """
    `path` should be a path to qsub submit script.
    """
    with open(path) as f:
        args = json.loads(f.read())
        qsub_script_path = args[1]
    with open(qsub_script_path) as f:
        data = f.read()
    return [
        line
        for line in data.splitlines(keepends=False)
        if line and not line.startswith("#")
    ][0].split(" ")[1:]


def test_pbs_pass_cpu_and_resources_to_worker(hq_env: HqEnv):
    path = join(hq_env.work_path, "qsub.out")
    qsub_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("qsub", qsub_code):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="pbs",
            additional_worker_args=[
                "--cpus",
                "2x8",
                "--resource",
                "x=sum(100)",
                "--resource",
                "y=range(1-4)",
                "--resource",
                "z=list(1,2,4)",
            ],
        )
        wait_until(lambda: os.path.exists(path))

        assert get_worker_args(path) == [
            "worker",
            "start",
            "--idle-timeout",
            "5m",
            "--manager",
            "pbs",
            "--server-dir",
            f"{hq_env.server_dir}/001",
            "--cpus",
            "2x8",
            "--resource",
            '"x=sum(100)"',
            "--resource",
            '"y=range(1-4)"',
            "--resource",
            '"z=list(1,2,4)"',
            "--on-server-lost=finish-running",
        ]


def test_pbs_pass_idle_timeout_to_worker(hq_env: HqEnv):
    path = join(hq_env.work_path, "qsub.out")
    qsub_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("qsub", qsub_code):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="pbs",
            additional_worker_args=[
                "--idle-timeout",
                "30m",
            ],
        )
        wait_until(lambda: os.path.exists(path))

        assert get_worker_args(path) == [
            "worker",
            "start",
            "--idle-timeout",
            "30m",
            "--manager",
            "pbs",
            "--server-dir",
            f"{hq_env.server_dir}/001",
            "--on-server-lost=finish-running",
        ]


def wait_for_alloc(
    hq_env: HqEnv, state: str, allocation_id: str, timeout=DEFAULT_TIMEOUT
):
    """
    Wait until an allocation has the given `state`.
    Assumes a single allocation queue.
    """

    last_table = None

    def wait():
        nonlocal last_table

        last_table = hq_env.command(["alloc", "info", "1"], as_table=True)
        for index in range(len(last_table)):
            if (
                last_table.get_column_value("ID")[index] == allocation_id
                and last_table.get_column_value("State")[index] == state
            ):
                return True
        return False

    try:
        wait_until(wait, timeout_s=timeout)
    except TimeoutException as e:
        if last_table is not None:
            raise Exception(f"{e}, most recent table:\n{last_table}")
        raise e


def test_pbs_pass_on_server_lost(hq_env: HqEnv):
    path = join(hq_env.work_path, "qsub.out")
    qsub_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("qsub", qsub_code):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="pbs",
            additional_worker_args=["--on-server-lost=stop"],
        )
        wait_until(lambda: os.path.exists(path))

        with open(path) as f:
            args = json.loads(f.read())
            qsub_script_path = args[1]
        with open(qsub_script_path) as f:
            data = f.read()
        worker_args = [
            line
            for line in data.splitlines(keepends=False)
            if line and not line.startswith("#")
        ][0].split(" ")[1:]
        assert worker_args == [
            "worker",
            "start",
            "--idle-timeout",
            "5m",
            "--manager",
            "pbs",
            "--server-dir",
            f"{hq_env.server_dir}/001",
            "--on-server-lost=stop",
        ]


@pbs_test
def test_external_pbs_submit_single_worker(cluster_hq_env: HqEnv, pbs_credentials: str):
    cluster_hq_env.start_server()
    prepare_tasks(cluster_hq_env, count=10)

    add_queue(
        cluster_hq_env, additional_args=pbs_credentials, time_limit="5m", dry_run=True
    )
    wait_for_worker_state(cluster_hq_env, 1, "RUNNING", timeout_s=PBS_TIMEOUT)
    wait_for_job_state(cluster_hq_env, 1, "FINISHED")


@pbs_test
def test_external_pbs_submit_multiple_workers(
    cluster_hq_env: HqEnv, pbs_credentials: str
):
    cluster_hq_env.start_server()
    prepare_tasks(cluster_hq_env, count=100)

    add_queue(
        cluster_hq_env,
        additional_args=pbs_credentials,
        time_limit="5m",
        dry_run=True,
        workers_per_alloc=2,
    )
    wait_for_worker_state(cluster_hq_env, [1, 2], "RUNNING", timeout_s=PBS_TIMEOUT)
    wait_for_job_state(cluster_hq_env, 1, "FINISHED")


def add_worker(hq_env: HqEnv, allocation_id: str) -> Popen:
    return hq_env.start_worker(
        env={"PBS_JOBID": allocation_id, "PBS_ENVIRONMENT": "1"},
        args=["--manager", "pbs", "--time-limit", "30m"],
    )


def start_server_with_quick_refresh(
    hq_env: HqEnv, autoalloc_refresh_ms=100, autoalloc_status_check_ms=100
):
    hq_env.start_server(
        env={
            "HQ_AUTOALLOC_REFRESH_INTERVAL_MS": str(autoalloc_refresh_ms),
            "HQ_AUTOALLOC_STATUS_CHECK_INTERVAL_MS": str(autoalloc_status_check_ms),
        }
    )

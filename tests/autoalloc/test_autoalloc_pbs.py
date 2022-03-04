import json
import os
import time
from os.path import dirname, join
from pathlib import Path
from subprocess import Popen
from typing import List

from ..conftest import HqEnv
from ..utils.io import check_file_contents
from ..utils.wait import TimeoutException, wait_until
from .pbs_mock import JobState, NewJobFailed, NewJobId, PbsMock
from .utils import (
    add_queue,
    extract_script_args,
    prepare_tasks,
    program_code_store_args_json,
    program_code_store_cwd_json,
    remove_queue,
)


def test_add_pbs_descriptor(hq_env: HqEnv):
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


def test_pbs_allocations_job_lifecycle(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    job_id = mock.job_id(0)

    with mock.activate():
        hq_env.start_server()
        prepare_tasks(hq_env)

        mock.update_job_state(job_id, JobState(status="Q"))
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


def test_pbs_cancel_queued_jobs_on_remove_descriptor(hq_env: HqEnv):
    mock = PbsMock(hq_env)

    for index in range(2):
        mock.update_job_state(
            mock.job_id(index),
            JobState(
                status="Q",
                qtime="Thu Aug 19 13:05:38 2021",
                stime="Thu Aug 19 13:05:39 2021",
                mtime="Thu Aug 19 13:05:39 2021",
            ),
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


def test_fail_on_remove_descriptor_with_running_jobs(hq_env: HqEnv):
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


def test_pbs_cancel_active_jobs_on_forced_remove_descriptor(hq_env: HqEnv):
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


def dry_run_cmd() -> List[str]:
    return ["alloc", "dry-run", "pbs", "--time-limit", "1h"]


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
                "y=indices(1-4)",
            ],
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
            "--cpus",
            "2x8",
            "--resource",
            '"x=sum(100)"',
            "--resource",
            '"y=indices(1-4)"',
            "--on-server-lost=finish-running",
        ]


def wait_for_alloc(hq_env: HqEnv, state: str, allocation_id: str):
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
        wait_until(wait)
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


def add_worker(hq_env: HqEnv, allocation_id: str) -> Popen:
    return hq_env.start_worker(
        env={"PBS_JOBID": allocation_id, "PBS_ENVIRONMENT": "1"},
        args=["--manager", "pbs", "--time-limit", "30m"],
    )

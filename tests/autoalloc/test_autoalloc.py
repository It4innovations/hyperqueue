import dataclasses
import time
from os.path import dirname, join
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

import pytest
from inline_snapshot import snapshot

from ..conftest import HqEnv, get_hq_binary
from ..utils.wait import (
    DEFAULT_TIMEOUT,
    TimeoutException,
    wait_for_job_state,
    wait_for_worker_state,
    wait_until,
)
from .conftest import PBS_AVAILABLE, PBS_TIMEOUT, SLURM_TIMEOUT, pbs_test, slurm_test
from .mock.handler import CommandHandler, CommandResponse, MockInput, response_error
from .mock.manager import DefaultManager, JobData, Manager, WrappedManager
from .mock.mock import MockJobManager
from .mock.pbs import PbsManager, adapt_pbs
from .mock.slurm import SlurmManager, adapt_slurm
from .utils import (
    ExtractSubmitScriptPath,
    ManagerQueue,
    ManagerType,
    add_queue,
    extract_script_args,
    extract_script_commands,
    pause_queue,
    prepare_tasks,
    remove_queue,
    resume_queue,
)


class ManagerSpec:
    def __init__(self, manager: DefaultManager):
        self.manager = manager

    def manager_type(self) -> ManagerType:
        raise NotImplementedError

    def handler(self) -> CommandHandler:
        return self.adapt(self.manager)

    def adapt(self, manager: Manager) -> CommandHandler:
        raise NotImplementedError


class PbsManagerSpec(ManagerSpec):
    def __init__(self):
        super().__init__(PbsManager())

    def manager_type(self) -> ManagerType:
        return "pbs"

    def adapt(self, manager: Manager) -> CommandHandler:
        return adapt_pbs(manager)


class SlurmManagerSpec(ManagerSpec):
    def __init__(self):
        super().__init__(SlurmManager())

    def manager_type(self) -> ManagerType:
        return "slurm"

    def adapt(self, manager: Manager) -> CommandHandler:
        return adapt_slurm(manager)


# The following parametrization is a function to create a new instance of the manager specs per
# each test
def all_managers(fn):
    return pytest.mark.parametrize(
        "spec",
        (
            SlurmManagerSpec(),
            PbsManagerSpec(),
        ),
        ids=["slurm", "pbs"],
    )(fn)


@all_managers
def test_autoalloc_queue_list(hq_env: HqEnv, spec: ManagerSpec):
    hq_env.start_server()
    add_queue(hq_env, manager=spec.manager_type(), name=None, backlog=5)

    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(
        (
            "ID",
            "Backlog size",
            "Workers per alloc",
            "Timelimit",
            "Manager",
            "Name",
        ),
        0,
        ("1", "5", "1", "1h", spec.manager_type().upper(), ""),
    )

    add_queue(
        hq_env,
        manager=spec.manager_type(),
        name="bar",
        backlog=1,
        workers_per_alloc=2,
        time_limit="1h",
    )
    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(
        ("ID", "Backlog size", "Workers per alloc", "Timelimit", "Name", "Manager"),
        1,
        ("2", "1", "2", "1h", "bar", spec.manager_type().upper()),
    )


@all_managers
def test_autoalloc_timelimit_hms(hq_env: HqEnv, spec: ManagerSpec):
    hq_env.start_server()
    add_queue(hq_env, manager=spec.manager_type(), time_limit="01:10:15")

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("Timelimit", 0, "1h 10m 15s")


@all_managers
def test_autoalloc_timelimit_human_format(hq_env: HqEnv, spec: ManagerSpec):
    hq_env.start_server()
    add_queue(hq_env, manager=spec.manager_type(), time_limit="3h 15m 10s")

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("Timelimit", 0, "3h 15m 10s")


@all_managers
def test_autoalloc_require_timelimit(hq_env: HqEnv, spec: ManagerSpec):
    hq_env.start_server()
    add_queue(
        hq_env,
        manager=spec.manager_type(),
        time_limit=None,
        expect_fail="--time-limit <TIME_LIMIT>",
    )


@all_managers
def test_autoalloc_worker_time_limit_too_large(hq_env: HqEnv, spec: ManagerSpec):
    hq_env.start_server()
    add_queue(
        hq_env,
        manager=spec.manager_type(),
        time_limit="1h",
        worker_time_limit="2h",
        expect_fail="Worker time limit cannot be larger than queue time limit",
    )


def test_autoalloc_remove_queue(hq_env: HqEnv):
    hq_env.start_server()
    add_queue(hq_env, manager="pbs")
    add_queue(hq_env, manager="pbs")
    add_queue(hq_env, manager="pbs")

    result = remove_queue(hq_env, queue_id=2)
    assert "Allocation queue 2 successfully removed" in result

    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(["ID"], 0, ["1"])
    table.check_columns_value(["ID"], 1, ["3"])


def test_autoalloc_zero_backlog(hq_env: HqEnv):
    hq_env.start_server()
    add_queue(
        hq_env,
        manager="pbs",
        name=None,
        backlog=0,
        expect_fail="Backlog has to be at least 1",
    )


@all_managers
def test_add_queue(hq_env: HqEnv, spec: ManagerSpec):
    hq_env.start_server()
    output = add_queue(
        hq_env,
        manager=spec.manager_type(),
        name="foo",
        backlog=5,
        workers_per_alloc=2,
    )
    assert "Allocation queue 1 successfully created" in output

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("ID", 0, "1")


def test_pbs_queue_qsub_args(hq_env: HqEnv):
    queue = ManagerQueue()

    with MockJobManager(hq_env, adapt_pbs(ExtractSubmitScriptPath(queue, PbsManager()))):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="pbs",
            time_limit="3m",
            additional_args="--foo=bar a b --baz 42",
        )
        qsub_script_path = queue.get()

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


def test_slurm_queue_sbatch_args(hq_env: HqEnv):
    queue = ManagerQueue()
    handler = ExtractSubmitScriptPath(queue, SlurmManager())

    with MockJobManager(hq_env, adapt_slurm(handler)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="slurm",
            time_limit="3m",
            additional_args="--foo=bar a b --baz 42",
        )
        sbatch_script_path = queue.get()
        with open(sbatch_script_path) as f:
            data = f.read()
            pbs_args = extract_script_args(data, "#SBATCH")
            assert pbs_args == [
                "--nodes=1",
                "--job-name=hq-alloc-1",
                f"--output={join(dirname(sbatch_script_path), 'stdout')}",
                f"--error={join(dirname(sbatch_script_path), 'stderr')}",
                "--time=00:03:00",
                "--foo=bar a b --baz 42",
            ]


def test_slurm_queue_sbatch_additional_output(hq_env: HqEnv):
    class Manager(SlurmManager):
        def submit_response(self, job_id: str) -> str:
            return f"""
No reservation for this job
--> Verifying valid submit host (login)...OK
--> Verifying valid jobname...OK
--> Verifying valid ssh keys...OK
--> Verifying access to desired queue (normal)...OK
--> Checking available allocation...OK
Submitted batch job {job_id}            
"""

    handler = Manager()

    with MockJobManager(hq_env, adapt_slurm(handler)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        job_id = handler.job_id(0)

        add_queue(
            hq_env,
            manager="slurm",
            time_limit="3m",
        )
        wait_for_alloc(hq_env, "QUEUED", job_id)


@all_managers
def test_queue_submit_success(hq_env: HqEnv, spec: ManagerSpec):
    with MockJobManager(hq_env, spec.handler()):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, manager=spec.manager_type())
        wait_for_alloc(hq_env, "QUEUED", "1.job")


@all_managers
def test_submit_time_request_equal_to_time_limit(hq_env: HqEnv, spec: ManagerSpec):
    with MockJobManager(hq_env, spec.handler()):
        hq_env.start_server()
        hq_env.command(["submit", "--time-request", "10m", "sleep", "1"])

        add_queue(hq_env, manager=spec.manager_type(), time_limit="10m")
        wait_for_alloc(hq_env, "QUEUED", "1.job")


def normalize_output(hq_env: HqEnv, manager: Literal["pbs", "slurm"], output: str) -> str:
    return (
        output.replace(get_hq_binary(), "<hq-binary>")
        .replace(hq_env.server_dir, "<server-dir>")
        .replace(f'"{manager}"', '"<manager>"')
    )


def test_pbs_multinode_allocation(hq_env: HqEnv):
    queue = ManagerQueue()

    with MockJobManager(hq_env, adapt_pbs(ExtractSubmitScriptPath(queue, PbsManager()))):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="pbs", workers_per_alloc=2)
        qsub_script_path = queue.get()

        with open(qsub_script_path) as f:
            commands = normalize_output(hq_env, "pbs", extract_script_commands(f.read()))
            assert commands == snapshot(
                "pbsdsh -- bash -l -c 'RUST_LOG=tako=trace,hyperqueue=trace <hq-binary> worker start --idle-timeout"
                ' "5m" --manager "<manager>" --server-dir "<server-dir>/001" --on-server-lost "finish-running"'
                ' --time-limit "1h"\''
            )


def test_slurm_multinode_allocation(hq_env: HqEnv):
    queue = ManagerQueue()
    handler = ExtractSubmitScriptPath(queue, SlurmManager())

    with MockJobManager(hq_env, adapt_slurm(handler)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm", workers_per_alloc=2)
        sbatch_script_path = queue.get()
        with open(sbatch_script_path) as f:
            commands = normalize_output(hq_env, "slurm", extract_script_commands(f.read()))
            assert commands == snapshot(
                'srun --overlap RUST_LOG=tako=trace,hyperqueue=trace <hq-binary> worker start --idle-timeout "5m"'
                ' --manager "<manager>" --server-dir "<server-dir>/001" --on-server-lost "finish-running" --time-limit'
                ' "1h"'
            )


@all_managers
def test_allocations_job_lifecycle(hq_env: HqEnv, spec: ManagerSpec):
    with MockJobManager(hq_env, spec.handler()):
        hq_env.start_server()
        prepare_tasks(hq_env)

        job_id = spec.manager.job_id(0)
        add_queue(hq_env, manager=spec.manager_type())

        # Queued
        wait_for_alloc(hq_env, "QUEUED", job_id)

        # Started
        worker = spec.manager.add_worker(hq_env, job_id)
        wait_for_alloc(hq_env, "RUNNING", job_id)

        # Finished
        worker.kill()
        worker.wait()
        hq_env.check_process_exited(worker, expected_code="error")
        wait_for_alloc(hq_env, "FAILED", job_id)


@all_managers
def test_check_submit_working_directory(hq_env: HqEnv, spec: ManagerSpec):
    """Check that manager submit command is invoked from the autoalloc working directory"""

    queue = ManagerQueue()

    class Manager(WrappedManager):
        async def handle_submit(self, input: MockInput):
            queue.put(input.cwd)
            return await super().handle_submit(input)

    with MockJobManager(hq_env, spec.adapt(Manager(spec.manager))):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, spec.manager_type(), name="foo")

        expected_cwd = Path(hq_env.server_dir) / "001/autoalloc/1-foo/001"
        assert queue.get() == str(expected_cwd)


@all_managers
def test_cancel_jobs_on_server_stop(hq_env: HqEnv, spec: ManagerSpec):
    manager = spec.manager
    with MockJobManager(hq_env, spec.handler()):
        process = hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=spec.manager_type(),
            name="foo",
            backlog=2,
            workers_per_alloc=1,
        )
        w1 = manager.add_worker(hq_env, manager.job_id(0))
        w2 = manager.add_worker(hq_env, manager.job_id(1))

        def wait_until_fixpoint():
            jobs = hq_env.command(["alloc", "info", "1"], as_table=True)
            # 2 running + 2 queued
            return len(jobs) == 4

        wait_until(wait_until_fixpoint)

        hq_env.command(["server", "stop"])
        process.wait()
        hq_env.check_process_exited(process)

        w1.wait()
        hq_env.check_process_exited(w1, expected_code="error")
        w2.wait()
        hq_env.check_process_exited(w2, expected_code="error")

        expected_job_ids = set(manager.job_id(index) for index in range(4))
        wait_until(lambda: expected_job_ids == manager.deleted_jobs)


@all_managers
def test_fail_on_remove_queue_with_running_jobs(hq_env: HqEnv, spec: ManagerSpec):
    manager = spec.manager
    with MockJobManager(hq_env, spec.handler()):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=spec.manager_type(),
            name="foo",
            backlog=2,
            workers_per_alloc=1,
        )
        job_id = manager.job_id(0)

        manager.add_worker(hq_env, job_id)
        wait_for_alloc(hq_env, "RUNNING", job_id)

        remove_queue(
            hq_env,
            1,
            expect_fail=(
                "Allocation queue has running jobs, so it will not be removed. "
                "Use `--force` if you want to remove the queue anyway"
            ),
        )
        wait_for_alloc(hq_env, "RUNNING", job_id)


@all_managers
def test_cancel_active_jobs_on_forced_remove_queue(hq_env: HqEnv, spec: ManagerSpec):
    manager = spec.manager
    with MockJobManager(hq_env, spec.handler()):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=spec.manager_type(),
            name="foo",
            backlog=2,
            workers_per_alloc=1,
        )

        manager.add_worker(hq_env, manager.job_id(0))
        manager.add_worker(hq_env, manager.job_id(1))

        def wait_until_fixpoint():
            jobs = hq_env.command(["alloc", "info", "1"], as_table=True)
            # 2 running + 2 queued
            return len(jobs) == 4

        wait_until(wait_until_fixpoint)

        remove_queue(hq_env, 1, force=True)
        wait_until(lambda: len(hq_env.command(["alloc", "list"], as_table=True)) == 0)

        expected_job_ids = set(manager.job_id(index) for index in range(4))
        wait_until(lambda: expected_job_ids == manager.deleted_jobs)


def test_pbs_refresh_allocation_remove_queued_job(hq_env: HqEnv):
    spec = PbsManagerSpec()
    manager = spec.manager
    with MockJobManager(hq_env, spec.handler()):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, manager=spec.manager_type(), name="foo")

        job_id = manager.job_id(0)
        wait_for_alloc(hq_env, "QUEUED", job_id)

        manager.set_job_data(job_id, None)
        wait_for_alloc(hq_env, "FAILED", job_id)


@all_managers
def test_refresh_allocation_finish_queued_job(hq_env: HqEnv, spec: ManagerSpec):
    manager = spec.manager
    with MockJobManager(hq_env, spec.handler()):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        job_id = manager.job_id(0)
        add_queue(hq_env, manager=spec.manager_type(), name="foo")
        wait_for_alloc(hq_env, "QUEUED", job_id)
        manager.set_job_data(
            job_id,
            JobData.finished(),
        )
        wait_for_alloc(hq_env, "FINISHED", job_id)


@all_managers
def test_refresh_allocation_fail_queued_job(hq_env: HqEnv, spec: ManagerSpec):
    manager = spec.manager
    with MockJobManager(hq_env, spec.handler()):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        job_id = manager.job_id(0)
        add_queue(hq_env, manager=spec.manager_type(), name="foo")
        wait_for_alloc(hq_env, "QUEUED", job_id)
        manager.set_job_data(
            job_id,
            JobData.failed(),
        )
        wait_for_alloc(hq_env, "FAILED", job_id)


def dry_run_cmd(spec: ManagerSpec) -> List[str]:
    return ["alloc", "dry-run", spec.manager_type(), "--time-limit", "1h"]


@pytest.mark.skipif(PBS_AVAILABLE, reason="This test will not work properly if `qsub` is available")
def test_pbs_dry_run_missing_qsub(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        dry_run_cmd(PbsManagerSpec()),
        expect_fail="Could not submit allocation: qsub start failed",
    )


@all_managers
def test_dry_run_submit_error(hq_env: HqEnv, spec: ManagerSpec):
    class Manager(WrappedManager):
        async def handle_submit(self, _input: MockInput) -> CommandResponse:
            return response_error(stderr="FOOBAR")

    with MockJobManager(hq_env, spec.adapt(Manager(spec.manager))):
        hq_env.start_server()
        hq_env.command(dry_run_cmd(spec), expect_fail="Stderr: FOOBAR")


@all_managers
def test_dry_run_cancel_error(hq_env: HqEnv, spec: ManagerSpec):
    class Manager(WrappedManager):
        async def handle_delete(self, _input: MockInput) -> CommandResponse:
            return response_error()

    manager = Manager(spec.manager)
    with MockJobManager(hq_env, spec.adapt(manager)):
        hq_env.start_server()
        hq_env.command(
            dry_run_cmd(spec),
            expect_fail=f"Could not cancel allocation {spec.manager.job_id(0)}",
        )


@all_managers
def test_dry_run_success(hq_env: HqEnv, spec: ManagerSpec):
    with MockJobManager(hq_env, spec.handler()):
        hq_env.start_server()
        hq_env.command(dry_run_cmd(spec))


@all_managers
def test_add_queue_dry_run_fail(hq_env: HqEnv, spec: ManagerSpec):
    class Manager(WrappedManager):
        async def handle_submit(self, _input: MockInput) -> CommandResponse:
            return response_error(stderr="FOOBAR")

    program = "qsub"
    if isinstance(spec, SlurmManagerSpec):
        program = "sbatch"
    with MockJobManager(hq_env, spec.adapt(Manager(spec.manager))):
        hq_env.start_server()
        add_queue(
            hq_env,
            manager=spec.manager_type(),
            dry_run=True,
            expect_fail=f"Could not submit allocation: {program} execution failed",
        )


@all_managers
def test_too_high_time_request(hq_env: HqEnv, spec: ManagerSpec):
    with MockJobManager(hq_env, spec.handler()):
        start_server_with_quick_refresh(hq_env)
        hq_env.command(["submit", "--time-request", "1h", "sleep", "1"])

        add_queue(hq_env, manager=spec.manager_type(), name="foo", time_limit="30m")
        time.sleep(1)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        assert len(table) == 0


def get_exec_line(script_path: str):
    """
    `script_path` should be a path to qsub or sbatch submit script.
    """
    with open(script_path) as f:
        data = f.read()
        return [line for line in data.splitlines(keepends=False) if line and not line.startswith("#")][0]


@dataclasses.dataclass(frozen=True)
class WorkerExecLine:
    cmd: str
    env: Dict[str, str]


def parse_exec_line(script_path: str) -> WorkerExecLine:
    """
    `script_path` should be a path to qsub or sbatch submit script.
    """
    line = get_exec_line(script_path)

    def get_env(item: str) -> Optional[Tuple[str, str]]:
        if "=" in item:
            key, value = item.split("=", maxsplit=1)
            if key.isupper():
                return (key, value)
        return None

    env = {}
    cmd = None
    items = line.split(" ")
    for index, item in enumerate(items):
        env_item = get_env(item)
        if env_item is not None:
            assert env_item[0] not in env
            env[env_item[0]] = env_item[1]
        else:
            cmd = " ".join(items[index:])
            break

    assert cmd is not None
    return WorkerExecLine(cmd=cmd, env=env)


@all_managers
def test_pass_cpu_and_resources_to_worker(hq_env: HqEnv, spec: ManagerSpec):
    queue = ManagerQueue()
    manager = ExtractSubmitScriptPath(queue, spec.manager)

    with MockJobManager(hq_env, spec.adapt(manager)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=spec.manager_type(),
            additional_worker_args=[
                "--cpus",
                "2x8",
                "--resource",
                "x=sum(100)",
                "--resource",
                "y=range(1-4)",
                "--resource",
                "z=[1,2,4]",
                "--no-hyper-threading",
                "--no-detect-resources",
            ],
        )

        script = queue.get()
        line = parse_exec_line(script)
        assert normalize_output(hq_env, spec.manager_type(), line.cmd) == snapshot(
            '<hq-binary> worker start --idle-timeout "5m" --manager "<manager>" --server-dir "<server-dir>/001" --cpus'
            ' "2x8" --resource "x=sum(100)" --resource "y=range(1-4)" --resource "z=[1,2,4]" --no-hyper-threading'
            ' --no-detect-resources --on-server-lost "finish-running" --time-limit "1h"'
        )


@all_managers
def test_propagate_rust_log_env(hq_env: HqEnv, spec: ManagerSpec):
    queue = ManagerQueue()
    manager = ExtractSubmitScriptPath(queue, spec.manager)

    with MockJobManager(hq_env, spec.adapt(manager)):
        hq_env.start_server(env=dict(RUST_LOG="foo"))
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=spec.manager_type(),
        )

        script = queue.get()
        line = parse_exec_line(script)
        assert line.env["RUST_LOG"] == "foo"


@all_managers
def test_pass_idle_timeout_to_worker(hq_env: HqEnv, spec: ManagerSpec):
    queue = ManagerQueue()
    manager = ExtractSubmitScriptPath(queue, spec.manager)

    with MockJobManager(hq_env, spec.adapt(manager)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=spec.manager_type(),
            additional_worker_args=[
                "--idle-timeout",
                "30m",
            ],
        )

        script_path = queue.get()
        line = parse_exec_line(script_path)
        assert normalize_output(hq_env, spec.manager_type(), line.cmd) == snapshot(
            '<hq-binary> worker start --idle-timeout "30m" --manager "<manager>" --server-dir "<server-dir>/001"'
            ' --on-server-lost "finish-running" --time-limit "1h"'
        )


@all_managers
def test_pass_on_server_lost(hq_env: HqEnv, spec: ManagerSpec):
    queue = ManagerQueue()
    manager = ExtractSubmitScriptPath(queue, spec.manager)

    with MockJobManager(hq_env, spec.adapt(manager)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=spec.manager_type(),
            additional_worker_args=["--on-server-lost=stop"],
        )
        script_path = queue.get()
        line = parse_exec_line(script_path)
        assert normalize_output(hq_env, spec.manager_type(), line.cmd) == snapshot(
            '<hq-binary> worker start --idle-timeout "5m" --manager "<manager>" --server-dir "<server-dir>/001"'
            ' --on-server-lost "stop" --time-limit "1h"'
        )


@all_managers
def test_pass_worker_time_limit(hq_env: HqEnv, spec: ManagerSpec):
    queue = ManagerQueue()
    manager = ExtractSubmitScriptPath(queue, PbsManager())

    with MockJobManager(hq_env, spec.adapt(manager)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, manager=spec.manager_type(), worker_time_limit="30m")
        script_path = queue.get()
        line = parse_exec_line(script_path)
        assert normalize_output(hq_env, spec.manager_type(), line.cmd) == snapshot(
            '<hq-binary> worker start --idle-timeout "5m" --manager "<manager>" --server-dir "<server-dir>/001"'
            ' --on-server-lost "finish-running" --time-limit "30m"'
        )


@all_managers
def test_start_stop_cmd(hq_env: HqEnv, spec: ManagerSpec):
    queue = ManagerQueue()
    manager = ExtractSubmitScriptPath(queue, spec.manager)

    with MockJobManager(hq_env, spec.adapt(manager)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=spec.manager_type(),
            start_cmd="init.sh",
            stop_cmd="unload.sh",
        )

        script = queue.get()
        assert normalize_output(hq_env, spec.manager_type(), get_exec_line(script)) == snapshot(
            'init.sh && RUST_LOG=tako=trace,hyperqueue=trace <hq-binary> worker start --idle-timeout "5m" --manager'
            ' "<manager>" --server-dir "<server-dir>/001" --on-server-lost "finish-running" --time-limit "1h";'
            " unload.sh"
        )


def test_autoalloc_pause_resume_queue_status(hq_env: HqEnv):
    hq_env.start_server()
    add_queue(hq_env, manager="pbs")

    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_column_value("State", 0, "RUNNING")

    pause_queue(hq_env, 1)
    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_column_value("State", 0, "PAUSED")

    resume_queue(hq_env, 1)
    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_column_value("State", 0, "RUNNING")


@all_managers
def test_do_not_submit_from_paused_queue(hq_env: HqEnv, spec: ManagerSpec):
    with MockJobManager(hq_env, spec.handler()):
        hq_env.start_server()

        add_queue(hq_env, manager=spec.manager_type())
        pause_queue(hq_env, 1)

        prepare_tasks(hq_env)

        time.sleep(1)

        allocations = hq_env.command(["alloc", "info", "1"], as_table=True)
        assert len(allocations) == 0


@pbs_test
def test_external_pbs_submit_single_worker(cluster_hq_env: HqEnv, pbs_credentials: str):
    cluster_hq_env.start_server()
    prepare_tasks(cluster_hq_env, count=10)
    add_queue(
        cluster_hq_env,
        manager="pbs",
        additional_args=pbs_credentials,
        time_limit="5m",
        dry_run=True,
    )
    wait_for_worker_state(cluster_hq_env, 1, "RUNNING", timeout_s=PBS_TIMEOUT)
    wait_for_job_state(cluster_hq_env, 1, "FINISHED")


@pbs_test
def test_external_pbs_submit_multiple_workers(cluster_hq_env: HqEnv, pbs_credentials: str):
    cluster_hq_env.start_server()
    prepare_tasks(cluster_hq_env, count=100)

    add_queue(
        cluster_hq_env,
        manager="pbs",
        additional_args=pbs_credentials,
        time_limit="5m",
        dry_run=True,
        workers_per_alloc=2,
    )
    wait_for_worker_state(cluster_hq_env, [1, 2], "RUNNING", timeout_s=PBS_TIMEOUT)
    wait_for_job_state(cluster_hq_env, 1, "FINISHED")


@slurm_test
def test_external_slurm_submit_single_worker(cluster_hq_env: HqEnv, slurm_credentials: str):
    cluster_hq_env.start_server()
    prepare_tasks(cluster_hq_env, count=10)

    add_queue(
        cluster_hq_env,
        manager="slurm",
        additional_args=slurm_credentials,
        time_limit="5m",
        dry_run=True,
    )
    wait_for_worker_state(cluster_hq_env, 1, "RUNNING", timeout_s=SLURM_TIMEOUT)
    wait_for_job_state(cluster_hq_env, 1, "FINISHED")


@slurm_test
def test_external_slurm_submit_multiple_workers(cluster_hq_env: HqEnv, slurm_credentials: str):
    cluster_hq_env.start_server()
    prepare_tasks(cluster_hq_env, count=100)

    add_queue(
        cluster_hq_env,
        manager="slurm",
        additional_args=slurm_credentials,
        time_limit="5m",
        dry_run=True,
        workers_per_alloc=2,
    )
    wait_for_worker_state(cluster_hq_env, [1, 2], "RUNNING", timeout_s=SLURM_TIMEOUT)
    wait_for_job_state(cluster_hq_env, 1, "FINISHED")


def wait_for_alloc(hq_env: HqEnv, state: str, allocation_id: str, timeout=DEFAULT_TIMEOUT):
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


def start_server_with_quick_refresh(hq_env: HqEnv, autoalloc_refresh_ms=100, autoalloc_status_check_ms=100):
    hq_env.start_server(
        env={
            "HQ_AUTOALLOC_REFRESH_INTERVAL_MS": str(autoalloc_refresh_ms),
            "HQ_AUTOALLOC_STATUS_CHECK_INTERVAL_MS": str(autoalloc_status_check_ms),
        }
    )

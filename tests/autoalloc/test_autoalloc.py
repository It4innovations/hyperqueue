import dataclasses
import time
from os.path import dirname, join
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple, List

from inline_snapshot import snapshot

from .mock.manager import (
    CommandHandler,
    CommandInput,
    CommandOutput,
    response,
    default_job_id,
    JobData,
    JobId,
    ManagerException,
)
from ..conftest import HqEnv, get_hq_binary
from ..utils.wait import (
    wait_until,
)
from .flavor import ManagerFlavor, PbsManagerFlavor, SlurmManagerFlavor, all_flavors
from .mock.mock import MockJobManager
from .utils import (
    ExtractSubmitScriptPath,
    CommQueue,
    add_queue,
    extract_script_args,
    extract_script_commands,
    pause_queue,
    prepare_tasks,
    remove_queue,
    wait_for_alloc,
    start_server_with_quick_refresh,
)


def test_pbs_queue_qsub_args(hq_env: HqEnv):
    manager = ExtractSubmitScriptPath(PbsManagerFlavor().create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="pbs",
            time_limit="3m",
            additional_args="--foo=bar a b --baz 42",
        )
        qsub_script_path = manager.get_script_path()

        with open(qsub_script_path) as f:
            data = f.read()
            pbs_args = extract_script_args(data, "#PBS")
            assert pbs_args == [
                "-l select=1",
                "-N hq-1-1",
                f"-o {join(dirname(qsub_script_path), 'stdout')}",
                f"-e {join(dirname(qsub_script_path), 'stderr')}",
                "-l walltime=00:03:00",
                "--foo=bar a b --baz 42",
            ]


def test_slurm_queue_sbatch_args(hq_env: HqEnv):
    manager = ExtractSubmitScriptPath(SlurmManagerFlavor().create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="slurm",
            time_limit="3m",
            additional_args="--foo=bar a b --baz 42",
        )
        sbatch_script_path = manager.get_script_path()
        with open(sbatch_script_path) as f:
            data = f.read()
            slurm_args = extract_script_args(data, "#SBATCH")
            assert slurm_args == [
                "--nodes=1",
                "--job-name=hq-1-1",
                f"--output={join(dirname(sbatch_script_path), 'stdout')}",
                f"--error={join(dirname(sbatch_script_path), 'stderr')}",
                "--time=00:03:00",
                "--foo=bar a b --baz 42",
            ]


def test_slurm_queue_sbatch_additional_output(hq_env: HqEnv):
    class Handler(CommandHandler):
        async def handle_cli_submit(self, input: CommandInput) -> CommandOutput:
            output = await super().handle_cli_submit(input)
            return response(
                f"""
No reservation for this job
--> Verifying valid submit host (login)...OK
--> Verifying valid jobname...OK
--> Verifying valid ssh keys...OK
--> Verifying access to desired queue (normal)...OK
--> Checking available allocation...OK
{output.stdout}
"""
            )

    with MockJobManager(hq_env, Handler(SlurmManagerFlavor().create_adapter())):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="slurm",
            time_limit="3m",
        )
        wait_for_alloc(hq_env, "QUEUED", default_job_id(0))


@all_flavors
def test_queue_submit_success(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, manager=flavor.manager_type())
        wait_for_alloc(hq_env, "QUEUED", "1.job")


@all_flavors
def test_submit_time_request_equal_to_time_limit(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()):
        start_server_with_quick_refresh(hq_env)
        hq_env.command(["submit", "--time-request", "10m", "sleep", "1"])

        add_queue(hq_env, manager=flavor.manager_type(), time_limit="10m")
        wait_for_alloc(hq_env, "QUEUED", "1.job")


def normalize_output(hq_env: HqEnv, manager: Literal["pbs", "slurm"], output: str) -> str:
    return (
        output.replace(get_hq_binary(), "<hq-binary>")
        .replace(hq_env.server_dir, "<server-dir>")
        .replace(f'"{manager}"', '"<manager>"')
    )


def test_pbs_multinode_allocation(hq_env: HqEnv):
    manager = ExtractSubmitScriptPath(PbsManagerFlavor().create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env, nodes=2)

        add_queue(hq_env, manager="pbs", max_workers_per_alloc=2)
        qsub_script_path = manager.get_script_path()

        with open(qsub_script_path) as f:
            commands = normalize_output(hq_env, "pbs", extract_script_commands(f.read()))
            assert commands == snapshot(
                "pbsdsh -- bash -l -c 'RUST_LOG=tako=trace,hyperqueue=trace <hq-binary> worker start --idle-timeout"
                ' "5m" --manager "<manager>" --server-dir "<server-dir>/001" --on-server-lost "finish-running"'
                ' --time-limit "1h"\''
            )


def test_slurm_multinode_allocation(hq_env: HqEnv):
    manager = ExtractSubmitScriptPath(SlurmManagerFlavor().create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env, nodes=2)

        add_queue(hq_env, manager="slurm", max_workers_per_alloc=2)
        sbatch_script_path = manager.get_script_path()
        with open(sbatch_script_path) as f:
            commands = normalize_output(hq_env, "slurm", extract_script_commands(f.read()))
            assert commands == snapshot(
                'srun --overlap --ntasks=2 --nodes=2 RUST_LOG=tako=trace,hyperqueue=trace <hq-binary> worker start --idle-timeout "5m"'
                ' --manager "<manager>" --server-dir "<server-dir>/001" --on-server-lost "finish-running" --time-limit'
                ' "1h"'
            )


@all_flavors
def test_allocations_job_lifecycle(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()) as mock:
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        job_id = default_job_id(0)
        add_queue(hq_env, manager=flavor.manager_type())

        # Queued
        wait_for_alloc(hq_env, "QUEUED", job_id)

        # Started
        worker = mock.handler.add_worker(hq_env, job_id)
        wait_for_alloc(hq_env, "RUNNING", job_id)

        # Finished
        worker.kill()
        worker.wait()
        hq_env.check_process_exited(worker, expected_code="error")
        wait_for_alloc(hq_env, "FAILED", job_id)


@all_flavors
def test_check_submit_working_directory(hq_env: HqEnv, flavor: ManagerFlavor):
    """Check that manager submit command is invoked from the autoalloc working directory"""

    queue = CommQueue()

    class Manager(CommandHandler):
        async def handle_cli_submit(self, input: CommandInput):
            queue.put(input.cwd)
            return await super().handle_cli_submit(input)

    with MockJobManager(hq_env, Manager(flavor.create_adapter())):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, flavor.manager_type(), name="foo")

        expected_cwd = Path(hq_env.server_dir) / "001/autoalloc/1-foo/001"
        assert queue.get() == str(expected_cwd)


@all_flavors
def test_cancel_jobs_on_server_stop(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, CommandHandler(flavor.create_adapter())) as mock:
        process = start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
            name="foo",
            backlog=2,
            max_workers_per_alloc=1,
        )
        w1 = mock.handler.add_worker(hq_env, default_job_id(0))
        w2 = mock.handler.add_worker(hq_env, default_job_id(1))

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

        expected_job_ids = set(default_job_id(index) for index in range(4))
        wait_until(lambda: expected_job_ids == mock.handler.deleted_jobs)


@all_flavors
def test_fail_on_remove_queue_with_running_jobs(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()) as mock:
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
            name="foo",
            backlog=2,
            max_workers_per_alloc=1,
        )
        job_id = default_job_id(0)

        wait_for_alloc(hq_env, "QUEUED", job_id)
        mock.handler.add_worker(hq_env, job_id)
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


@all_flavors
def test_cancel_active_jobs_on_forced_remove_queue(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, CommandHandler(flavor.create_adapter())) as mock:
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
            name="foo",
            backlog=2,
            max_workers_per_alloc=1,
        )

        mock.handler.add_worker(hq_env, default_job_id(0))
        mock.handler.add_worker(hq_env, default_job_id(1))

        def wait_until_fixpoint():
            jobs = hq_env.command(["alloc", "info", "1"], as_table=True)
            # 2 running + 2 queued
            return len(jobs) == 4

        wait_until(wait_until_fixpoint)

        remove_queue(hq_env, 1, force=True)
        wait_until(lambda: len(hq_env.command(["alloc", "list"], as_table=True)) == 0)

        expected_job_ids = set(default_job_id(index) for index in range(4))
        wait_until(lambda: expected_job_ids == mock.handler.deleted_jobs)


@all_flavors
def test_refresh_allocation_remove_queued_job(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()) as mock:
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, manager=flavor.manager_type(), name="foo")

        job_id = default_job_id()
        wait_for_alloc(hq_env, "QUEUED", job_id)

        mock.handler.set_job_data(job_id, None)
        wait_for_alloc(hq_env, "FAILED", job_id)


@all_flavors
def test_refresh_allocation_finish_queued_job(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()) as mock:
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        job_id = default_job_id()
        add_queue(hq_env, manager=flavor.manager_type(), name="foo")
        wait_for_alloc(hq_env, "QUEUED", job_id)
        mock.handler.set_job_data(
            job_id,
            JobData.finished(),
        )
        wait_for_alloc(hq_env, "FINISHED", job_id)


@all_flavors
def test_refresh_allocation_fail_queued_job(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()) as mock:
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        job_id = default_job_id()
        add_queue(hq_env, manager=flavor.manager_type(), name="foo")
        wait_for_alloc(hq_env, "QUEUED", job_id)
        mock.handler.set_job_data(
            job_id,
            JobData.failed(),
        )
        wait_for_alloc(hq_env, "FAILED", job_id)


@all_flavors
def test_repeated_status_error(hq_env: HqEnv, flavor: ManagerFlavor):
    """
    Submit an allocation that will forever be returned as an error when trying to get its status.
    After some time, that allocation should be removed, to allow new allocations to be started.
    """
    first_job = default_job_id()

    class FailingStatusManager(CommandHandler):
        async def handle_status(self, job_ids: List[JobId]) -> Dict[JobId, JobData]:
            if first_job in job_ids:
                raise ManagerException("Failed to get allocation status")
            return await super().handle_status(job_ids)

    with MockJobManager(hq_env, FailingStatusManager(flavor.create_adapter())):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, manager=flavor.manager_type(), backlog=1)
        # Check that after many status failures, the job is deemed to be finished and a new one is
        # queued.
        wait_for_alloc(hq_env, "FAILED", first_job)
        wait_for_alloc(hq_env, "QUEUED", default_job_id(1))


@all_flavors
def test_too_high_time_request(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()):
        start_server_with_quick_refresh(hq_env)
        hq_env.command(["submit", "--time-request", "1h", "sleep", "1"])

        add_queue(hq_env, manager=flavor.manager_type(), name="foo", time_limit="30m")
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


@all_flavors
def test_pass_cpu_and_resources_to_worker(hq_env: HqEnv, flavor: ManagerFlavor):
    manager = ExtractSubmitScriptPath(flavor.create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
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

        script = manager.get_script_path()
        line = parse_exec_line(script)
        assert normalize_output(hq_env, flavor.manager_type(), line.cmd) == snapshot(
            '<hq-binary> worker start --idle-timeout "5m" --manager "<manager>" --server-dir "<server-dir>/001" --cpus'
            ' "2x8" --resource "x=sum(100)" --resource "y=range(1-4)" --resource "z=[1,2,4]" --no-hyper-threading'
            ' --no-detect-resources --on-server-lost "finish-running" --time-limit "1h"'
        )


@all_flavors
def test_propagate_rust_log_env(hq_env: HqEnv, flavor: ManagerFlavor):
    manager = ExtractSubmitScriptPath(flavor.create_adapter())

    with MockJobManager(hq_env, manager):
        hq_env.start_server(env=dict(RUST_LOG="foo"))
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
        )

        script = manager.get_script_path()
        line = parse_exec_line(script)
        assert line.env["RUST_LOG"] == "foo"


@all_flavors
def test_pass_idle_timeout_to_worker(hq_env: HqEnv, flavor: ManagerFlavor):
    manager = ExtractSubmitScriptPath(flavor.create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
            additional_worker_args=[
                "--idle-timeout",
                "30m",
            ],
        )

        script_path = manager.get_script_path()
        line = parse_exec_line(script_path)
        assert normalize_output(hq_env, flavor.manager_type(), line.cmd) == snapshot(
            '<hq-binary> worker start --idle-timeout "30m" --manager "<manager>" --server-dir "<server-dir>/001"'
            ' --on-server-lost "finish-running" --time-limit "1h"'
        )


@all_flavors
def test_pass_on_server_lost(hq_env: HqEnv, flavor: ManagerFlavor):
    manager = ExtractSubmitScriptPath(flavor.create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
            additional_worker_args=["--on-server-lost=stop"],
        )
        script_path = manager.get_script_path()
        line = parse_exec_line(script_path)
        assert normalize_output(hq_env, flavor.manager_type(), line.cmd) == snapshot(
            '<hq-binary> worker start --idle-timeout "5m" --manager "<manager>" --server-dir "<server-dir>/001"'
            ' --on-server-lost "stop" --time-limit "1h"'
        )


@all_flavors
def test_pass_worker_time_limit(hq_env: HqEnv, flavor: ManagerFlavor):
    manager = ExtractSubmitScriptPath(flavor.create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, manager=flavor.manager_type(), worker_time_limit="30m")
        script_path = manager.get_script_path()
        line = parse_exec_line(script_path)
        assert normalize_output(hq_env, flavor.manager_type(), line.cmd) == snapshot(
            '<hq-binary> worker start --idle-timeout "5m" --manager "<manager>" --server-dir "<server-dir>/001"'
            ' --on-server-lost "finish-running" --time-limit "30m"'
        )


@all_flavors
def test_start_stop_cmd(hq_env: HqEnv, flavor: ManagerFlavor):
    manager = ExtractSubmitScriptPath(flavor.create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
            start_cmd="init.sh",
            stop_cmd="unload.sh",
        )

        script = manager.get_script_path()
        assert normalize_output(hq_env, flavor.manager_type(), get_exec_line(script)) == snapshot(
            'init.sh && RUST_LOG=tako=trace,hyperqueue=trace <hq-binary> worker start --idle-timeout "5m" --manager'
            ' "<manager>" --server-dir "<server-dir>/001" --on-server-lost "finish-running" --time-limit "1h";'
            " unload.sh"
        )


@all_flavors
def test_do_not_submit_from_paused_queue(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()):
        start_server_with_quick_refresh(hq_env)

        add_queue(hq_env, manager=flavor.manager_type())
        pause_queue(hq_env, 1)

        prepare_tasks(hq_env)

        time.sleep(1)

        allocations = hq_env.command(["alloc", "info", "1"], as_table=True)
        assert len(allocations) == 0


def test_slurm_allocation_name(hq_env: HqEnv):
    handler = ExtractSubmitScriptPath(SlurmManagerFlavor().create_adapter())

    def check_name(path: str, name: str):
        with open(path) as f:
            data = f.read()
            slurm_args = extract_script_args(data, "#SBATCH")
            for arg in slurm_args:
                if "--job-name=" in arg:
                    assert arg[len("--job-name=") :] == name
                    return
            raise Exception(f"Slurm name {name} not found in {path}")

    with MockJobManager(hq_env, handler):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm", name="foo", backlog=2)
        check_name(handler.get_script_path(), "foo-1")
        handler.add_worker(hq_env, default_job_id())
        check_name(handler.get_script_path(), "foo-2")


@all_flavors
def test_worker_wrap_cmd(hq_env: HqEnv, flavor: ManagerFlavor):
    manager = ExtractSubmitScriptPath(flavor.create_adapter())

    with MockJobManager(hq_env, manager):
        start_server_with_quick_refresh(hq_env)
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager=flavor.manager_type(),
            wrap_cmd="podman run myimage",
        )

        script = manager.get_script_path()
        assert normalize_output(hq_env, flavor.manager_type(), get_exec_line(script)) == snapshot(
            'RUST_LOG=tako=trace,hyperqueue=trace podman run myimage <hq-binary> worker start --idle-timeout "5m" --manager'
            ' "<manager>" --server-dir "<server-dir>/001" --on-server-lost "finish-running" --time-limit "1h"'
        )

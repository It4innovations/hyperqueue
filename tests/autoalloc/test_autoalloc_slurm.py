from os.path import dirname, join
from queue import Queue

from ..conftest import HqEnv, get_hq_binary
from ..utils.wait import wait_for_job_state, wait_for_worker_state
from .conftest import SLURM_TIMEOUT, slurm_test
from .mock.mock import MockJobManager
from .mock.slurm import SlurmManager, adapt_slurm
from .utils import (
    ExtractSubmitScriptPath,
    add_queue,
    extract_script_args,
    extract_script_commands,
    prepare_tasks,
)


def test_slurm_add_queue(hq_env: HqEnv):
    hq_env.start_server()
    output = add_queue(
        hq_env,
        manager="slurm",
        name="foo",
        backlog=5,
    )
    assert "Allocation queue 1 successfully created" in output

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("ID", 0, "1")


def test_slurm_queue_sbatch_args(hq_env: HqEnv):
    queue = Queue()
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


def test_slurm_command_multinode_allocation(hq_env: HqEnv):
    queue = Queue()
    handler = ExtractSubmitScriptPath(queue, SlurmManager())

    with MockJobManager(hq_env, adapt_slurm(handler)):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm", workers_per_alloc=2)
        sbatch_script_path = queue.get()
        with open(sbatch_script_path) as f:
            commands = extract_script_commands(f.read())
            assert (
                commands
                == f"srun --overlap {get_hq_binary()} worker start --idle-timeout 5m \
--manager slurm --server-dir {hq_env.server_dir}/001 --on-server-lost=finish-running"
            )


@slurm_test
def test_external_slurm_submit_single_worker(
    cluster_hq_env: HqEnv, slurm_credentials: str
):
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
def test_external_slurm_submit_multiple_workers(
    cluster_hq_env: HqEnv, slurm_credentials: str
):
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

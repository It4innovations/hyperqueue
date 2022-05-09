import json
import os
from os.path import dirname, join

from ..conftest import HqEnv, get_hq_binary
from ..utils.wait import wait_for_job_state, wait_for_worker_state, wait_until
from .conftest import SLURM_TIMEOUT, slurm_test
from .utils import (
    add_queue,
    extract_script_args,
    extract_script_commands,
    prepare_tasks,
    program_code_store_args_json,
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
    path = join(hq_env.work_path, "sbatch.out")
    sbatch_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("sbatch", sbatch_code):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="slurm",
            time_limit="3m",
            additional_args="--foo=bar a b --baz 42",
        )
        wait_until(lambda: os.path.exists(path))
        with open(path) as f:
            args = json.loads(f.read())
            sbatch_script_path = args[1]
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
    path = join(hq_env.work_path, "sbatch.out")
    sbatch_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("sbatch", sbatch_code):
        hq_env.start_server()
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm", workers_per_alloc=2)
        wait_until(lambda: os.path.exists(path))
        with open(path) as f:
            args = json.loads(f.read())
            sbatch_script_path = args[1]
        with open(sbatch_script_path) as f:
            commands = extract_script_commands(f.read())
            assert (
                commands
                == f"srun {get_hq_binary()} worker start --idle-timeout 5m \
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

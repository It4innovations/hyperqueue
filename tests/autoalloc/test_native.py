"""
Contains test that actually connect to an external instance of PBS or Slurm.
"""

from ..conftest import HqEnv
from ..utils.wait import (
    wait_for_job_state,
    wait_for_worker_state,
)
from .conftest import PBS_TIMEOUT, SLURM_TIMEOUT, pbs_test, slurm_test
from .utils import (
    add_queue,
    prepare_tasks,
)


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
        max_workers_per_alloc=2,
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
        max_workers_per_alloc=2,
    )
    wait_for_worker_state(cluster_hq_env, [1, 2], "RUNNING", timeout_s=SLURM_TIMEOUT)
    wait_for_job_state(cluster_hq_env, 1, "FINISHED")

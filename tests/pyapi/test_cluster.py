import pytest
from hyperqueue.cluster import LocalCluster, WorkerConfig
from hyperqueue.job import Job

from ..utils import wait_for_worker_state
from ..utils.io import check_file_contents
from . import hq_env_from_cluster


def test_cluster_create():
    with LocalCluster():
        pass


def test_cluster_stop_twice():
    cluster = LocalCluster()
    cluster.stop()

    with pytest.raises(BaseException):
        cluster.stop()


def test_cluster_create_client():
    with LocalCluster() as cluster:
        client = cluster.client()
        job = Job()
        job.program(["hostname"])
        client.submit(job)


def test_cluster_add_worker():
    with LocalCluster() as cluster:
        cluster.start_worker()
        client = cluster.client()
        job = Job()
        job.program(["echo", "hello"], stdout="out.txt")
        job_id = client.submit(job)
        client.wait_for_jobs([job_id])

        check_file_contents("out.txt", "hello\n")


def test_cluster_worker_cores():
    with LocalCluster() as cluster:
        cluster.start_worker(WorkerConfig(cores=4))
        hq_env = hq_env_from_cluster(cluster)
        wait_for_worker_state(hq_env, 1, "RUNNING")

        table = hq_env.command(["worker", "list"], as_table=True)
        table.check_columns_value(["Resources"], 0, ["cpus 4"])

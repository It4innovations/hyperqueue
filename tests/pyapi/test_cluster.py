import pytest

from hyperqueue.cluster import LocalCluster
from hyperqueue.job import Job

from ..utils.io import check_file_contents


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

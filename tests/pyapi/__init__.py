import os.path
from typing import List, Tuple

from hyperqueue.client import Client
from hyperqueue.job import Job

from hyperqueue import LocalCluster

from ..conftest import HqEnv
from ..utils.mock import ProgramMock


def prepare_job_client(
    hq_env: HqEnv, with_worker=True, **job_args
) -> Tuple[Job, Client]:
    hq_env.start_server()
    if with_worker:
        hq_env.start_worker()
    client = Client(hq_env.server_dir)
    return (Job(**job_args), client)


def bash(command: str) -> List[str]:
    return ["bash", "-c", command]


def hq_env_from_cluster(cluster: LocalCluster) -> HqEnv:
    server_dir = cluster.cluster.server_dir
    return HqEnv(server_dir, ProgramMock(os.path.join(server_dir, "mock")))

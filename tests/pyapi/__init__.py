from typing import List, Tuple

from hyperqueue.client import Client
from hyperqueue.job import Job

from ..conftest import HqEnv


def prepare_job_client(hq_env: HqEnv, with_worker=True, **job_args) -> Tuple[Job, Client]:
    hq_env.start_server()
    if with_worker:
        hq_env.start_worker()
    client = Client(hq_env.server_dir)
    return (Job(**job_args), client)


def bash(command: str) -> List[str]:
    return ["bash", "-c", command]

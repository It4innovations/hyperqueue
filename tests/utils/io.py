import socket
from contextlib import closing

from . import wait_for_job_state
from .job import default_task_output
from .wait import wait_until
from ..conftest import HqEnv


def check_file_contents(path: str, content):
    assert read_file(path) == str(content)


def read_file(path: str) -> str:
    with open(path) as f:
        return f.read()


def write_file(path: str, content: str):
    with open(path, "w") as f:
        f.write(content)


def create_file(path: str):
    write_file(path, "")


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def read_task_pid(hq_env: HqEnv, job_id: int = 1) -> int:
    """
    Reads the task's PID from its stdout.
    """
    wait_for_job_state(hq_env, job_id, "RUNNING")

    def get_pid():
        pid = read_file(default_task_output(job_id=job_id)).strip()
        if not pid:
            return None
        return int(pid)

    return wait_until(get_pid)

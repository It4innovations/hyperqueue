"""
This file contains tests related to automatic kill of tasks when a worker terminates, is stopped
or a task is cancelled.
"""

import subprocess
from typing import Callable

import pytest
import signal

from .utils.cmd import python
from .utils.io import read_file
from .utils.job import default_task_output
from .utils.wait import wait_until, wait_for_pid_exit, wait_for_worker_state
from .utils import wait_for_job_state
from .conftest import HqEnv


def test_kill_task_child_when_worker_is_cancelled(hq_env: HqEnv):
    def cancel(_worker_process):
        hq_env.command(["job", "cancel", "1"])
        wait_for_job_state(hq_env, 1, "CANCELED")

    check_task_processes_exited(hq_env, cancel)


def test_cancel_sigint_then_sigkill(hq_env: HqEnv):
    """
    Test that the worker sends SIGINT when a task is cancelled,
    and then continues with sending SIGKILL when the task refuses to exit.
    """
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--",
            *python(
                """
import os
import sys
import time
import signal

def signal_handler(sig, frame):
    print(os.getpid(), flush=True)
    time.sleep(3600)

signal.signal(signal.SIGINT, signal_handler)

print("ready", flush=True)
time.sleep(3600)
"""
            ),
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")
    wait_until(lambda: read_file(default_task_output()).strip() == "ready")

    hq_env.command(["job", "cancel", "1"])
    wait_for_job_state(hq_env, 1, "CANCELED")

    wait_until(lambda: len(read_file(default_task_output()).splitlines()) == 2)

    pid = int(read_file(default_task_output()).splitlines()[1])
    wait_for_pid_exit(pid)


@pytest.mark.parametrize(
    "signal",
    [
        signal.SIGINT,
        signal.SIGTERM,
        signal.SIGKILL,
    ],
)
def test_kill_task_when_worker_receives_signal(hq_env: HqEnv, signal: int):
    """
    Make sure that a single child task is killed when the worker receives a signal.
    Note that the whole group is not killed, only the worker, which simulates a more realistic
    situation.

    Usage of PR_SET_PDEATHSIG is what makes this work with SIGKILL.
    """
    hq_env.start_server()
    worker_process = hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--",
            *python(
                """
import os
import time

print(os.getpid(), flush=True)
time.sleep(3600)
"""
            ),
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")

    def get_pid():
        pid = read_file(default_task_output()).strip()
        if not pid:
            return None
        return int(pid)

    pid = wait_until(get_pid)

    worker_process.send_signal(signal)

    wait_for_pid_exit(pid)


@pytest.mark.parametrize(
    "signal",
    [
        signal.SIGINT,
        signal.SIGTERM,
    ],
)
def test_kill_task_child_when_worker_receives_signal(hq_env: HqEnv, signal: int):
    """
    Test that nested child subprocesses are killed when the worker is signalled.
    This does not work with SIGKILL, as the worker doesn't get a chance to react to it,
    and PR_SET_PDEATHSIG doesn't apply to nested processes.
    """

    def interrupt_worker(worker_process):
        worker_process.send_signal(signal)

    check_task_processes_exited(hq_env, interrupt_worker)


@pytest.mark.xfail
def test_kill_task_child_when_worker_receives_sigkill(hq_env: HqEnv):
    """
    HyperQueue currently cannot handle this situation gracefully due to it running in user space
    and not having control over (grand)child processes.
    """

    def terminate_worker(worker_process):
        hq_env.kill_worker(1, signal=signal.SIGKILL)

    check_task_processes_exited(hq_env, terminate_worker)


def test_kill_task_child_when_worker_is_stopped(hq_env: HqEnv):
    def stop_worker(worker_process):
        hq_env.command(["worker", "stop", "1"])
        wait_for_worker_state(hq_env, 1, "STOPPED")
        hq_env.check_process_exited(worker_process)

    check_task_processes_exited(hq_env, stop_worker)


def check_task_processes_exited(hq_env: HqEnv, stop_fn: Callable[[subprocess.Popen], None]):
    """
    Creates a task that spawns a child, and then calls `stop_fn`, which should kill either the task
    or the worker. The function then checks that both the task process and its child have been killed.
    """
    hq_env.start_server()
    worker_process = hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--",
            *python(
                """
import os
import sys
import time
print(os.getpid(), flush=True)
pid = os.fork()
if pid > 0:
    print(pid, flush=True)
time.sleep(3600)
"""
            ),
        ]
    )
    wait_for_job_state(hq_env, 1, "RUNNING")
    wait_until(lambda: len(read_file(default_task_output()).splitlines()) == 2)
    pids = [int(pid) for pid in read_file(default_task_output()).splitlines()]

    stop_fn(worker_process)

    parent, child = pids
    wait_for_pid_exit(parent)
    wait_for_pid_exit(child)

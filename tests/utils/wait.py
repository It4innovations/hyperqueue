import time
from typing import List, Union

import psutil

DEFAULT_TIMEOUT = 10


class TimeoutException(BaseException):
    pass


def wait_until(fn, sleep_s=0.2, timeout_s=DEFAULT_TIMEOUT):
    end = time.time() + timeout_s

    while time.time() < end:
        value = fn()
        if value is not None and value is not False:
            return value
        time.sleep(sleep_s)
    raise TimeoutException(f"Wait timeouted after {timeout_s} seconds")


TERMINAL_STATES = ["failed", "cancelled", "finished"]


def wait_for_state(
    env,
    ids: Union[int, List[int]],
    target_states: Union[str, List[str]],
    commands: List[str],
    state_index: int,
    **kwargs,
):
    if isinstance(ids, int):
        ids = {str(ids)}
    else:
        ids = set(str(id) for id in ids)

    if isinstance(target_states, str):
        target_states = {target_states.lower()}
    else:
        target_states = set(state.lower() for state in target_states)

    last_table = None

    def check():
        nonlocal last_table
        env.check_running_processes()
        table = env.command(commands, as_table=True)
        last_table = table
        items = [row[state_index].lower() for row in table if row[0].lstrip("*") in ids]
        if len(items) < len(ids):
            return False
        r = all(s in target_states for s in items)
        if not r:
            if all(s in TERMINAL_STATES for s in items):
                raise Exception(f"Waiting for {target_states} but job(s) are already in terminal states: {items}")
        return r

    try:
        wait_until(check, **kwargs)
    except TimeoutException as e:
        if last_table is not None:
            raise Exception(f"{e}, most recent table:\n{last_table}")
        raise e


def wait_for_job_state(env, ids: Union[int, List[int]], target_states: Union[str, List[str]], **kwargs):
    wait_for_state(env, ids, target_states, ["job", "list", "--all"], 2, **kwargs)


def wait_for_worker_state(env, ids: Union[int, List[int]], target_states: Union[str, List[str]], **kwargs):
    wait_for_state(env, ids, target_states, ["worker", "list", "--all"], 1, **kwargs)


def wait_for_task_state(env, job_id: int, task_ids: Union[int, List[int]], states: Union[str, List[str]], **kwargs):
    if isinstance(task_ids, int):
        task_ids = [task_ids]

    if isinstance(states, str):
        states = [states]
    assert len(task_ids) == len(states)

    ids = ",".join(str(t) for t in task_ids)
    states = [s.lower() for s in states]

    def check():
        result = env.command(["--output-mode=json", "task", "info", str(job_id), ids], as_json=True)
        return [r["state"] for r in result] == states

    wait_until(check, **kwargs)


def wait_for_pid_exit(pid: int):
    """
    Waits until the given `pid` does not represent any process anymore.
    """
    wait_until(lambda: not psutil.pid_exists(pid))


def wait_for_job_list_count(env, count: int):
    wait_until(lambda: len(env.command(["job", "list", "--all"], as_table=True)) == count)

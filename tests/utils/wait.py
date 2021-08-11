import time
from typing import Union, List

DEFAULT_TIMEOUT = 5


def wait_until(fn, sleep_s=0.2, timeout_s=DEFAULT_TIMEOUT):
    end = time.time() + timeout_s

    while time.time() < end:
        value = fn()
        if value is not None and value is not False:
            return value
        time.sleep(sleep_s)
    raise Exception(f"Wait timeouted after {timeout_s} seconds")


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

    def check():
        table = env.command(commands, as_table=True)
        jobs = [row for row in table[1:] if row[0] in ids]
        return len(jobs) >= len(ids) and all(
            j[state_index].lower() in target_states for j in jobs
        )

    wait_until(check, **kwargs)


def wait_for_job_state(
    env, ids: Union[int, List[int]], target_states: Union[str, List[str]], **kwargs
):
    wait_for_state(env, ids, target_states, ["jobs"], 2, **kwargs)


def wait_for_worker_state(
    env, ids: Union[int, List[int]], target_states: Union[str, List[str]], **kwargs
):
    wait_for_state(env, ids, target_states, ["worker", "list"], 1, **kwargs)
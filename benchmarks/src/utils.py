import os
import time


def get_pyenv_from_env() -> str:
    return os.environ.get("VIRTUAL_ENV")


DEFAULT_TIMEOUT = 15


class TimeoutException(BaseException):
    pass


def wait_until(fn, sleep_s=0.5, timeout_s=DEFAULT_TIMEOUT):
    end = time.time() + timeout_s

    while time.time() < end:
        value = fn()
        if value is not None and value is not False:
            return value
        time.sleep(sleep_s)
    raise TimeoutException(f"Wait timeouted after {timeout_s} seconds")

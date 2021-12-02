import atexit
import contextlib
import multiprocessing.context
import time
from multiprocessing.pool import ThreadPool
from typing import Callable, TypeVar

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


TIMEOUT_POOL = None
T = TypeVar("T")


def with_timeout(fn: Callable[..., T], timeout_s: float) -> T:
    global TIMEOUT_POOL

    if TIMEOUT_POOL is None:
        # it needs to be more than 1 to avoid deadlocks when with_timeout is nested
        TIMEOUT_POOL = ThreadPool(8)
        atexit.register(TIMEOUT_POOL.close)

    future = TIMEOUT_POOL.apply_async(fn)
    try:
        return future.get(timeout=timeout_s)
    except multiprocessing.context.TimeoutError:
        raise TimeoutException()


class Timings:
    def __init__(self):
        self.timings = {}

    def add(self, name, duration):
        assert name not in self.timings
        self.timings[name] = duration

    def duration(self) -> float:
        return self.timings["duration"]

    def to_dict(self):
        return dict(self.timings)

    def __repr__(self):
        return repr(self.timings)

    @contextlib.contextmanager
    def time(self, name="duration"):
        start = time.time()
        yield
        duration = time.time() - start
        self.add(name, duration)

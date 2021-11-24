from abc import ABC


class BenchmarkResult(ABC):
    pass


class Timeout(BenchmarkResult):
    def __init__(self, timeout: float):
        self.timeout = timeout

    def __repr__(self):
        return f"Timeout after {self.timeout}s"


class Failure(BenchmarkResult):
    def __init__(self, exception: BaseException, traceback: str):
        self.exception = exception
        self.traceback = traceback

    def __repr__(self):
        return f"Failure: {self.traceback}"


class Success(BenchmarkResult):
    def __init__(self, duration: float):
        self.duration = duration

    def __repr__(self):
        return f"Success: {self.duration}s"

import dataclasses


@dataclasses.dataclass(frozen=True)
class BenchmarkResult:
    pass


@dataclasses.dataclass(frozen=True)
class Timeout(BenchmarkResult):
    timeout: float

    def __repr__(self):
        return f"Timeout after {self.timeout}s"


@dataclasses.dataclass(frozen=True)
class Failure(BenchmarkResult):
    traceback: str

    def __repr__(self):
        return f"Failure: {self.traceback}"


@dataclasses.dataclass(frozen=True)
class Success(BenchmarkResult):
    duration: float

    def __repr__(self):
        return f"Success: {self.duration}s"

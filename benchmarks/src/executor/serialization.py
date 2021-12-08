import dataclasses
from pathlib import Path
from typing import Any, Dict

from ..benchmark.identifier import BenchmarkDescriptor
from ..benchmark.result import BenchmarkResult, Failure, Success, Timeout
from .executor import BenchmarkContext


@dataclasses.dataclass(frozen=True)
class SerializedBenchmark:
    descriptor: BenchmarkDescriptor
    ctx: BenchmarkContext
    cwd: Path


def serialize_result(result: BenchmarkResult) -> Dict[str, Any]:
    if isinstance(result, Success):
        type = "success"
    elif isinstance(result, Timeout):
        type = "timeout"
    elif isinstance(result, Failure):
        type = "failure"
    else:
        assert False
    return dict(type=type, data=result.to_dict())


def deserialize_result(data: Dict[str, Any]) -> BenchmarkResult:
    type = data["type"]
    data = data["data"]
    if type == "success":
        return Success.from_dict(data)
    elif type == "timeout":
        return Timeout.from_dict(data)
    elif type == "failure":
        return Failure.from_dict(data)
    else:
        assert False

import dataclasses
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from mashumaro import DataClassDictMixin

from ..environment import EnvironmentDescriptor
from ..workloads import Workload


@dataclasses.dataclass(frozen=True)
class BenchmarkIdentifier(DataClassDictMixin):
    # Name of the workload
    workload: str
    # Environment type
    environment: str
    # Parameters of the benchmark environment (# of workers, etc.)
    environment_params: Dict[str, Any]
    # Directory where will the benchmark be executed
    workdir: str
    # Unique key that describes this benchmark
    key: str
    # Number of the benchmark run
    index: int = 0
    # Parameters passed to the workload function
    workload_params: Dict[str, Any] = dataclasses.field(default_factory=dict)
    # Timeout of the benchmark in seconds
    timeout: Optional[int] = None
    # Additional metadata describing the benchmark
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.metadata["workdir"] = self.workdir
        self.metadata["key"] = self.key


@dataclasses.dataclass(frozen=True)
class BenchmarkExecutorConfig:
    init_script: Path


@dataclasses.dataclass(frozen=True)
class BenchmarkDescriptor:
    env_descriptor: EnvironmentDescriptor
    workload: Workload
    repeat_count: int = 1
    executor_config: Optional[BenchmarkExecutorConfig] = None
    timeout: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class BenchmarkInstance:
    identifier: BenchmarkIdentifier
    descriptor: BenchmarkDescriptor


def create_identifiers(
    descriptors: List[BenchmarkDescriptor], workdir: Path, default_timeout_s: int
) -> List[BenchmarkInstance]:
    identifiers = []
    for descriptor in descriptors:
        identifiers.extend(
            BenchmarkInstance(
                identifier=create_identifier(workdir, descriptor, default_timeout_s, index=index),
                descriptor=descriptor,
            )
            for index in range(descriptor.repeat_count)
        )
    return identifiers


def create_identifier(
    workdir: Path, descriptor: BenchmarkDescriptor, default_timeout_s: int, index: int
) -> BenchmarkIdentifier:
    env_name = descriptor.env_descriptor.name()
    env_params = descriptor.env_descriptor.parameters()
    metadata = descriptor.env_descriptor.metadata()
    workload_name = descriptor.workload.name()
    workload_params = descriptor.workload.parameters()

    key = create_benchmark_key(
        workload=workload_name,
        workload_params=workload_params,
        environment=env_name,
        environment_params=env_params,
        index=index,
    )

    workdir = Path(workdir / key).absolute()
    workdir.mkdir(parents=True, exist_ok=True)

    return BenchmarkIdentifier(
        workload=workload_name,
        workload_params=workload_params,
        workdir=str(workdir),
        key=key,
        environment=env_name,
        environment_params=env_params,
        index=index,
        metadata=metadata,
        timeout=descriptor.timeout or default_timeout_s,
    )


def create_benchmark_key(
    workload: str,
    workload_params: Dict[str, Any],
    environment: str,
    environment_params: Dict[str, Any],
    index: int,
) -> str:
    return f"{workload}-{format_value(workload_params)}-{environment}-{format_value(environment_params)}-{index}"


def format_value(value):
    if isinstance(value, dict):
        items = sorted(value.items())
        return "_".join(f"{format_value(k)}={format_value(v)}" for (k, v) in items)
    elif isinstance(value, (list, tuple)):
        items = sorted(value)
        return "_".join(format_value(v) for v in items)
    elif isinstance(value, str):
        try:
            path = Path(value)
            if path.is_absolute() and path.exists():
                return os.path.basename(value)
        except BaseException:
            pass
        return value
    elif value is None:
        return format_value("default")
    else:
        return str(value)

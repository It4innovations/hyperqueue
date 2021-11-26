import os.path
from pathlib import Path
from typing import Any, Dict, List

from . import BenchmarkInstance
from .identifier import BenchmarkIdentifier
from .. import ROOT_DIR
from ..clusterutils import ClusterInfo, NodeList
from ..clusterutils.node_list import Local
from ..environment import Environment
from ..environment.hq import HqClusterInfo, ProfileMode, HqEnvironment, HqWorkerConfig
from ..workloads import Workload, SleepHQ

DEFAULT_HQ_BINARY_PATH = ROOT_DIR / "target" / "release" / "hq"

HQ_ENV = "hq"

WORKLOADS = {
    "sleep": {
        HQ_ENV: SleepHQ
    }
}


def _check_type_all(iterable, type):
    for item in iterable:
        if not isinstance(item, type):
            return False
    return True


def parse_hq_workers(data) -> List[HqWorkerConfig]:
    if data is None:
        return [HqWorkerConfig()]
    elif isinstance(data, int):
        return [HqWorkerConfig()] * data
    elif isinstance(data, list) and _check_type_all(data, int):
        return [HqWorkerConfig(cpus) for cpus in data]
    else:
        return [HqWorkerConfig(
            cpus=config.get("cpus"),
            node=config.get("node")
        ) for config in data]


def parse_hq_environment(info: ClusterInfo, identifier: BenchmarkIdentifier) -> HqEnvironment:
    metadata = identifier.metadata
    hq_metadata = metadata.get("hq", {})

    hq_info = HqClusterInfo(
        cluster=info,
        binary=Path(hq_metadata.get("binary", DEFAULT_HQ_BINARY_PATH)).absolute(),
        workers=parse_hq_workers(hq_metadata.get("workers")),
        profile_mode=parse_profile_mode(metadata.get("profile"))
    )
    return HqEnvironment(hq_info)


def parse_environment(identifier: BenchmarkIdentifier, workdir: Path) -> Environment:
    env_type = identifier.environment

    if env_type == HQ_ENV:
        info = parse_cluster_info(identifier, workdir)
        return parse_hq_environment(info, identifier)
    else:
        raise Exception(f"Unknown environment type {env_type}")


def parse_workload(identifier: BenchmarkIdentifier, env_type: str) -> Workload:
    workload = identifier.workload
    if workload in WORKLOADS:
        env_workloads = WORKLOADS[workload]
        if env_type in env_workloads:
            return env_workloads[env_type]()
        else:
            raise Exception(f"Workload {workload} is not implemented for {env_type}")
    else:
        raise Exception(f"Unknown workload {workload}")


def parse_node_list(data) -> NodeList:
    nodes_type = data.get("type", "local")
    if nodes_type == "local":
        return Local()
    else:
        raise Exception(f"Unknown node list type {nodes_type}")


def parse_cluster_info(identifier: BenchmarkIdentifier, workdir: Path) -> ClusterInfo:
    return ClusterInfo(
        workdir=workdir.absolute(),
        node_list=parse_node_list(identifier.metadata.get("nodes", {})),
        monitor_nodes=identifier.metadata.get("monitoring", False)
    )


def parse_profile_mode(data) -> ProfileMode:
    if data is None:
        return ProfileMode()
    if data is True:
        return ProfileMode(server=True, workers=True)
    if isinstance(data, dict):
        return ProfileMode(
            server=data.get("server", False),
            workers=data.get("workers", False),
            frequency=int(data.get("frequency", 99))
        )
    else:
        raise Exception(f"Unknown profile mode: {data}")


def materialize_benchmark(identifier: BenchmarkIdentifier, workdir: Path) -> BenchmarkInstance:
    env_type = identifier.environment

    workload = parse_workload(identifier, env_type)
    key = create_benchmark_key(
        identifier.workload,
        identifier.workload_params,
        identifier.environment,
        identifier.environment_params,
        identifier.index
    )
    workdir = Path(workdir / key).absolute()
    workdir.mkdir(parents=True, exist_ok=True)

    environment = parse_environment(identifier, workdir)
    return BenchmarkInstance(
        workload=workload,
        environment=environment,
        workload_params=identifier.workload_params
    )


def create_benchmark_key(
        benchmark: str,
        params: Dict[str, Any],
        environment: str,
        environment_params: Dict[str, Any],
        index: int
) -> str:
    return (
        f"{benchmark}-{format_dict(params)}-{environment}-{format_dict(environment_params)}-"
        f"{index}"
    )


def format_dict(data: Dict[str, Any]) -> str:
    items = sorted(data.items())
    return "_".join(format_parameter(key, value) for (key, value) in items)


def format_parameter(key: str, value):
    value = value or "default"

    if isinstance(value, str) and Path(value).exists():
        value = os.path.basename(value)
    return f"{key}-{value}"

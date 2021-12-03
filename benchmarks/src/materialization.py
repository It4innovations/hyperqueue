import inspect
import os.path
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import dataclasses

from . import ROOT_DIR
from .benchmark import BenchmarkInstance
from .benchmark.identifier import BenchmarkIdentifier
from .clusterutils import ClusterInfo, NodeList
from .clusterutils.node_list import Local
from .environment import Environment
from .environment.hq import HqClusterInfo, HqEnvironment, HqWorkerConfig, ProfileMode, \
    HqWorkerResources, HqSumWorkerResource, HqIndicesWorkerResource
from .workloads import SleepHQ, Workload
from .workloads.sleep_resources import SleepWithResourcesHQ
from .workloads.stress import StressHQ

DEFAULT_HQ_BINARY_PATH = ROOT_DIR / "target" / "release" / "hq"

HQ_ENV = "hq"

WORKLOADS = {"sleep": {HQ_ENV: SleepHQ}, "sleep_resources": {HQ_ENV: SleepWithResourcesHQ},
             "stress": {HQ_ENV: StressHQ}}


def _check_type_all(iterable, type):
    for item in iterable:
        if not isinstance(item, type):
            return False
    return True


def parse_hq_worker_resources(data) -> HqWorkerResources:
    if data is None:
        return {}
    assert isinstance(data, dict)

    def parse_resource(item):
        if item["type"] == "sum":
            return HqSumWorkerResource(item["amount"])
        elif item["type"] == "indices":
            return HqIndicesWorkerResource(item["start"], item["end"])
        else:
            assert False

    return {
        k: parse_resource(v)
        for (k, v) in data.items()
    }


def parse_hq_workers(data) -> List[HqWorkerConfig]:
    """
    None -> one simple worker
    integer `n` -> `n` simple workers
    list of integer or `None` -> each worker will be parsed as `None` or integer
    list of dict {
        "cpus",
        "node"
    }
    """
    if data is None:
        return [HqWorkerConfig()]
    elif isinstance(data, int):
        return [HqWorkerConfig()] * data
    elif isinstance(data, list) and _check_type_all(data, (type(None), int)):
        return [HqWorkerConfig(cpus) for cpus in data]
    else:
        return [
            HqWorkerConfig(
                cpus=config.get("cpus"),
                node=config.get("node"),
                resources=parse_hq_worker_resources(config.get("resources"))
            )
            for config in data
        ]


def parse_hq_environment(
        info: ClusterInfo, identifier: BenchmarkIdentifier
) -> HqEnvironment:
    metadata = identifier.metadata
    hq_metadata = metadata.get("hq", {})

    hq_info = HqClusterInfo(
        cluster=info,
        binary=Path(hq_metadata.get("binary", DEFAULT_HQ_BINARY_PATH)).absolute(),
        workers=parse_hq_workers(hq_metadata.get("workers")),
        debug=hq_metadata.get("debug", False),
        profile_mode=parse_profile_mode(metadata.get("profile")),
    )
    return HqEnvironment(hq_info)


def parse_environment(identifier: BenchmarkIdentifier, workdir: Path) -> Environment:
    env_type = identifier.environment

    if env_type == HQ_ENV:
        info = parse_cluster_info(identifier, workdir)
        return parse_hq_environment(info, identifier)
    else:
        raise Exception(f"Unknown environment type {env_type}")


def validate_workload_args(
        workload: Workload, workload_name: str, arguments: Dict[str, Any]
):
    signature = inspect.signature(workload.execute)
    params = dict(signature.parameters)
    del params["env"]

    required_params = {
        k: v for (k, v) in params.items() if v.default == inspect.Parameter.empty
    }
    param_set = set(params)
    required_param_set = set(required_params.keys())
    arg_set = set(arguments.keys())
    unknown_args = arg_set - param_set

    def format_args(args: Set[str]) -> str:
        return ", ".join(f"`{arg}`" for arg in sorted(args))

    if required_param_set - arg_set:
        raise Exception(
            f"""Provide all required arguments for workload `{workload_name}`
You have entered: {format_args(arg_set)}
The workload requires: {format_args(required_param_set)}"""
        )
    elif unknown_args:
        raise Exception(
            f"""You have entered unknown arguments for workload `{workload_name}`:
{format_args(unknown_args)}"""
        )


def parse_workload(identifier: BenchmarkIdentifier, env_type: str) -> Workload:
    workload_name = identifier.workload
    if workload_name in WORKLOADS:
        env_workloads = WORKLOADS[workload_name]
        if env_type in env_workloads:
            workload = env_workloads[env_type]()
            validate_workload_args(workload, workload_name, identifier.workload_params)
            return workload
        else:
            raise Exception(
                f"Workload {workload_name} is not implemented for {env_type}"
            )
    else:
        raise Exception(f"Unknown workload {workload_name}")


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
        monitor_nodes=identifier.metadata.get("monitoring", False),
    )


def parse_profile_mode(data) -> ProfileMode:
    if data is None or data is False:
        return ProfileMode()
    if data is True:
        return ProfileMode(server=True, workers=True)
    if isinstance(data, dict):
        return ProfileMode(
            server=data.get("server", False),
            workers=data.get("workers", False),
            frequency=int(data.get("frequency", 99)),
        )
    else:
        raise Exception(f"Unknown profile mode: {data}")


def materialize_benchmark(
        identifier: BenchmarkIdentifier, workdir: Path
) -> Tuple[BenchmarkIdentifier, BenchmarkInstance]:
    env_type = identifier.environment

    workload = parse_workload(identifier, env_type)
    key = create_benchmark_key(
        identifier.workload,
        identifier.workload_params,
        identifier.environment,
        identifier.environment_params,
        identifier.index,
    )
    workdir = Path(workdir / key).absolute()
    workdir.mkdir(parents=True, exist_ok=True)

    environment = parse_environment(identifier, workdir)

    metadata = identifier.metadata.copy()
    identifier = dataclasses.replace(
        identifier, metadata=dict(**metadata, workdir=str(workdir), key=key)
    )
    return (
        identifier,
        BenchmarkInstance(
            workload=workload,
            environment=environment,
            workload_params=identifier.workload_params,
        ),
    )


def create_benchmark_key(
        workload: str,
        workload_params: Dict[str, Any],
        environment: str,
        environment_params: Dict[str, Any],
        index: int,
) -> str:
    return (
        f"{workload}-{format_value(workload_params)}-{environment}-{format_value(environment_params)}"
        f"-{index}"
    )


def format_value(value):
    if isinstance(value, dict):
        items = sorted(value.items())
        return "_".join(f"{format_value(k)}={format_value(v)}" for (k, v) in items)
    elif isinstance(value, (list, tuple)):
        items = sorted(value)
        return "_".join(format_value(v) for v in items)
    elif isinstance(value, str):
        if Path(value).exists():
            return os.path.basename(value)
        return value
    elif value is None:
        return format_value("default")
    else:
        return str(value)

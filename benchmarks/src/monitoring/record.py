import logging
import typing
from typing import Dict, List, TypeVar

import dataclasses
import psutil


@dataclasses.dataclass
class ResourceRecord:
    cpu: List[float]
    mem: float
    connections: int
    net_write: int
    net_read: int
    disk_write: int
    disk_read: int


@dataclasses.dataclass
class ProcessCpuTimes:
    user: float
    system: float
    children_user: float
    children_system: float


@dataclasses.dataclass
class ProcessRecord:
    rss: int
    vm: int
    cpu: float
    cpu_children: float
    cpu_times: ProcessCpuTimes


@dataclasses.dataclass
class MonitoringRecord:
    timestamp: float
    resources: ResourceRecord
    processes: Dict[str, ProcessRecord]

    @staticmethod
    def deserialize_records(file) -> List["MonitoringRecord"]:
        records = []
        for line in file:
            records.append(from_json(MonitoringRecord, line))
        return records

    def serialize(self, file):
        to_json(self, file)


@dataclasses.dataclass
class MonitoringOptions:
    observe_network: bool = False


def record_resources(options: MonitoringOptions) -> ResourceRecord:
    cpus = psutil.cpu_percent(percpu=True)
    mem = psutil.virtual_memory().percent

    if options.observe_network:
        connections = sum(1 if c[5] == "ESTABLISHED" else 0 for c in psutil.net_connections())
        bytes = psutil.net_io_counters()
        io = psutil.disk_io_counters()
    else:
        connections = 0
        bytes = None
        io = None

    return ResourceRecord(
        cpu=cpus,
        mem=mem,
        connections=connections,
        net_write=0 if bytes is None else bytes.bytes_sent,
        net_read=0 if bytes is None else bytes.bytes_recv,
        disk_write=0 if io is None else io.write_bytes,
        disk_read=0 if io is None else io.read_bytes,
    )


def record_processes(
    processes: List[psutil.Process], process_map: Dict[int, psutil.Process]
) -> Dict[str, ProcessRecord]:
    data = {}
    for process in processes:
        try:
            memory_info = process.memory_info()
            cpu_utilization = process.cpu_percent()
            cpu_utilization_children = 0

            # We need to cache the child processes so that `.cpu_percent()` returns the correct
            # results.
            for child in process.children(recursive=True):
                if child.pid not in process_map:
                    process_map[child.pid] = child
                child = process_map[child.pid]
                cpu_utilization_children += child.cpu_percent()

            cpu_times = process.cpu_times()
            cpu_times = ProcessCpuTimes(
                user=cpu_times.user,
                system=cpu_times.system,
                children_user=cpu_times.children_user,
                children_system=cpu_times.children_system,
            )
            data[str(process.pid)] = ProcessRecord(
                rss=memory_info.rss,
                vm=memory_info.vms,
                cpu=cpu_utilization,
                cpu_children=cpu_utilization_children,
                cpu_times=cpu_times,
            )
        except BaseException as e:
            logging.error(e)
    return data


def generate_record(
    timestamp: int, processes: List[psutil.Process], process_map: Dict[int, psutil.Process], options: MonitoringOptions
) -> MonitoringRecord:
    return MonitoringRecord(
        timestamp=timestamp,
        resources=record_resources(options),
        processes=record_processes(processes, process_map),
    )


# This code is duplicated because Python cannot handle both relative imports and this file being executed as a package.
Type = TypeVar("Type")


def from_json(cls: type[Type], input: typing.Union[typing.TextIO, str]) -> Type:
    from serde import json

    if not isinstance(input, str):
        input = input.read()
    return json.from_json(cls, input)


def to_json(object: typing.Any, file: typing.TextIO):
    from serde import json

    serialized = json.to_json(object)
    file.write(serialized)

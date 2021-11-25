import dataclasses
import logging
from typing import Dict, List

import psutil
from mashumaro import DataClassJSONMixin


@dataclasses.dataclass
class ResourceRecord(DataClassJSONMixin):
    cpu: List[float]
    mem: float
    connections: int
    net_write: int
    net_read: int
    disk_write: int
    disk_read: int


@dataclasses.dataclass
class ProcessRecord(DataClassJSONMixin):
    rss: int
    vm: int
    cpu: float


@dataclasses.dataclass
class MonitoringRecord(DataClassJSONMixin):
    timestamp: float
    resources: ResourceRecord
    processes: Dict[str, ProcessRecord]

    @staticmethod
    def deserialize_records(file) -> List["MonitoringRecord"]:
        records = []
        for line in file:
            records.append(MonitoringRecord.from_json(line))
        return records

    def serialize(self, file):
        file.write(self.to_json())


def record_resources() -> ResourceRecord:
    cpus = psutil.cpu_percent(percpu=True)
    mem = psutil.virtual_memory().percent
    connections = sum(1 if c[5] == "ESTABLISHED" else 0 for c in psutil.net_connections())
    bytes = psutil.net_io_counters()
    io = psutil.disk_io_counters()

    return ResourceRecord(
        cpu=cpus,
        mem=mem,
        connections=connections,
        net_write=0 if bytes is None else bytes.bytes_sent,
        net_read=0 if bytes is None else bytes.bytes_recv,
        disk_write=0 if io is None else io.write_bytes,
        disk_read=0 if io is None else io.read_bytes
    )


def record_processes(processes: List[psutil.Process]) -> Dict[str, ProcessRecord]:
    data = {}
    for process in processes:
        try:
            memory_info = process.memory_info()
            cpu_utilization = process.cpu_percent()
            data[str(process.pid)] = ProcessRecord(
                rss=memory_info.rss,
                vm=memory_info.vms,
                cpu=cpu_utilization
            )
        except BaseException as e:
            logging.error(e)
    return data


def generate_record(timestamp: int, processes: List[psutil.Process]) -> MonitoringRecord:
    return MonitoringRecord(
        timestamp=timestamp,
        resources=record_resources(),
        processes=record_processes(processes)
    )

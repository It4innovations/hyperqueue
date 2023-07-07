import dataclasses
import logging
from pathlib import Path
from typing import Dict, List

from cluster.cluster import Cluster, Node

from ..clusterutils.cluster_helper import CLUSTER_FILENAME, node_monitoring_trace
from ..clusterutils.profiler import PROFILER_METADATA_KEY
from ..monitoring.record import MonitoringRecord

MonitoringData = Dict[Node, List[MonitoringRecord]]


ProcessKey = str
ProfilerTag = str
ProfilingData = Dict[ProcessKey, Dict[ProfilerTag, Path]]


@dataclasses.dataclass(frozen=True)
class ClusterReport:
    cluster: Cluster
    monitoring: MonitoringData
    profiling_data: ProfilingData
    directory: Path

    @staticmethod
    def load(directory: Path) -> "ClusterReport":
        cluster_file = directory / CLUSTER_FILENAME
        with open(cluster_file) as f:
            cluster = Cluster.deserialize(f)
        monitoring = load_monitoring_data(directory, cluster)
        profiling_data = load_profiling_data(cluster)
        return ClusterReport(
            cluster=cluster,
            monitoring=monitoring,
            profiling_data=profiling_data,
            directory=directory,
        )


def load_profiling_data(cluster: Cluster) -> ProfilingData:
    data = {}

    for _, process in cluster.processes():
        if PROFILER_METADATA_KEY in process.metadata:
            records = process.metadata[PROFILER_METADATA_KEY]
            process_records = {}

            for tag, file in records.items():
                file = Path(file)
                if file.is_file():
                    process_records[tag] = file
                else:
                    logging.warning(f"Profiler record `{tag}` for `{process.key}` not found at {file}")
            data[process.key] = process_records
    return data


def load_monitoring_data(directory: Path, cluster: Cluster) -> MonitoringData:
    data = {}

    for node, process in cluster.processes():
        if "monitor" == process.key:
            trace_file = node_monitoring_trace(directory, node.hostname)
            if trace_file.exists():
                with open(trace_file) as f:
                    data[node] = MonitoringRecord.deserialize_records(f)
            else:
                logging.warning(f"Monitoring trace for {node.hostname} not found at {trace_file}")
    return data

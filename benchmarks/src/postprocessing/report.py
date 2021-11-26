import logging
from pathlib import Path
from typing import Dict, List

import dataclasses
from cluster.cluster import Cluster, Node

from ..clusterutils.cluster_helper import CLUSTER_FILENAME, node_monitoring_trace
from ..monitoring.record import MonitoringRecord

MonitoringData = Dict[Node, List[MonitoringRecord]]
FlamegraphData = Dict[Node, Path]


@dataclasses.dataclass(frozen=True)
class ClusterReport:
    cluster: Cluster
    monitoring: MonitoringData
    flamegraphs: FlamegraphData
    directory: Path

    @staticmethod
    def load(directory: Path) -> "ClusterReport":
        cluster_file = directory / CLUSTER_FILENAME
        with open(cluster_file) as f:
            cluster = Cluster.deserialize(f)
        monitoring = load_monitoring_data(directory, cluster)
        flamegraphs = load_flamegraphs(cluster)
        return ClusterReport(
            cluster=cluster,
            monitoring=monitoring,
            flamegraphs=flamegraphs,
            directory=directory
        )


def load_flamegraphs(cluster: Cluster) -> FlamegraphData:
    data = {}

    for (_, process) in cluster.processes():
        if "flamegraph" in process.metadata:
            flamegraph_file = Path(process.metadata["flamegraph"])
            if flamegraph_file.is_file():
                data[process.key] = flamegraph_file
            else:
                logging.warning(f"Flamegraph for {process} not found at {flamegraph_file}")
    return data


def load_monitoring_data(directory: Path, cluster: Cluster) -> MonitoringData:
    data = {}

    for (node, process) in cluster.processes():
        if "monitor" == process.key:
            trace_file = node_monitoring_trace(directory, node.hostname)
            if trace_file.exists():
                with open(trace_file) as f:
                    data[node] = MonitoringRecord.deserialize_records(f)
            else:
                logging.warning(f"Monitoring trace for {node.hostname} not found at {trace_file}")
    return data

import dataclasses
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
from cluster.cluster import ProcessInfo

from ..benchmark.database import Database, DatabaseRecord
from .report import ClusterReport


def format_dict(dict: Dict[str, Any]) -> str:
    return ",".join(f"{k}={v}" for (k, v) in sorted(dict.items()))


def create_database_df(database: Database) -> pd.DataFrame:
    data = defaultdict(list)
    for record in database.records:
        data["workload"].append(record.workload)
        data["workload-params"].append(format_dict(record.workload_params))
        data["env"].append(record.environment)
        data["env-params"].append(format_dict(record.environment_params))
        data["index"].append(record.index)
        data["duration"].append(record.duration)
        data["workdir"].append(record.benchmark_metadata["workdir"])
        data["key"].append(record.benchmark_metadata["key"])
    return pd.DataFrame(data)


def groupby_workload(df: pd.DataFrame):
    return df.groupby(["workload", "workload-params"])


def groupby_environment(df: pd.DataFrame):
    return df.groupby(["env", "env-params"])


def create_process_key(process: ProcessInfo):
    return (process.hostname, process.key, process.pid)


@dataclasses.dataclass(frozen=True)
class ProcessStats:
    max_rss: int
    avg_cpu: float


def get_process_aggregated_stats(report: ClusterReport) -> Dict[Any, ProcessStats]:
    process_stats = {
        create_process_key(p): {"max_rss": 0, "avg_cpu": []}
        for (_, p) in report.cluster.processes()
        if p.key != "monitor"
    }
    pid_to_key = {int(p[2]): p for p in process_stats.keys()}
    used_keys = set()

    for node, records in report.monitoring.items():
        for record in records:
            for pid, process_record in record.processes.items():
                pid = int(pid)
                if pid in pid_to_key:
                    key = pid_to_key[pid]
                    used_keys.add(key)
                    process_stats[key]["max_rss"] = max(process_record.rss, process_stats[key]["max_rss"])
                    process_stats[key]["avg_cpu"].append(process_record.cpu)

    for worker in process_stats:
        process_stats[worker]["avg_cpu"] = average(process_stats[worker]["avg_cpu"])

    unused_keys = set(process_stats.keys()) - used_keys
    for key in unused_keys:
        del process_stats[key]

    return {k: ProcessStats(max_rss=v["max_rss"], avg_cpu=v["avg_cpu"]) for (k, v) in process_stats.items()}


def average(data) -> float:
    if not data:
        return np.nan
    return sum(data) / float(len(data))


def pd_print_all():
    return pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.expand_frame_repr",
        False,
    )


def format_large_int(number: int) -> str:
    if len(str(number)) == 4:
        return str(number)
    return f"{number:,}".replace(",", " ")


def analyze_results_utilization(db: Database) -> pd.DataFrame:
    """
    Analyzes the average node and CPU core utilization of benchmarks in the given database.
    """
    results = defaultdict(list)

    for key, row in db.data.items():
        workdir = row.benchmark_metadata["workdir"]
        params = row.workload_params
        duration = row.duration

        for key, value in params.items():
            results[f"workload-{key}"].append(value)

        results["environment"].append(row.environment)
        results["worker-count"].append(row.environment_params["worker_count"])
        results["duration"].append(duration)

        cluster_report = ClusterReport.load(Path(workdir))
        worker_node_utilizations = []
        worker_cpu_utilizations = []

        for node, records in cluster_report.monitoring.items():
            processes = node.processes
            worker_processes = tuple(proc.pid for proc in processes if proc.key.startswith("worker"))
            if len(worker_processes) > 0:
                for record in records:
                    avg_node_util = np.mean(record.resources.cpu)
                    worker_node_utilizations.append(avg_node_util)

                    worker_cpu_util = 0
                    for pid, process_resources in record.processes.items():
                        pid = int(pid)
                        if pid in worker_processes:
                            worker_cpu_util += process_resources.cpu + process_resources.cpu_children
                    worker_cpu_utilizations.append(worker_cpu_util)

        node_util = np.mean(worker_node_utilizations)
        worker_util = np.mean(worker_cpu_utilizations)

        results["worker-node-util"].append(node_util)
        results["worker-cpu-util"].append(worker_util)

    return pd.DataFrame(results)


def analyze_per_worker_utilization(db: Database) -> pd.DataFrame:
    """
    Analyzes the average node utilization of individual workers of
    benchmarks in the given database.
    """
    results = defaultdict(list)

    def push(row: DatabaseRecord, utilization: float, node: str, timestamp: int):
        params = row.workload_params

        # Identify benchmark
        for key, value in params.items():
            results[f"workload-{key}"].append(value)

        results["environment"].append(row.environment)
        results["worker-count"].append(row.environment_params["worker_count"])
        results["uuid"].append(row.uuid)
        results["workdir"].append(row.benchmark_metadata["workdir"])

        # Identify worker
        results["worker"].append(node)

        # Identify result
        results["timestamp"].append(timestamp)
        results["utilization"].append(utilization)

    for key, row in db.data.items():
        workdir = row.benchmark_metadata["workdir"]

        cluster_report = ClusterReport.load(Path(workdir))

        for node, records in cluster_report.monitoring.items():
            processes = node.processes
            worker_processes = tuple(proc.pid for proc in processes if proc.key.startswith("worker"))
            if len(worker_processes) > 0:
                for record in records:
                    avg_node_util = np.mean(record.resources.cpu)
                    push(row, avg_node_util, node.hostname, record.timestamp)

    return pd.DataFrame(results)

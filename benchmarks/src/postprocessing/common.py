import dataclasses
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
from cluster.cluster import ProcessInfo

from ..benchmark.database import Database
from ..materialization import DEFAULT_DATA_JSON
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


def load_database(path: Path) -> Database:
    if path.is_file():
        database_path = path
    elif path.is_dir():
        database_path = path / DEFAULT_DATA_JSON
        assert database_path.is_file()
    else:
        raise Exception(f"{path} is not a valid file or directory")

    return Database(database_path)


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

    for (node, records) in report.monitoring.items():
        for record in records:
            for (pid, process_record) in record.processes.items():
                pid = int(pid)
                if pid in pid_to_key:
                    key = pid_to_key[pid]
                    used_keys.add(key)
                    process_stats[key]["max_rss"] = max(
                        process_record.rss, process_stats[key]["max_rss"]
                    )
                    process_stats[key]["avg_cpu"].append(process_record.cpu)

    for worker in process_stats:
        process_stats[worker]["avg_cpu"] = average(process_stats[worker]["avg_cpu"])

    unused_keys = set(process_stats.keys()) - used_keys
    for key in unused_keys:
        del process_stats[key]

    return {
        k: ProcessStats(max_rss=v["max_rss"], avg_cpu=v["avg_cpu"])
        for (k, v) in process_stats.items()
    }


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

from collections import defaultdict
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from ..benchmark.database import Database
from ..materialization import DEFAULT_DATA_JSON


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
    return pd.DataFrame(data)


def load_database(path: Path) -> Database:
    if path.is_file():
        database_path = path
    elif path.is_dir():
        database_path = path / DEFAULT_DATA_JSON
        assert database_path.is_file()
    else:
        raise Exception(f"{path} is not a valid file or directory")

    return Database(database_path)

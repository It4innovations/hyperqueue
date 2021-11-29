import logging
from collections import defaultdict
from typing import Dict, Any

import pandas as pd

from ..benchmark.database import Database


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


def generate_summary(database: Database, file):
    df = create_database_df(database)
    if df.empty:
        logging.warning("No data found")
        return

    grouped = df.groupby(["workload", "workload-params", "env", "env-params"])["duration"]

    with pd.option_context("display.max_rows", None,
                           "display.max_columns", None,
                           "display.expand_frame_repr", False):
        for (group, data) in sorted(grouped, key=lambda item: item[0]):
            result = data.describe().to_frame().transpose()
            print(group, file=file)
            print(f"{result}\n", file=file)

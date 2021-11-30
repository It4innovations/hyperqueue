import logging

import pandas as pd

from .common import create_database_df
from ..benchmark.database import Database


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

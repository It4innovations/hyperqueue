from collections import defaultdict
from typing import List, Any, Callable, Tuple

import pandas as pd

from ..benchmark.database import Database, DatabaseRecord


class DataFrameExtractor:
    def __init__(self, database: Database):
        self.database = database
        self.keys: List[str] = []
        self.transforms: List[Tuple[str, Callable[[DatabaseRecord], Any]]] = []

    def extract(self, *args: str) -> "DataFrameExtractor":
        self.keys.extend(args)
        return self

    def transform(self, key: str, transform: Callable[[DatabaseRecord], Any]) -> "DataFrameExtractor":
        self.transforms.append((key, transform))
        return self

    def build(self) -> pd.DataFrame:
        records = defaultdict(list)

        keys = frozenset(self.keys)
        for record in self.database.records:
            for key in keys:
                records[key].append(getattr(record, key))
            for key, transform in self.transforms:
                records[key].append(transform(record))
        return pd.DataFrame(records)

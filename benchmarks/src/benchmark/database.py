import dataclasses
import json
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ..utils import ensure_directory
from .identifier import BenchmarkIdentifier

UUID_KEY = "uuid"
WORKLOAD_KEY = "workload"
ENVIRONMENT_KEY = "env"
ENVIRONMENT_PARAMS_KEY = "env-params"
INDEX_KEY = "index"
WORKLOAD_PARAMS_KEY = "workload-params"
BENCHMARK_METADATA_KEY = "benchmark-metadata"

DURATION_KEY = "duration"
METADATA_KEY = "metadata"
TIMESTAMP_KEY = "timestamp"


@dataclasses.dataclass
class DatabaseRecord:
    uuid: str
    workload: str
    environment: str
    environment_params: Dict[str, Any]
    index: int
    workload_params: Dict[str, Any]
    benchmark_metadata: Dict[str, Any]
    duration: float
    timestamp: int
    metadata: Dict[str, Any]


@dataclasses.dataclass(frozen=True)
class BenchmarkResultRecord:
    # None => timeout
    duration: Optional[float]


class Database:
    def __init__(self, path: Path, metadata: Optional[Any] = None):
        self.path = path
        self.data = {}
        if self.path.is_file():
            self.data = load_database(path)
        self.metadata = metadata or np.nan

    @property
    def records(self) -> List[DatabaseRecord]:
        return list(self.data.values())

    def has_key(self, key) -> bool:
        return key in self.data

    def has_record_for(self, identifier: BenchmarkIdentifier) -> bool:
        return self.has_key(create_identifier_key(identifier))

    def store(self, identifier: BenchmarkIdentifier, result: BenchmarkResultRecord):
        key = create_identifier_key(identifier)
        assert not self.has_key(key)

        key = create_identifier_key(identifier)
        record = DatabaseRecord(
            uuid=uuid.uuid4().hex,
            workload=identifier.workload,
            workload_params=identifier.workload_params,
            environment=identifier.environment,
            environment_params=identifier.environment_params,
            index=identifier.index,
            benchmark_metadata=identifier.metadata,
            duration=result.duration or np.nan,
            metadata=self.metadata,
            timestamp=int(time.time()),
        )
        self.data[key] = record

    def save(self):
        ensure_directory(self.path.parent)

        data = defaultdict(list)
        for record in self.data.values():
            data[UUID_KEY].append(record.uuid)
            data[WORKLOAD_KEY].append(record.workload)
            data[WORKLOAD_PARAMS_KEY].append(record.workload_params)
            data[ENVIRONMENT_KEY].append(record.environment)
            data[ENVIRONMENT_PARAMS_KEY].append(record.environment_params)
            data[INDEX_KEY].append(record.index)
            data[BENCHMARK_METADATA_KEY].append(record.benchmark_metadata)
            data[DURATION_KEY].append(record.duration)
            data[METADATA_KEY].append(record.metadata)
            data[TIMESTAMP_KEY].append(record.timestamp)

        df = pd.DataFrame(data)
        df.to_json(self.path, orient="split", index=False)


def serialize(value) -> str:
    if isinstance(value, dict):
        value = sorted(value.items())
    return json.dumps(value)


def parse_record(entry) -> DatabaseRecord:
    return DatabaseRecord(
        uuid=entry[UUID_KEY],
        workload=entry[WORKLOAD_KEY],
        environment=entry[ENVIRONMENT_KEY],
        environment_params=entry[ENVIRONMENT_PARAMS_KEY],
        index=entry[INDEX_KEY],
        workload_params=entry[WORKLOAD_PARAMS_KEY],
        benchmark_metadata=entry[BENCHMARK_METADATA_KEY],
        duration=entry[DURATION_KEY],
        timestamp=entry[TIMESTAMP_KEY],
        metadata=entry[METADATA_KEY],
    )


def load_database(path: Path) -> Dict[Any, DatabaseRecord]:
    database = pd.read_json(path, orient="split", convert_dates=False)
    entries = database.to_dict("records")

    data = {}
    for entry in entries:
        record = parse_record(entry)
        key = create_record_key(record)
        data[key] = record
    return data


def create_record_key(record: DatabaseRecord):
    return (
        record.workload,
        record.environment,
        serialize(record.environment_params),
        record.index,
        serialize(record.workload_params),
    )


def create_identifier_key(identifier: BenchmarkIdentifier):
    return (
        identifier.workload,
        identifier.environment,
        serialize(identifier.environment_params),
        identifier.index,
        serialize(identifier.workload_params),
    )

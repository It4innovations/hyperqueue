from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import typer
from swclient.client import Client, Measurement

from src.benchutils import load_database

app = typer.Typer()


def unpack_dict(dictionary: Dict[str, Any]) -> Dict[str, str]:
    result = {}
    for (key, value) in dictionary.items():
        if isinstance(value, dict):
            value = unpack_dict(value)
            for (k, v) in value.items():
                result[f"{key}/{k}"] = v
        else:
            result[key] = value
    return result


def prefix_dict(dictionary: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    return {f"{prefix}/{k}": str(v) for (k, v) in dictionary.items()}


def normalize_dict(dictionary: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    return prefix_dict(unpack_dict(dictionary), prefix=prefix)


@app.command()
def upload(
    database_path: Path = typer.Argument(..., exists=True),
    token: str = typer.Option(...),
):
    client = Client("https://snailwatch.it4i.cz/api", token)
    database = load_database(database_path)
    measurements = []

    for record in database.records:
        timestamp = datetime.fromtimestamp(record.timestamp)
        measurement = Measurement(
            benchmark=record.workload,
            environment=dict(
                **normalize_dict(record.workload_params, "workload"),
                **normalize_dict(record.environment_params, "env"),
                env=record.environment,
                **normalize_dict(record.benchmark_metadata, "metadata"),
            ),
            result=dict(duration=dict(type="time", value=record.duration)),
            timestamp=timestamp,
        )
        measurements.append(measurement)

    client.upload_measurements(measurements)


if __name__ == "__main__":
    app()

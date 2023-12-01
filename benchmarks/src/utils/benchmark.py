import logging
import time
from datetime import timedelta
from pathlib import Path
from typing import List

import tqdm

from ..benchmark.database import Database
from ..benchmark.identifier import BenchmarkDescriptor
from ..benchmark.runner import BenchmarkRunner
from ..postprocessing.overview import generate_summary_html, generate_summary_text

DEFAULT_DATA_JSON = "data.json"


def has_work_left(workdir: Path, descriptors: List[BenchmarkDescriptor]) -> bool:
    database = load_or_create_database(workdir)
    runner = BenchmarkRunner(database, workdir=workdir)

    materialized = runner.materialize_and_skip(descriptors)
    return len(materialized) > 0


def run_benchmarks(workdir: Path, descriptors: List[BenchmarkDescriptor]) -> Database:
    workdir.mkdir(parents=True, exist_ok=True)

    database = load_or_create_database(workdir)
    runner = BenchmarkRunner(database, workdir=workdir)

    materialized = runner.materialize_and_skip(descriptors)

    try:
        last_save_time = time.time()
        for _info, _result in tqdm.tqdm(runner.compute_materialized(materialized), total=len(materialized)):
            duration = time.time() - last_save_time
            if duration > timedelta(seconds=30).total_seconds():
                last_save_time = time.time()
                runner.save()
    except KeyboardInterrupt:
        runner.save()
        raise

    runner.save()
    return database


def run_benchmarks_with_postprocessing(workdir: Path, descriptors: List[BenchmarkDescriptor]) -> Database:
    database = run_benchmarks(workdir, descriptors)

    summary_txt = workdir / "summary.txt"
    generate_summary_text(database, summary_txt)

    summary_html = workdir / "summary"
    html_index = generate_summary_html(database, summary_html)

    logging.info(f"You can find text summary at {summary_txt}")
    logging.info(f"You can find HTML summary at {html_index}")
    return database


def load_existing_database(path: Path) -> Database:
    """
    Loads an existing database from a file.
    """
    return Database.from_file(path)


def load_or_create_database(workdir: Path) -> Database:
    """
    Loads an existing database from a directory, or creates a new one.
    """
    database_path = workdir / DEFAULT_DATA_JSON
    if database_path.is_file():
        return Database.from_file(database_path)
    else:
        return Database.empty(database_path)

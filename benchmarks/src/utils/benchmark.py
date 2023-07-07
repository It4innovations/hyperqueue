import logging
from pathlib import Path
from typing import List

import tqdm

from ..benchmark.database import Database
from ..benchmark.identifier import BenchmarkDescriptor
from ..benchmark.runner import BenchmarkRunner
from ..postprocessing.overview import generate_summary_html, generate_summary_text

DEFAULT_DATA_JSON = "data.json"


def run_benchmarks(workdir: Path, descriptors: List[BenchmarkDescriptor]) -> Database:
    database = Database(workdir / DEFAULT_DATA_JSON)
    runner = BenchmarkRunner(database, workdir=workdir)

    materialized = runner.materialize_and_skip(descriptors)
    for _info, _result in tqdm.tqdm(runner.compute_materialized(materialized), total=len(materialized)):
        pass

    runner.save()
    return database


def run_benchmarks_with_postprocessing(workdir: Path, descriptors: List[BenchmarkDescriptor]):
    database = run_benchmarks(workdir, descriptors)

    summary_txt = workdir / "summary.txt"
    generate_summary_text(database, summary_txt)

    summary_html = workdir / "summary"
    html_index = generate_summary_html(database, summary_html)

    logging.info(f"You can find text summary at {summary_txt}")
    logging.info(f"You can find HTML summary at {html_index}")


def load_database(path: Path) -> Database:
    if path.is_file():
        database_path = path
    elif path.is_dir():
        database_path = path / DEFAULT_DATA_JSON
        assert database_path.is_file()
    else:
        raise Exception(f"{path} is not a valid file or directory")
    return Database(database_path)

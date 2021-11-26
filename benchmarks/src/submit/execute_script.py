import logging
import os
import shutil
import sys
import time
import traceback
from datetime import timedelta
from pathlib import Path
from typing import Optional

import typer
from tqdm import tqdm

CURRENT_DIR = Path(__file__).absolute().parent
SRC_DIR = CURRENT_DIR.parent.parent
sys.path.insert(0, str(SRC_DIR))

from src.utils.timing import TimeoutException, with_timeout  # noqa
from src.benchmark.runner import BenchmarkRunner  # noqa
from src.benchmark.serde import materialize_benchmark  # noqa
from src.benchmark.database import Database  # noqa
from src.submit.options import deserialize_submit_options  # noqa
from src.submit.submit import submit, deserialize_identifiers  # noqa
from src.submit.utils import generate_job_dir  # noqa


def execute(
        directory: Path = typer.Option(...),
        benchmarks: Path = typer.Option(...),
        submit_options: Path = typer.Option(...),
        database_path: Optional[Path] = None,
        resubmit: Optional[bool] = False
):
    identifiers = deserialize_identifiers(benchmarks)
    options = deserialize_submit_options(submit_options)

    database_file = (directory / "result.json").resolve()
    if database_path and database_path.is_file() and database_file != database_path.resolve():
        database_file = directory / database_path.name
        shutil.copyfile(database_path, database_file)

    database = Database(database_file)
    runner = BenchmarkRunner(database, workdir=directory, materialize_fn=materialize_benchmark)
    benchmark_count = len(identifiers)

    def run():
        for (identifier, benchmark, result) in tqdm(runner.compute(identifiers),
                                                    total=benchmark_count):
            logging.info(f"Finished benchmark {identifier}: {result}")

    max_runtime = options.walltime
    if max_runtime.total_seconds() > 60:
        max_runtime = max_runtime - timedelta(minutes=1)

    logging.info(f"Starting to benchmark {benchmark_count} benchmarks, max time is {max_runtime}")

    start = time.time()
    try:
        with_timeout(run, timeout_s=max_runtime.total_seconds())
        runner.save()

        duration = time.time() - start
        logging.info(f"Benchmark finished in {duration}s")

        # Store a symlink to the final resubmitted directory into the root directory
        if resubmit:
            root_dir = directory.parent.parent
            os.symlink(directory, root_dir / "final-run", target_is_directory=True)
    except TimeoutException:
        runner.save()

        if resubmit:
            root_dir = directory.parent
        else:
            root_dir = directory / "resubmits"
        directory = generate_job_dir(root_dir)

        remaining = [identifier for identifier in identifiers if
                     not database.has_record_for(identifier)]

        logging.warning(
            f"Benchmark didn't finish in {max_runtime}, computed {benchmark_count - len(remaining)}"
            f"/{benchmark_count}, resubmitting at {directory}")
        submit(remaining, workdir=directory, options=options, database_path=database_file,
               resubmit=True)
    except:
        runner.save()
        logging.error(f"Error occurred while benchmarking: {traceback.format_exc()}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    typer.run(execute)

import datetime
import inspect
import logging
import shutil
from pathlib import Path
from typing import Iterable, Optional, Type

import typer
from typer import Typer

from .benchmark.database import Database
from .benchmark.identifier import BenchmarkDescriptor, create_benchmark_instances
from .submit.slurm import run_in_slurm, SlurmOptions
from .utils.benchmark import (
    run_benchmarks_with_postprocessing,
    has_work_left,
    load_or_create_database,
)


class TestCase:
    def name(self) -> str:
        def camel_to_kebab(s: str) -> str:
            return "".join(["-" + c.lower() if c.isupper() else c for c in s]).lstrip("-")

        return camel_to_kebab(self.__class__.__name__)

    def generate_descriptors(self) -> Iterable[BenchmarkDescriptor]:
        raise NotImplementedError

    def postprocess(self, workdir: Path, database: Database):
        pass


def create_cli() -> Typer:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s.%(msecs)03d:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cli = typer.Typer()

    @cli.command(hidden=True)
    def dummy():
        """
        This command is here because Typer has silly defaults and doesn't generate
        a subcommand if there is exactly one command.
        """
        raise NotImplementedError()

    return cli


def run_test_case(workdir: Path, case: TestCase, slurm: bool):
    descriptors = list(case.generate_descriptors())
    has_work = has_work_left(workdir, descriptors)

    def compute():
        if has_work:
            database = run_benchmarks_with_postprocessing(workdir, descriptors)
        else:
            database = load_or_create_database(workdir)

        # Keep only results that we are interested in, there might be some old stale results
        identifiers = [instance.identifier for instance in create_benchmark_instances(descriptors, workdir=workdir)]
        database = database.filter(identifiers)
        case.postprocess(workdir=workdir, database=database)

    if has_work and slurm:
        run_in_slurm(
            SlurmOptions(
                name="slurm-auto-submit",
                queue="qcpu_exp",
                project="DD-21-9",
                walltime=datetime.timedelta(hours=2),
                init_script=Path("/mnt/proj2/dd-21-9/beranekj/modules.sh"),
                workdir=Path("slurm").absolute(),
            ),
            compute,
        )
    else:
        compute()


def register_case(app: Typer):
    def wrap_cls(cls: Type[TestCase]):
        case = cls()

        @app.command(name=case.name())
        def command(workdir: Optional[Path] = None, slurm: bool = False, clear: bool = False):
            if workdir is None:
                workdir = Path("benchmarks") / case.name()
            workdir = workdir.absolute()
            if clear:
                shutil.rmtree(workdir, ignore_errors=True)
            run_test_case(workdir=workdir, case=case, slurm=slurm)

        command.__doc__ = f"""
Run the {case.name()} benchmark.

{inspect.getdoc(cls)}
"""

        return cls

    return wrap_cls

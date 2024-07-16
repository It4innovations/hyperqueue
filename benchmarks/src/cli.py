import datetime
import inspect
import logging
import shutil
from pathlib import Path
from typing import Iterable, Optional, Type

import dataclasses
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
            return "".join(["-" + c.lower() if c.isupper() else c for c in s]).lstrip(
                "-"
            )

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

    return typer.Typer()


@dataclasses.dataclass
class SlurmCliOptions:
    queue: str
    project: str
    init_script: Path
    node_count: int = 1
    wait_for_job: bool = False
    walltime_h: int = 1


def run_test_case(
    workdir: Path,
    case: TestCase,
    slurm_cli: Optional[SlurmCliOptions] = None,
    postprocess_only: bool = False,
):
    descriptors = list(case.generate_descriptors())
    has_work = has_work_left(workdir, descriptors)

    def compute():
        if has_work and not postprocess_only:
            database = run_benchmarks_with_postprocessing(workdir, descriptors)
        else:
            database = load_or_create_database(workdir)

        # Keep only results that we are interested in, there might be some old stale results
        identifiers = [
            instance.identifier
            for instance in create_benchmark_instances(descriptors, workdir=workdir)
        ]
        database = database.filter(identifiers)
        case.postprocess(workdir=workdir, database=database)

    if not postprocess_only and has_work and slurm_cli is not None:
        run_in_slurm(
            SlurmOptions(
                name="slurm-auto-submit",
                queue=slurm_cli.queue,
                project=slurm_cli.project,
                walltime=datetime.timedelta(hours=slurm_cli.walltime_h),
                init_script=slurm_cli.init_script.absolute(),
                node_count=slurm_cli.node_count,
                workdir=Path("slurm").absolute(),
                wait_for_job=slurm_cli.wait_for_job,
            ),
            compute,
        )
    else:
        compute()


def register_case(app: Typer):
    def wrap_cls(cls: Type[TestCase]):
        case = cls()

        @app.command(name=case.name())
        def command(
            workdir: Optional[Path] = None,
            queue: Optional[str] = None,
            project: Optional[str] = None,
            init_script: Optional[Path] = None,
            node_count: Optional[int] = 1,
            walltime_h: Optional[int] = 1,
            wait: bool = False,
            clear: bool = False,
            postprocess_only: bool = False,
        ):
            if workdir is None:
                workdir = Path("benchmarks") / case.name()
            workdir = workdir.absolute()
            if clear:
                shutil.rmtree(workdir, ignore_errors=True)

            if queue is not None or project is not None or init_script is not None:
                assert (
                    queue is not None
                    and project is not None
                    and init_script is not None
                )
                slurm = SlurmCliOptions(
                    queue=queue,
                    project=project,
                    init_script=init_script,
                    node_count=node_count,
                    wait_for_job=wait,
                    walltime_h=walltime_h,
                )
            else:
                slurm = None

            run_test_case(
                workdir=workdir,
                case=case,
                slurm_cli=slurm,
                postprocess_only=postprocess_only,
            )

        command.__doc__ = f"""
Run the {case.name()} benchmark.

{inspect.getdoc(cls)}
"""

        return cls

    return wrap_cls

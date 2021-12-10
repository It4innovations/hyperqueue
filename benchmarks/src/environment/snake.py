from typing import Dict, Any

import dataclasses
import logging

from pathlib import Path
from . import Environment, EnvironmentDescriptor
from .utils import EnvStateManager


@dataclasses.dataclass(frozen=True)
class SnakeClusterInfo:
    workdir: Path


class SnakeEnvironmentDescriptor(EnvironmentDescriptor):
    def create_environment(self, workdir: Path) -> Environment:
        info = SnakeClusterInfo(workdir)
        return SnakeEnvironment(info)

    def name(self) -> str:
        return "snake"

    def parameters(self) -> Dict[str, Any]:
        return {}

    def metadata(self) -> Dict[str, Any]:
        return {}


class SnakeEnvironment(Environment, EnvStateManager):
    def __init__(self, info: SnakeClusterInfo):
        EnvStateManager.__init__(self)
        self.info = info
        self.snakefile = info.workdir / "Snakefile"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def workdir(self) -> Path:
        return self.info.workdir

    def start(self):
        self.state_start()

    def stop(self):
        self.state_stop()

    def submit(self, cmds: str, cpus_per_task: int):
        logging.info(f"Starting Snakemake {cmds, cpus_per_task}")
        with open(self.snakefile, "w") as f:
            f.writelines(cmds)

        from snakemake import snakemake
        snakemake(snakefile=str(self.snakefile), quiet=True, cores=cpus_per_task)

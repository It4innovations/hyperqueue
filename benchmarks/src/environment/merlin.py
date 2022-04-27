import dataclasses
import logging
import subprocess as sp
import argparse

from pathlib import Path
from typing import Any, Dict

from . import Environment, EnvironmentDescriptor
from .utils import EnvStateManager


@dataclasses.dataclass(frozen=True)
class MerlinClusterInfo:
    workdir: Path


class MerlinEnvironmentDescriptor(EnvironmentDescriptor):
    def create_environment(self, workdir: Path) -> Environment:
        info = MerlinClusterInfo(workdir)
        return MerlinEnvironment(info)

    def name(self) -> str:
        return "merlin"

    def parameters(self) -> Dict[str, Any]:
        return {}

    def metadata(self) -> Dict[str, Any]:
        return {}


class MerlinEnvironment(Environment, EnvStateManager):
    def __init__(self, info: MerlinClusterInfo):
        EnvStateManager.__init__(self)
        self.info = info
        self.merlinfile = info.workdir / "merlin"
        self.merlinsamples = info.workdir / "samples.csv"

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

    def submit(self, cmds: str):
        logging.info(f"Starting Merlin {cmds}")

        with open(self.merlinfile, "w") as f:
            f.write(
                "description:\n"
                "   name: Merlin Benchmark\n"
                "   description: Specific benchmark\n\n"
            )

            f.writelines(
                "merlin:\n"
                "   samples: \n"
                "      generate: \n"
                f"      file: {self.merlinsamples}\n"
                "      column_labels: [ID]\n\n"
            )

            f.writelines(cmds)

        sp.call(
            ["merlin", "run", "--local", self.merlinfile],
            shell=False,
            stdout=sp.DEVNULL,
            stderr=sp.STDOUT,
            cwd=self.info.workdir,
        )

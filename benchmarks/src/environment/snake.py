from typing import List

import dataclasses
import logging
import subprocess
import os
from pathlib import Path
from . import Environment


class ProfileMode:
    def __init__(self, server=False, workers=False, frequency=99):
        self.server = server
        self.workers = workers
        self.frequency = frequency


@dataclasses.dataclass(frozen=True)
class SnakeClusterInfo:
    workdir: Path


class SnakeEnvironment(Environment):
    def __init__(self, info: SnakeClusterInfo):
        self.info = info
        self.snakefile = "Snakefile"
        self.state = "initial"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def workdir(self) -> Path:
        return self.info.workdir

    def start(self):
        logging.info("Creating Snakefile")
        file = open(self.snakefile, "w")
        file.close()
        self.state = "started"

    def stop(self):
        logging.info("Deleting Snakefile")
        os.remove(self.snakefile)
        self.state = "stopped"

    def submit(self, args: List[str]):
        logging.info(f"Snake Submitting {args[0], args[1]}")
        file = open(self.snakefile, "a")
        file.writelines(args[0])
        file.close()

        result = subprocess.call([f'snakemake --quiet --cores {args[1]}'], shell=True, stdout=subprocess.PIPE)
        self.state = "submitted"
        return result

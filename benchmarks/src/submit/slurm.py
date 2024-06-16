import logging
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Callable, Optional

import dataclasses

from .utils import format_allocation_time, generate_job_dir


@dataclasses.dataclass
class SlurmOptions:
    name: str
    queue: str
    project: str
    walltime: timedelta
    init_script: Path
    workdir: Path
    node_count: int
    wait_for_job: bool


@dataclasses.dataclass
class SubmittedSlurmJob:
    id: int


def run_in_slurm(options: SlurmOptions, fn: Callable[[], None]) -> Optional[SubmittedSlurmJob]:
    if running_in_slurm():
        fn()
        return None
    else:
        venv_path = Path(os.environ["VIRTUAL_ENV"]).absolute() / "bin" / "activate"

        workdir = os.getcwd()
        alloc_dir = generate_job_dir(options.workdir)
        stdout = alloc_dir / "stdout"
        stderr = alloc_dir / "stderr"

        script = f"""#!/bin/bash
#SBATCH --job-name {options.name}
#SBATCH --time {format_allocation_time(options.walltime)}
#SBATCH -p {options.queue}
#SBATCH -A {options.project}
#SBATCH -N {options.node_count}
#SBATCH --output {stdout}
#SBATCH --error {stderr}

source {options.init_script}
source {venv_path}

cd {workdir} || exit 1
{sys.executable} -u {' '.join(sys.argv)}
"""
        alloc_dir.mkdir(parents=True, exist_ok=True)
        script_path = alloc_dir / "submit.sh"
        with open(script_path, "w") as f:
            f.write(script)
        logging.info(f"Submitting\n{script}\nfrom `{script_path}`")

        args = ["sbatch"]
        if options.wait_for_job:
            args.append("--wait")
        args.append(str(script_path))

        if options.wait_for_job:
            subprocess.check_call(args)
        else:
            output = subprocess.check_output(args)
            job_id = int(output.decode().strip().split(" ")[-1])

            with open(alloc_dir / "jobid", "w") as f:
                f.write(f"{str(job_id)}\n")

            return SubmittedSlurmJob(job_id)


def running_in_slurm():
    return "SLURM_JOB_NODELIST" in os.environ

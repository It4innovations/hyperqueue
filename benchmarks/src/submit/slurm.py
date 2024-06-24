import logging
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
import time
from typing import Callable

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


def run_in_slurm(options: SlurmOptions, fn: Callable[[], None]) -> SubmittedSlurmJob:
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
        args.append(str(script_path))

        output = subprocess.check_output(args)
        job_id = int(output.decode().strip().split(" ")[-1])

        with open(alloc_dir / "jobid", "w") as f:
            f.write(f"{str(job_id)}\n")

        if options.wait_for_job:
            try:
                while True:
                    if stderr.is_file():
                        break
                    time.sleep(1)
                print(f"Job {job_id} has started")

                tail_process = subprocess.Popen(["tail", "-f", stderr])
                while True:
                    status = get_job_status(job_id)
                    if status in ("CANCELLED", "COMPLETED", "COMPLETING", "FAILED"):
                        print(f"Job {job_id} has finished with status {status}")
                        break
                    time.sleep(10)
                tail_process.kill()
            except KeyboardInterrupt:
                print(f"Cancelling job {job_id}")
                subprocess.run(["scancel", str(job_id)])
        return SubmittedSlurmJob(job_id)


def get_job_status(job_id: int) -> str:
    output = subprocess.run(["scontrol", "show", "job", str(job_id)], stdout=subprocess.PIPE)
    assert output.returncode == 0
    for line in output.stdout.decode("utf-8").splitlines():
        line = line.strip().split()
        if line:
            line = line[0]
            if line.startswith("JobState="):
                return line[len("JobState=") :]


def running_in_slurm():
    return "SLURM_JOB_NODELIST" in os.environ

import datetime
import json
import logging
import os
import subprocess
from pathlib import Path
from typing import List, Optional

from ..benchmark.identifier import BenchmarkIdentifier
from .options import PBSSubmitOptions, serialize_submit_options

CURRENT_DIR = Path(__file__).absolute().parent
EXECUTE_SCRIPT_PATH = CURRENT_DIR / "execute_script.py"
assert EXECUTE_SCRIPT_PATH.is_file()


def serialize_identifiers(identifiers: List[BenchmarkIdentifier], path: Path):
    items = [identifier.to_dict() for identifier in identifiers]

    with open(path, "w") as f:
        json.dump(items, f)


def deserialize_identifiers(path: Path) -> List[BenchmarkIdentifier]:
    with open(path) as f:
        items = json.load(f)
    return [BenchmarkIdentifier.from_dict(item) for item in items]


def format_pbs_time(duration: datetime.timedelta) -> str:
    days, seconds = duration.days, duration.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    return f"{hours:02}:{minutes:02}:{seconds:02}"


def create_submit_script_header(directory: Path, options: PBSSubmitOptions):
    stdout = directory / "stdout"
    stderr = directory / "stderr"
    name = options.name or f"bench-{directory.name}"

    project = f"#PBS -A {options.project}" if options.project else ""

    return f"""#!/bin/bash
#PBS -l select={options.nodes},walltime={format_pbs_time(options.walltime)}
#PBS -q {options.queue}
#PBS -N {name}
#PBS -o {stdout}
#PBS -e {stderr}
{project}
""".strip()


def create_submit_script_body(
    header: str,
    options: PBSSubmitOptions,
    identifiers_path: Path,
    submit_options_path: Path,
    directory: Path,
    database_path: Optional[Path] = None,
    resubmit=False,
) -> str:
    workdir = Path(os.getcwd()).absolute()
    resubmit_flag = "--resubmit" if resubmit else "--no-resubmit"

    init_cmd = f"source {options.init_script.absolute()} || exit 1" if options.init_script else ""

    command = f"""{header}

{init_cmd}

cd {workdir} || exit 1
python {EXECUTE_SCRIPT_PATH} \\
  --directory {directory} \\
  --benchmarks {identifiers_path} \\
  --submit-options {submit_options_path} \\
  {resubmit_flag}"""
    if database_path:
        command += f""" \\
  --database-path {database_path}"""

    return command


def submit(
    identifiers: List[BenchmarkIdentifier],
    workdir: Path,
    options: PBSSubmitOptions,
    database_path: Optional[Path] = None,
    resubmit=False,
) -> str:
    workdir = Path(workdir).absolute()
    workdir.mkdir(parents=True, exist_ok=True)

    identifiers_path = workdir / "benchmarks.json"
    serialize_identifiers(identifiers, identifiers_path)

    submit_options_path = workdir / "submit-options.json"
    serialize_submit_options(options, submit_options_path)

    header = create_submit_script_header(workdir, options)
    script_body = create_submit_script_body(
        header,
        options,
        identifiers_path,
        submit_options_path,
        workdir,
        database_path=database_path,
        resubmit=resubmit,
    )

    script_path = workdir / "submit.sh"
    with open(script_path, "w") as f:
        f.write(script_body)

    logging.info(f"Submitting PBS script: {script_path}")
    logging.debug(f"Script body: {script_body}")
    result = subprocess.run(["qsub", script_path], stdout=subprocess.PIPE, check=True)
    job_id = result.stdout.decode().strip()
    logging.info(f"Job id: {job_id}")

    return job_id


def watch_pbs(job_id: str):
    subprocess.run(
        [
            "watch",
            "-n",
            "10",
            f"check-pbs-jobs --jobid {job_id} --print-job-err --print-job-out | tail -n 40",
        ]
    )

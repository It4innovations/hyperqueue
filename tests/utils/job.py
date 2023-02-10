import os
from typing import List, Optional

from ..conftest import HqEnv
from .table import Table


def default_task_output(
    job_id=1, task_id=0, type="stdout", working_dir: Optional[str] = None
) -> str:
    working_dir = working_dir if working_dir else os.getcwd()
    return f"{working_dir}/job-{job_id}/{task_id}.{type}"


def list_jobs(hq_env: HqEnv, all=True, filters: List[str] = None) -> Table:
    args = ["job", "list"]
    if all:
        assert filters is None
        args.append("--all")
    elif filters:
        args.extend(["--filter", ",".join(filters)])

    return hq_env.command(args, as_table=True)

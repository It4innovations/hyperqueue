import os
from typing import Optional


def default_task_output(
    job_id=1, task_id=0, type="stdout", submit_dir: Optional[str] = None
) -> str:
    submit_dir = submit_dir if submit_dir else os.getcwd()
    return f"{submit_dir}/job-{job_id}/{task_id}.{type}"

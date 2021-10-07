def default_task_output(job_id=1, task_id=0, type="stdout") -> str:
    return f"job-{job_id}/{task_id}.{type}"

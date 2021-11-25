from pathlib import Path


def generate_job_dir(workdir: Path) -> Path:
    """Tries to find a directory in `workdir` which name is an integer and return a large integer padded.
    The returned name is padded by zeros."""
    workdir.mkdir(parents=True, exist_ok=True)

    ids = []
    for item in workdir.iterdir():
        if item.is_dir():
            try:
                ids.append(int(item.name))
            except:
                pass
    max_id = max(ids or [0])
    dir_name = f"{max_id + 1:03}"
    return (workdir / dir_name).absolute()

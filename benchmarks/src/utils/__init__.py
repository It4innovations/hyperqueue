import os
from pathlib import Path


def get_pyenv_from_env() -> str:
    return os.environ.get("VIRTUAL_ENV")


def ensure_directory(path: Path) -> Path:
    if not path.is_dir():
        path = path.parent
    path.mkdir(parents=True, exist_ok=True)
    return path.absolute()

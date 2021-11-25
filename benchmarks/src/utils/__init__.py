import os
from pathlib import Path


def get_pyenv_from_env() -> str:
    return os.environ.get("VIRTUAL_ENV")


def ensure_directory(path: Path) -> Path:
    if not path.is_dir():
        path = path.parent
    path.mkdir(parents=True, exist_ok=True)
    return path.absolute()


def check_file_exists(path: Path):
    if not path.exists():
        raise Exception(f"Path {path} does not exist")
    if not path.is_file():
        raise Exception(f"Path {path} is not a file")

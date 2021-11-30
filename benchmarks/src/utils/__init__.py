import contextlib
import os
import shutil
from pathlib import Path


def get_pyenv_from_env() -> str:
    return os.environ.get("VIRTUAL_ENV")


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path.absolute()


def check_file_exists(path: Path):
    if not path.exists():
        raise Exception(f"Path {path} does not exist")
    if not path.is_file():
        raise Exception(f"Path {path} is not a file")


def is_binary_available(binary: str) -> bool:
    return shutil.which(binary) is not None


@contextlib.contextmanager
def activate_cwd(directory: Path):
    cwd = os.getcwd()
    os.chdir(directory)

    try:
        yield
    finally:
        os.chdir(cwd)

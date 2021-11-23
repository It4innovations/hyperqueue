import os


def get_pyenv_from_env() -> str:
    return os.environ.get("VIRTUAL_ENV")

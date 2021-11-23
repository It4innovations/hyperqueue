import contextlib
import logging
import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import List

import typer
from git import Repo

CURRENT_DIR = Path(__file__).absolute().parent
ROOT_DIR = CURRENT_DIR.parent
BUILD_DIR = ROOT_DIR / "target" / "release"

REPO = Repo(ROOT_DIR)

CURRENT_TAG = "current"


def binary_name(tag: str) -> str:
    return f"hq-{tag}"


def binary_path(tag: str) -> Path:
    return BUILD_DIR / binary_name(tag)


def binary_exists(tag: str) -> bool:
    if tag == CURRENT_TAG:
        return False
    return binary_path(tag).is_file()


@contextlib.contextmanager
def checkout_tag(tag: str):
    if tag == CURRENT_TAG:
        yield
    else:
        logging.info("Stashing repository")
        REPO.git.stash()
        try:
            logging.info(f"Checking out {tag}")
            REPO.git.checkout(tag)
            yield
        finally:
            logging.info("Reverting to original state")
            REPO.git.stash("pop")


def build_tag(tag: str) -> Path:
    if binary_exists(tag):
        logging.info(f"{tag} already built at {binary_path(tag)}")
        return binary_path(tag)

    with checkout_tag(tag):
        env = os.environ.copy()
        env["RUSTFLAGS"] = "-C target-cpu=native"
        subprocess.run(["cargo", "build", "--release"], env=env, check=True)
        built_binary = BUILD_DIR / "hq"
        target = binary_path(tag)
        shutil.copyfile(built_binary, target)
        os.chmod(target, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)


def build(tags: List[str]):
    """
    Builds HQ binaries from the passed git references.
    """
    if CURRENT_TAG in tags:
        tags = [CURRENT_TAG] + sorted(set(tags) - {CURRENT_TAG})
    else:
        tags = sorted(tags)

    for tag in tags:
        logging.info(f"Building {tag}")
        build_tag(tag)
        logging.info(f"Built {tag}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    typer.run(build)

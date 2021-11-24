import logging
import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import List

import dataclasses

from .repository import TAG_WORKSPACE, checkout_tag, resolve_tag
from .. import ROOT_DIR


@dataclasses.dataclass
class BuildOptions:
    release: bool = True


def get_build_dir(options: BuildOptions) -> Path:
    path = ROOT_DIR / "target"
    if options.release:
        return path / "release"
    return path / "debug"


def binary_name(tag: str) -> str:
    return f"hq-{tag}"


def binary_path(tag: str, options: BuildOptions) -> Path:
    return get_build_dir(options) / binary_name(tag)


def build_tag(tag: str, options: BuildOptions) -> Path:
    path = binary_path(tag, options).absolute()
    is_workspace = tag == TAG_WORKSPACE

    if not is_workspace and path.is_file():
        logging.info(f"{tag} already built at {path}")
        return path

    with checkout_tag(tag):
        logging.info(f"Building {tag}")
        env = os.environ.copy()
        args = ["cargo", "build"]
        if options.release:
            env["RUSTFLAGS"] = "-C target-cpu=native"
            args.append("--release")

        subprocess.run(args, env=env, check=True)
        built_binary = get_build_dir(options) / "hq"
        assert built_binary.is_file()

        shutil.copyfile(built_binary, path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        logging.info(f"Building {tag} finished")
    return path


def iterate_binaries(tags: List[str], options=None):
    """
    Iterate HyperQueue binaries from the given git `tags`.
    If the binary is not build yet, it will be compiled first.
    """
    options = options or BuildOptions()
    for tag in tags:
        ref = resolve_tag(tag)
        logging.info(f"Resolved tag {tag} to {ref}")
        yield build_tag(ref, options)

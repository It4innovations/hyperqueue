import logging
import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import List, Iterator, Tuple

import dataclasses

from .repository import TAG_WORKSPACE, checkout_tag, resolve_tag
from .. import ROOT_DIR


@dataclasses.dataclass
class BuildConfig:
    git_ref: str = TAG_WORKSPACE
    release: bool = True
    zero_worker: bool = False


def get_build_dir(options: BuildConfig) -> Path:
    path = ROOT_DIR / "target"
    if options.release:
        return path / "release"
    return path / "debug"


def binary_name(options: BuildConfig, resolved_ref: str) -> str:
    name = f"hq-{resolved_ref}"
    if not options.release:
        name += "-debug"
    if options.zero_worker:
        name += "-zw"
    return name


def binary_path(options: BuildConfig, resolved_ref: str) -> Path:
    return get_build_dir(options) / binary_name(options, resolved_ref)


def build_tag(config: BuildConfig, resolved_ref: str) -> Path:
    path = binary_path(config, resolved_ref).absolute()
    tag = config.git_ref
    is_workspace = tag == TAG_WORKSPACE

    if not is_workspace and path.is_file():
        logging.info(f"{tag} already built at {path}")
        return path

    build_description = f"{tag} (release={config.release}, zw={config.zero_worker})"
    with checkout_tag(tag):
        logging.info(f"Building {build_description}")
        env = os.environ.copy()
        args = ["cargo", "build"]
        if config.release:
            env["RUSTFLAGS"] = "-C target-cpu=native"
            args += ["--release"]
        if config.zero_worker:
            args += ["--features", "zero-worker"]

        subprocess.run(args, env=env, check=True)
        built_binary = get_build_dir(config) / "hq"
        assert built_binary.is_file()

        shutil.copyfile(built_binary, path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        logging.info(f"Building {build_description} finished")
    return path


def iterate_binaries(configs: List[BuildConfig]) -> Iterator[Tuple[BuildConfig, str]]:
    """
    Iterate HyperQueue binaries from the given build configurations.
    If the binary is not built yet, it will be compiled first.
    """
    # Make sure that the current version is compiled first
    # Make sure that zero-worker is build directly after/before the non zero-worker variant
    def sort_key(git_ref: str) -> str:
        if git_ref == TAG_WORKSPACE:
            return ""
        return git_ref

    configs = sorted(configs, key=lambda c: (sort_key(c.git_ref), c.zero_worker))

    for config in configs:
        ref = resolve_tag(config.git_ref)
        logging.info(f"Resolved tag {config.git_ref} to {ref}")
        yield (config, build_tag(config, ref))

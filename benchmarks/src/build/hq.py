import enum
import logging
import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import List, Iterable

import dataclasses

from .repository import TAG_WORKSPACE, checkout_tag, resolve_tag
from .. import ROOT_DIR


class Profile(enum.Enum):
    Debug = 0
    Release = 1
    Dist = 2

    def name(self) -> str:
        if self == Profile.Debug:
            return "dev"
        elif self == Profile.Release:
            return "release"
        elif self == Profile.Dist:
            return "dist"
        else:
            assert False

    def target_name(self) -> str:
        if self == Profile.Debug:
            return "debug"
        elif self == Profile.Release:
            return "release"
        elif self == Profile.Dist:
            return "dist"
        else:
            assert False

    def flags(self) -> List[str]:
        if self == Profile.Debug:
            return []
        elif self == Profile.Release:
            return ["--release"]
        elif self == Profile.Dist:
            return ["--profile", "dist"]
        else:
            assert False


@dataclasses.dataclass
class BuildConfig:
    git_ref: str = TAG_WORKSPACE
    profile: Profile = Profile.Release
    zero_worker: bool = False
    jemalloc: bool = False
    debug_symbols: bool = False


@dataclasses.dataclass
class BuiltBinary:
    config: BuildConfig
    binary_path: Path


def get_build_dir(options: BuildConfig) -> Path:
    path = ROOT_DIR / "target"
    return path / options.profile.target_name()


def binary_name(options: BuildConfig, resolved_ref: str) -> str:
    name = f"hq-{resolved_ref}-{options.profile.name()}"
    if options.zero_worker:
        name += "-zw"
    if options.jemalloc:
        name += "-jemalloc"
    if options.debug_symbols:
        name += "-symbols"
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

    build_description = f"{tag} (profile={config.profile.name()}, zero_worker={config.zero_worker}, debug_symbols={config.debug_symbols})"
    with checkout_tag(tag):
        logging.info(f"Building {build_description}")
        env = os.environ.copy()

        if config.debug_symbols:
            profile_name = config.profile.name().upper()
            env[f"CARGO_PROFILE_{profile_name}_DEBUG"] = "line-tables-only"

        args = ["cargo", "build"]
        args += config.profile.flags()
        if config.profile != Profile.Debug:
            env["RUSTFLAGS"] = "-C target-cpu=native"
        args += ["--no-default-features"]
        if config.jemalloc:
            args += ["--features", "jemalloc"]
        if config.zero_worker:
            args += ["--features", "zero-worker"]

        subprocess.run(args, env=env, check=True)
        built_binary = get_build_dir(config) / "hq"
        assert built_binary.is_file()

        subprocess.run(["killall", os.path.basename(path)], check=False)
        shutil.copyfile(built_binary, path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        logging.info(f"Building {build_description} finished")
    return path


def iterate_binaries(configs: List[BuildConfig]) -> Iterable[BuiltBinary]:
    """
    Iterate HyperQueue binaries from the given build configurations.
    If the binary is not built yet, it will be compiled first.
    """

    # Make sure that the current version is compiled first
    # Make sure that zero-worker is built directly after/before the non zero-worker variant
    def sort_key(git_ref: str) -> str:
        if git_ref == TAG_WORKSPACE:
            return ""
        return git_ref

    configs = sorted(configs, key=lambda c: (sort_key(c.git_ref), c.zero_worker, c.jemalloc))

    for config in configs:
        ref = resolve_tag(config.git_ref)
        logging.info(f"Resolved tag {config.git_ref} to {ref}")
        yield BuiltBinary(config=config, binary_path=build_tag(config, ref))

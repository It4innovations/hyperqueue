#!/usr/bin/env python3
"""Obtain the two HyperQueue binaries the paper compares.

* ``v0.25.1`` -- the last release before the MILP scheduler, taken from the official
  published tarball rather than a local build, so the comparison is against the version as
  actually shipped to users.
* ``head``    -- the committed HEAD of this repository (the ``eval-scripts`` branch), built with
  ``--profile dist`` in a throwaway git worktree (``--build-local``). The branch is ``main`` plus
  the evaluation harness; its scheduler runs with production defaults unless a run sets one of
  the ``HQ_SCHED_*`` variables read by ``SchedulerConfig::from_env``, which only the prefill
  sweep does.

Both are ``dist`` (release + LTO + ``codegen-units=1``) builds. That is not cosmetic: the
published tarball is an LTO build, so a plain ``--release`` build of HEAD would carry no LTO
and would handicap the new scheduler in exactly the CPU-bound measurements this benchmark
reports.
"""

import argparse
import os
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

REPO = "It4innovations/hyperqueue"
#: Arms obtained from official release tarballs.
VERSIONS = ["v0.25.1"]
#: Arms built from this repository.
LOCAL_ARMS = ["head"]
HERE = Path(__file__).resolve().parent
BIN_DIR = HERE / "bin"
REPO_ROOT = HERE.parents[2]

#: The paper's arms, and the default matrix.
ARMS = VERSIONS + LOCAL_ARMS
#: Everything that may be named by ``--version``.
ALL_ARMS = ARMS


def asset_url(version: str) -> str:
    return f"https://github.com/{REPO}/releases/download/{version}/hq-{version}-linux-x64.tar.gz"


def fetch(version: str, force: bool) -> Path:
    target = BIN_DIR / f"hq-{version}"
    if target.exists() and not force:
        print(f"{target} already exists, skipping download")
        return target

    url = asset_url(version)
    print(f"Downloading {url}")
    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / "hq.tar.gz"
        urllib.request.urlretrieve(url, archive)
        with tarfile.open(archive) as tf:
            member = tf.getmember("hq")
            extracted = tf.extractfile(member)
            assert extracted is not None
            BIN_DIR.mkdir(parents=True, exist_ok=True)
            with open(target, "wb") as f:
                f.write(extracted.read())
    target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return target


def verify(binary: Path, version: str) -> None:
    out = subprocess.run([str(binary), "--version"], capture_output=True, text=True, check=True).stdout.strip()
    print(f"{binary.name}: {out}")
    expected = version.lstrip("v")
    if expected not in out:
        raise SystemExit(f"Version mismatch: expected {expected} in {out!r}")


def binary_for(version: str) -> Path:
    """Path of the (already fetched or built) binary for a version."""
    return BIN_DIR / f"hq-{version}"


def _cargo_build(source_dir: Path, target_dir: Path) -> Path:
    subprocess.run(
        ["cargo", "build", "--profile", "dist"],
        cwd=source_dir,
        env={**os.environ, "CARGO_TARGET_DIR": str(target_dir)},
        check=True,
    )
    return target_dir / "dist" / "hq"


def head_sha() -> str:
    """Commit the ``head`` arm is (or would be) built from.

    Recorded in every run's ``meta.json`` so a results directory says which build produced
    it, rather than relying on the mtime of ``bin/hq-head``.
    """
    out = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


def build_local(arm: str, force: bool) -> Path:
    target = binary_for(arm)
    if target.exists() and not force:
        print(f"{target} already exists, skipping build")
        return target
    BIN_DIR.mkdir(parents=True, exist_ok=True)

    if arm != "head":
        raise SystemExit(f"unknown local arm: {arm}")

    # A detached worktree at HEAD, so the live working tree is never touched and uncommitted
    # changes can never leak into a measured arm. This needs its own target dir, so it is a
    # cold build. Deliberately *not* `benchmarks/src/build/repository.py`, which does
    # `git stash` + `git checkout` on the live repo.
    with tempfile.TemporaryDirectory() as tmp:
        tree = Path(tmp) / "head"
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(tree), "HEAD"],
            cwd=REPO_ROOT,
            check=True,
        )
        try:
            built = _cargo_build(tree, Path(tmp) / "target")
            shutil.copy2(built, target)
        finally:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(tree)],
                cwd=REPO_ROOT,
                check=False,
            )
    (BIN_DIR / "hq-head.sha").write_text(head_sha() + "\n")
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="re-download/rebuild even if present")
    parser.add_argument("--version", action="append", dest="versions", help="limit to an arm")
    parser.add_argument(
        "--build-local",
        action="store_true",
        help="also build the 'head' arm from this repository (--profile dist, in a git worktree)",
    )
    args = parser.parse_args()

    # An explicit --version may name either kind of arm, so route each one to the right path
    # rather than trying to download a tarball for `head`.
    requested = args.versions or VERSIONS
    unknown = [v for v in requested if v not in ALL_ARMS]
    if unknown:
        raise SystemExit(f"unknown arm(s): {', '.join(unknown)} (known: {', '.join(ALL_ARMS)})")

    for version in [v for v in requested if v in VERSIONS]:
        binary = fetch(version, args.force)
        verify(binary, version)

    local = [a for a in requested if a in LOCAL_ARMS]
    if args.build_local:
        local = LOCAL_ARMS if not args.versions else local
    for arm in local:
        binary = build_local(arm, args.force)
        out = subprocess.run([str(binary), "--version"], capture_output=True, text=True, check=True).stdout.strip()
        print(f"{binary.name}: {out} ({head_sha()[:12]})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

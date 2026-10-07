"""Exercise journal directory environment variables with the real HQ CLI, without workers or Slurm."""

import argparse
import contextlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def run(binary, root, env, *args, check=True):
    return subprocess.run(
        [str(binary), *args], cwd=root, env=env, capture_output=True, text=True, check=check, timeout=10
    )


@contextlib.contextmanager
def server(binary, root, env, *args):
    with (root / "server.log").open("w") as log:
        process = subprocess.Popen(
            [str(binary), "server", "start", *args], cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            deadline = time.monotonic() + 10
            while run(binary, root, env, "server", "info", check=False).returncode:
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError((root / "server.log").read_text())
                time.sleep(0.05)
            yield
        finally:
            if process.poll() is None:
                run(binary, root, env, "server", "stop", check=False)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            assert process.returncode == 0, (root / "server.log").read_text()


def info(binary, root, env):
    return json.loads(run(binary, root, env, "--output-mode=json", "server", "info").stdout)


def check_environment_and_restore(binary, root, env):
    journal = Path(env["HQ_JOURNAL_DIR"]) / "hq.journal"
    assert not journal.parent.exists()
    assert not Path(env["HQ_SERVER_DIR"]).exists()
    with server(binary, root, env):
        assert info(binary, root, env)["journal_path"] == str(journal)
        assert journal.is_file()
        assert (Path(env["HQ_SERVER_DIR"]) / "hq-current").is_dir()
        # This stays pending: the regression starts no workers and submits no Slurm jobs.
        run(binary, root, env, "submit", "--", "true")
    with server(binary, root, env):
        jobs = json.loads(run(binary, root, env, "--output-mode=json", "job", "list").stdout)
        assert len(jobs) == 1, jobs
        assert jobs[0]["id"] == 1, jobs
        assert jobs[0]["task_stats"]["waiting"] == 1, jobs
        assert info(binary, root, env)["journal_path"] == str(journal)
    assert journal.stat().st_size > 0


def check_explicit_file(binary, root, env):
    journal = root / "explicit.journal"
    with server(binary, root, env, "--journal", str(journal)):
        assert info(binary, root, env)["journal_path"] == str(journal)
        assert journal.is_file()
        assert not Path(env["HQ_JOURNAL_DIR"]).exists()


def check_directory_option(binary, root, env):
    directory = root / "cli-journal"
    with server(binary, root, env, "--journal-dir", str(directory)):
        assert info(binary, root, env)["journal_path"] == str(directory / "hq.journal")
        assert (directory / "hq.journal").is_file()
        assert not Path(env["HQ_JOURNAL_DIR"]).exists()


def check_disabled(binary, root, env):
    with server(binary, root, env, "--no-journal"):
        assert info(binary, root, env)["journal_path"] is None
        assert not Path(env["HQ_JOURNAL_DIR"]).exists()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("hq", type=Path, help="Path to the compiled HQ binary")
    binary = parser.parse_args().hq.resolve()
    checks = (check_environment_and_restore, check_explicit_file, check_directory_option, check_disabled)
    for check in checks:
        with tempfile.TemporaryDirectory(prefix="hq-journal-regression-") as directory:
            root = Path(directory)
            env = dict(
                os.environ,
                HQ_SERVER_DIR=str(root / "server"),
                HQ_JOURNAL_DIR=str(root / "nested" / "journal"),
            )
            check(binary, root, env)
            print(f"PASS: {check.__name__}")


if __name__ == "__main__":
    main()

# This scripts writes all places where version has to be updated when a new release is made

import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def check_file(path, prefix, case_sensitive=False):
    with open(os.path.join(ROOT_DIR, path), "r") as f:
        for line in f:
            line = line.strip()
            if case_sensitive:
                line = line.lower()
            if line.startswith(prefix):
                print(path, ":", line)


check_file("crates/hyperqueue/Cargo.toml", "version")
check_file("crates/pyhq/Cargo.toml", "version")
check_file("nedoc.conf", "project_version")
check_file("CHANGELOG.md", "# dev", case_sensitive=True)

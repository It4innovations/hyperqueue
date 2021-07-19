import json
import os
import subprocess

import pytest

from .conftest import HqEnv
from .utils import parse_table


def test_server_host(hq_env: HqEnv):
    args = hq_env.server_args()
    args += ["--host", "abcd123"]
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    try:
        stdout, stderr = p.communicate(timeout=0.5)
    except subprocess.TimeoutExpired:
        p.kill()
        stdout, stderr = p.communicate()
    stdout = stdout.decode()
    table = parse_table(stdout)
    assert table[1][1] == "abcd123"


def test_version_mismatch(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command("jobs", as_table=True)

    access_file = os.path.join(hq_env.server_dir, "hq-current", "access.json")

    with open(access_file) as f:
        data = json.load(f)

    version = data["version"]
    data["version"] += ".1"

    # Make the file writable
    os.chmod(access_file, 0o600)

    with open(access_file, "w") as f:
        json.dump(data, f)

    with pytest.raises(
        Exception,
        match=f"Server was started with version {version}.1, but the current version is {version}",
    ):
        hq_env.command("jobs", as_table=True)


def test_server_info(hq_env: HqEnv):
    hq_env.start_server()
    table = hq_env.command(["server", "info"], as_table=True)
    assert len(table) == 7

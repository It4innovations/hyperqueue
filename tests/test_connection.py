import json
import os

import pytest

from .conftest import HqEnv


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

    with pytest.raises(Exception,
                       match=f"Server was started with version {version}.1, but the current version is {version}"):
        hq_env.command("jobs", as_table=True)

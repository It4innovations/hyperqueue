import json
import os
import signal
import socket
import subprocess
from typing import List

import pytest

from .conftest import HqEnv
from .utils import parse_table
from .utils.io import find_free_port
from .utils.table import Table


def start_server_get_output(hq_env: HqEnv, args: List[str]) -> Table:
    command = hq_env.server_args()
    command += args
    p = subprocess.Popen(command, stdout=subprocess.PIPE)
    try:
        stdout, stderr = p.communicate(timeout=0.5)
    except subprocess.TimeoutExpired:
        p.kill()
        stdout, stderr = p.communicate()
    stdout = stdout.decode()
    return parse_table(stdout)


def test_server_host(hq_env: HqEnv):
    table = start_server_get_output(hq_env, ["--host", "abcd123"])
    table.check_row_value("Host", "abcd123")


def test_server_client_port(hq_env: HqEnv):
    port = str(find_free_port())
    table = start_server_get_output(hq_env, ["--client-port", port])
    table.check_row_value("HQ port", port)


def test_server_worker_port(hq_env: HqEnv):
    port = str(find_free_port())
    table = start_server_get_output(hq_env, ["--worker-port", port])
    table.check_row_value("Workers port", port)


def test_version_mismatch(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["job", "list"], as_table=True)

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
        hq_env.command(["job", "list"], as_table=True)


def test_server_info(hq_env: HqEnv):
    process = hq_env.start_server()

    table = hq_env.command(["server", "info"], as_table=True)
    table.check_row_value("Server directory", hq_env.server_dir)
    table.check_row_value("Host", socket.gethostname())
    table.check_row_value("Pid", str(process.pid))
    server_uid = table.get_row_value("Server UID")
    assert len(server_uid) == 6
    assert server_uid.isalnum()

    assert len(table) == 8


def test_server_stop(hq_env: HqEnv):
    process = hq_env.start_server()
    hq_env.command(["server", "stop"])
    process.wait()
    hq_env.check_process_exited(process, 0)


def test_delete_symlink_after_server_stop(hq_env: HqEnv):
    process = hq_env.start_server()

    symlink_path = os.path.join(hq_env.server_dir, "hq-current")
    assert os.path.isdir(symlink_path)

    rundir_path = os.path.realpath(symlink_path)

    hq_env.command(["server", "stop"])
    process.wait()
    hq_env.check_process_exited(process, 0)

    assert not os.path.isdir(os.path.join(hq_env.server_dir, "hq-current"))
    assert os.path.isdir(rundir_path)


def test_delete_symlink_after_ctrl_c(hq_env: HqEnv):
    process = hq_env.start_server()
    process.send_signal(signal.SIGINT)

    process.wait()
    hq_env.check_process_exited(process, 0)

    assert not os.path.isdir(os.path.join(hq_env.server_dir, "hq-current"))

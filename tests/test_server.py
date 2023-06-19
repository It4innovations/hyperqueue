import json
import os
import random
import signal
import socket
import subprocess
from typing import List

import pytest
from schema import Schema

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
    table.check_row_value("Worker host", "abcd123")
    table.check_row_value("Client host", "abcd123")


def test_server_client_port(hq_env: HqEnv):
    port = str(find_free_port())
    table = start_server_get_output(hq_env, ["--client-port", port])
    table.check_row_value("Client port", port)


def test_server_worker_port(hq_env: HqEnv):
    port = str(find_free_port())
    table = start_server_get_output(hq_env, ["--worker-port", port])
    table.check_row_value("Worker port", port)


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
    table.check_row_value("Client host", socket.gethostname())
    table.check_row_value("Worker host", socket.gethostname())
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


def test_generate_access(hq_env: HqEnv):
    client_port = 18000 + random.randint(0, 1000)
    worker_port = 14000 + random.randint(0, 1000)
    hq_env.command(
        [
            "server",
            "generate-access",
            "myaccess.json",
            f"--worker-port={worker_port}",
            f"--client-port={client_port}",
        ],
        use_server_dir=False,
    )

    with open("myaccess.json", "r") as f:
        access = json.load(f)

    schema = Schema(
        {
            "version": str,
            "server_uid": str,
            "worker": {"host": str, "port": worker_port, "secret_key": str},
            "client": {"host": str, "port": client_port, "secret_key": str},
        },
    )
    schema.validate(access)

    hq_env.start_server(args=["--access-file=myaccess.json"])

    with open("hq-server/hq-current/access.json", "r") as f:
        access2 = json.load(f)
    assert access == access2


def test_generate_partial_access(hq_env: HqEnv):
    client_port = 18000 + random.randint(0, 1000)
    worker_port = 14000 + random.randint(0, 1000)
    hq_env.command(
        [
            "server",
            "generate-access",
            "myaccess.json",
            f"--worker-port={worker_port}",
            f"--client-port={client_port}",
            "--client-file=client.json",
            "--worker-file=worker.json",
        ],
        use_server_dir=False,
    )

    with open("client.json", "r") as f:
        access = json.load(f)

    schema = Schema(
        {
            "version": str,
            "client": {"host": str, "port": client_port, "secret_key": str},
        },
    )
    schema.validate(access)

    with open("worker.json", "r") as f:
        access = json.load(f)

    schema = Schema(
        {
            "version": str,
            "worker": {"host": str, "port": worker_port, "secret_key": str},
        },
    )
    schema.validate(access)

    hq_env.start_server(args=["--access-file=myaccess.json"])

    WORKER_SD = "acc/worker"
    os.makedirs(os.path.join(WORKER_SD, "hq-current"))
    os.rename("worker.json", os.path.join(WORKER_SD, "hq-current", "access.json"))
    hq_env.start_worker(server_dir=WORKER_SD)

    CLIENT_SD = "acc/client"
    os.makedirs(os.path.join(CLIENT_SD, "hq-current"))
    os.rename("client.json", os.path.join(CLIENT_SD, "hq-current", "access.json"))

    result = hq_env.command(
        [f"--server-dir={CLIENT_SD}", "worker", "list"],
        use_server_dir=False,
        as_table=True,
    )
    assert result.get_row_value("1") == "RUNNING"


def test_generate_partial_access_hosts(hq_env: HqEnv):
    client_port = 18000 + random.randint(0, 1000)
    worker_port = 14000 + random.randint(0, 1000)
    hq_env.command(
        [
            "server",
            "generate-access",
            "myaccess.json",
            f"--worker-port={worker_port}",
            f"--client-port={client_port}",
            "--client-file=client.json",
            "--worker-file=worker.json",
            "--worker-host=192.168.1.1",
            "--client-host=baf.bar.cz",
        ],
        use_server_dir=False,
    )

    with open("client.json", "r") as f:
        access = json.load(f)

    schema = Schema(
        {
            "version": str,
            "client": {"host": "baf.bar.cz", "port": client_port, "secret_key": str},
        },
    )
    schema.validate(access)

    with open("worker.json", "r") as f:
        access = json.load(f)

    schema = Schema(
        {
            "version": str,
            "worker": {"host": "192.168.1.1", "port": worker_port, "secret_key": str},
        },
    )
    schema.validate(access)

    hq_env.start_server(args=["--access-file=myaccess.json"])

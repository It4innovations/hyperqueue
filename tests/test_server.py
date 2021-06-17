import subprocess

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

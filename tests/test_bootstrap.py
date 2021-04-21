import subprocess
import time

from conftest import HQ_BINARY


def test_start_nonexistent_dir(tmpdir):
    dir = tmpdir / "hq"
    process = subprocess.Popen([HQ_BINARY, "start", "--rundir", dir])
    time.sleep(1)
    res = process.poll()
    a = 5


# TODO: stop from different hostname

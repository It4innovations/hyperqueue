import functools
import inspect
import shutil
from datetime import datetime
from pathlib import Path

import pytest

from ..conftest import run_hq_env

PBS_AVAILABLE = shutil.which("qsub") is not None
PBS_TIMEOUT = 5 * 60


@pytest.fixture(autouse=False, scope="function")
def pbs_hq_env(request):
    # We cannot create server directory in /tmp, because it wouldn't be available to compute nodes
    testname = request.node.name
    now = datetime.now()
    directory = (
        Path.home() / ".hq-pytest" / f"{testname}-{now.strftime('%d-%m-%y-%H-%M-%S')}"
    )
    directory.mkdir(parents=True, exist_ok=False)
    with run_hq_env(directory) as env:
        try:
            yield env
        finally:
            env.stop_server()


def pbs_test(fn):
    signature = inspect.signature(fn)
    if "hq_env" in signature.parameters:
        raise Exception(
            "Do not use the `hq_env` fixture with `pbs_test`. Use `pbs_hq_env` instead."
        )

    @pytest.mark.skipif(
        not PBS_AVAILABLE, reason="This test can be run only if PBS is available"
    )
    @pytest.mark.pbs
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapped

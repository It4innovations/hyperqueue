import contextlib
import functools
import inspect
import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest

from ..conftest import run_hq_env

PBS_AVAILABLE = shutil.which("qsub") is not None
PBS_TIMEOUT = 60 * 60

SLURM_AVAILABLE = shutil.which("sbatch") is not None
SLURM_TIMEOUT = 60 * 60


@pytest.fixture(scope="session")
def pbs_credentials() -> str:
    """
    Returns credentials passed to `hq alloc pbs add` additional arguments.
    """
    args = []
    if "PBS_QUEUE" in args:
        args += [f"-q{os.environ['PBS_QUEUE']}"]
    return " ".join(args)


@contextlib.contextmanager
def create_env_in_home(request):
    # We cannot create server directory in /tmp, because it wouldn't be available to compute nodes
    testname = request.node.name
    now = datetime.now()
    directory = Path.home() / ".hq-pytest" / f"{testname}-{now.strftime('%d-%m-%y-%H-%M-%S')}"
    directory.mkdir(parents=True, exist_ok=False)
    with run_hq_env(directory) as env:
        try:
            yield env
        finally:
            env.stop_server()


@pytest.fixture(autouse=False, scope="function")
def cluster_hq_env(request):
    with create_env_in_home(request) as env:
        yield env


def pbs_test(fn):
    signature = inspect.signature(fn)
    if "hq_env" in signature.parameters:
        raise Exception("Do not use the `hq_env` fixture with `pbs_test`. Use `cluster_hq_env` instead.")

    @pytest.mark.skipif(not PBS_AVAILABLE, reason="This test can be run only if PBS is available")
    @pytest.mark.pbs
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapped


@pytest.fixture(scope="session")
def slurm_credentials() -> str:
    """
    Returns credentials passed to `hq alloc pbs slurm` additional arguments.
    """
    args = []
    if "SLURM_ACCOUNT" in os.environ:
        args += ["--account", os.environ["SLURM_ACCOUNT"]]
    if "SLURM_PARTITION" in os.environ:
        args += ["--partition", os.environ["SLURM_PARTITION"]]
    return " ".join(args)


def slurm_test(fn):
    signature = inspect.signature(fn)
    if "hq_env" in signature.parameters:
        raise Exception("Do not use the `hq_env` fixture with `slurm_test`. Use `cluster_hq_env` instead.")

    @pytest.mark.skipif(not SLURM_AVAILABLE, reason="This test can be run only if SLURM is available")
    @pytest.mark.slurm
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapped

from typing import List

import pytest

from ..conftest import HqEnv
from .conftest import PBS_AVAILABLE
from .flavor import PbsManagerFlavor, all_flavors, ManagerFlavor
from .mock.manager import JobId, ManagerException, default_job_id, CommandHandler
from .mock.mock import MockJobManager
from .utils import add_queue


@pytest.mark.skipif(PBS_AVAILABLE, reason="This test will not work properly if `qsub` is available")
def test_pbs_dry_run_missing_qsub(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        dry_run_cmd(PbsManagerFlavor()),
        expect_fail="Could not submit allocation: qsub start failed",
    )


@all_flavors
def test_dry_run_submit_error(hq_env: HqEnv, flavor: ManagerFlavor):
    class Manager(CommandHandler):
        async def handle_submit(self) -> JobId:
            raise ManagerException("FOOBAR")

    with MockJobManager(hq_env, Manager(flavor.create_adapter())):
        hq_env.start_server()
        hq_env.command(dry_run_cmd(flavor), expect_fail="Stderr: FOOBAR")


@all_flavors
def test_dry_run_cancel_error(hq_env: HqEnv, flavor: ManagerFlavor):
    class Manager(CommandHandler):
        async def handle_delete(self, job_id: JobId):
            raise ManagerException()

    with MockJobManager(hq_env, Manager(flavor.create_adapter())):
        hq_env.start_server()
        hq_env.command(
            dry_run_cmd(flavor),
            expect_fail=f"Could not cancel allocation {default_job_id()}",
        )


@all_flavors
def test_dry_run_success(hq_env: HqEnv, flavor: ManagerFlavor):
    with MockJobManager(hq_env, flavor.default_handler()):
        hq_env.start_server()
        hq_env.command(dry_run_cmd(flavor))


@all_flavors
def test_add_queue_dry_run_fail(hq_env: HqEnv, flavor: ManagerFlavor):
    class Manager(CommandHandler):
        async def handle_submit(self) -> JobId:
            raise ManagerException()

    with MockJobManager(hq_env, Manager(flavor.create_adapter())):
        hq_env.start_server()
        add_queue(
            hq_env,
            manager=flavor.manager_type(),
            dry_run=True,
            expect_fail=f"Could not submit allocation: {flavor.submit_program_name()} execution failed",
        )


def dry_run_cmd(flavor) -> List[str]:
    return ["alloc", "dry-run", flavor.manager_type(), "--time-limit", "1h"]

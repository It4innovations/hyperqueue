import pytest

from ..conftest import HqEnv
from .utils import add_queue, remove_queue


def test_autoalloc_descriptor_list(hq_env: HqEnv):
    hq_env.start_server()
    add_queue(hq_env, name=None, backlog=5)

    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(
        (
            "ID",
            "Backlog size",
            "Workers per alloc",
            "Timelimit",
            "Manager",
            "Name",
        ),
        0,
        ("1", "5", "1", "1h", "PBS", ""),
    )

    add_queue(
        hq_env,
        manager="pbs",
        name="bar",
        backlog=1,
        workers_per_alloc=2,
        time_limit="1h",
    )
    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(
        (
            "ID",
            "Backlog size",
            "Workers per alloc",
            "Timelimit",
            "Name",
        ),
        1,
        ("2", "1", "2", "1h", "bar"),
    )

    add_queue(hq_env, manager="slurm", backlog=1)
    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(("ID", "Manager"), 2, ("3", "SLURM"))


@pytest.mark.parametrize("manager", ("pbs", "slurm"))
def test_timelimit_hms(hq_env: HqEnv, manager: str):
    hq_env.start_server()
    add_queue(hq_env, manager=manager, time_limit="01:10:15")

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("Timelimit", 0, "1h 10m 15s")


@pytest.mark.parametrize("manager", ("pbs", "slurm"))
def test_timelimit_human_format(hq_env: HqEnv, manager: str):
    hq_env.start_server()
    add_queue(hq_env, manager=manager, time_limit="3h 15m 10s")

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("Timelimit", 0, "3h 15m 10s")


@pytest.mark.parametrize("manager", ("pbs", "slurm"))
def test_require_timelimit(hq_env: HqEnv, manager: str):
    hq_env.start_server()
    add_queue(
        hq_env,
        manager=manager,
        time_limit=None,
        expect_fail="--time-limit <TIME_LIMIT>",
    )


def test_remove_descriptor(hq_env: HqEnv):
    hq_env.start_server()
    add_queue(hq_env)
    add_queue(hq_env)
    add_queue(hq_env)

    result = remove_queue(hq_env, queue_id=2)
    assert "Allocation queue 2 successfully removed" in result

    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(["ID"], 0, ["1"])
    table.check_columns_value(["ID"], 1, ["3"])

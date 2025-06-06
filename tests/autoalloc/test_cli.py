from ..conftest import HqEnv
from .flavor import ManagerFlavor, all_flavors
from .utils import (
    add_queue,
    pause_queue,
    remove_queue,
    resume_queue,
)


@all_flavors
def test_autoalloc_queue_list(hq_env: HqEnv, flavor: ManagerFlavor):
    hq_env.start_server()
    add_queue(hq_env, manager=flavor.manager_type(), name=None, backlog=5)

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
        ("1", "5", "1", "1h", flavor.manager_type().upper(), ""),
    )

    add_queue(
        hq_env,
        manager=flavor.manager_type(),
        name="bar",
        backlog=1,
        workers_per_alloc=2,
        time_limit="1h",
    )
    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(
        ("ID", "Backlog size", "Workers per alloc", "Timelimit", "Name", "Manager"),
        1,
        ("2", "1", "2", "1h", "bar", flavor.manager_type().upper()),
    )


@all_flavors
def test_autoalloc_timelimit_hms(hq_env: HqEnv, flavor: ManagerFlavor):
    hq_env.start_server()
    add_queue(hq_env, manager=flavor.manager_type(), time_limit="01:10:15")

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("Timelimit", 0, "1h 10m 15s")


@all_flavors
def test_autoalloc_timelimit_human_format(hq_env: HqEnv, flavor: ManagerFlavor):
    hq_env.start_server()
    add_queue(hq_env, manager=flavor.manager_type(), time_limit="3h 15m 10s")

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("Timelimit", 0, "3h 15m 10s")


@all_flavors
def test_autoalloc_require_timelimit(hq_env: HqEnv, flavor: ManagerFlavor):
    hq_env.start_server()
    add_queue(
        hq_env,
        manager=flavor.manager_type(),
        time_limit=None,
        expect_fail="--time-limit <TIME_LIMIT>",
    )


@all_flavors
def test_autoalloc_worker_time_limit_too_large(hq_env: HqEnv, flavor: ManagerFlavor):
    hq_env.start_server()
    add_queue(
        hq_env,
        manager=flavor.manager_type(),
        time_limit="1h",
        worker_time_limit="2h",
        expect_fail="Worker time limit cannot be larger than queue time limit",
    )


def test_autoalloc_remove_queue(hq_env: HqEnv):
    hq_env.start_server()
    add_queue(hq_env, manager="pbs")
    add_queue(hq_env, manager="pbs")
    add_queue(hq_env, manager="pbs")

    result = remove_queue(hq_env, queue_id=2)
    assert "Allocation queue 2 successfully removed" in result

    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_columns_value(["ID"], 0, ["1"])
    table.check_columns_value(["ID"], 1, ["3"])


def test_autoalloc_zero_backlog(hq_env: HqEnv):
    hq_env.start_server()
    add_queue(
        hq_env,
        manager="pbs",
        name=None,
        backlog=0,
        expect_fail="Backlog has to be at least 1",
    )


@all_flavors
def test_add_queue(hq_env: HqEnv, flavor: ManagerFlavor):
    hq_env.start_server()
    output = add_queue(
        hq_env,
        manager=flavor.manager_type(),
        name="foo",
        backlog=5,
        workers_per_alloc=2,
    )
    assert "Allocation queue 1 successfully created" in output

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("ID", 0, "1")


def test_autoalloc_pause_resume_queue_status(hq_env: HqEnv):
    hq_env.start_server()
    add_queue(hq_env, manager="pbs")

    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_column_value("State", 0, "ACTIVE")

    pause_queue(hq_env, 1)
    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_column_value("State", 0, "PAUSED")

    resume_queue(hq_env, 1)
    table = hq_env.command(["alloc", "list"], as_table=True)
    table.check_column_value("State", 0, "ACTIVE")

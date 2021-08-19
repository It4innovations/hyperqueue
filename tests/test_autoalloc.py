from .conftest import HqEnv


def test_autoalloc_info(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "1m 5s"])
    table = hq_env.command(["auto-alloc", "info"], as_table=True)
    table.check_value_row("Refresh interval", "1m 5s")


def test_add_pbs_queue(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "500ms"])
    output = hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "5", "--max-workers-per-alloc", "2"])
    assert "Allocation queue foo was successfully created" in output

    info = hq_env.command(["auto-alloc", "info"], as_table=True)[1:]
    info.check_value_column("Descriptor name", 0, "foo")

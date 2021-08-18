from .conftest import HqEnv


def test_autoalloc_info(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "1m 5s"])
    table = hq_env.command(["auto-alloc", "info"], as_table=True)
    table.check_value_row("Refresh interval", "1m 5s")

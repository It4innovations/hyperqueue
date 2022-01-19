from pathlib import Path

from .conftest import HqEnv


def test_hq_directives_from_file(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")

    content = """#! /bin/bash
# Hello!
#HQ --name abc --array=1-10
#HQ --cpus="2 compact"

./do-something

#HQ --this-should-be-ignored
"""

    Path("test.sh").write_text(content)
    Path("test").write_text(content)
    Path("input").write_text("line\n" * 5)

    hq_env.command(["submit", "test.sh"])
    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Name") == "abc"
    assert table.get_row_value("Resources") == "cpus: 2 compact"
    assert table.get_row_value("Tasks") == "10; Ids: 1-10"

    hq_env.command(["submit", "--name=xyz", "test.sh"])
    table = hq_env.command(["job", "info", "2"], as_table=True)
    assert table.get_row_value("Name") == "xyz"
    assert table.get_row_value("Resources") == "cpus: 2 compact"
    assert table.get_row_value("Tasks") == "10; Ids: 1-10"

    hq_env.command(["submit", "--name=xyz", "--each-line", "input", "test.sh"])
    table = hq_env.command(["job", "info", "3"], as_table=True)
    assert table.get_row_value("Name") == "xyz"
    assert table.get_row_value("Resources") == "cpus: 2 compact"
    assert table.get_row_value("Tasks") == "5; Ids: 0-4"

    hq_env.command(["submit", "test"])
    table = hq_env.command(["job", "info", "4"], as_table=True)
    assert table.get_row_value("Name") == "test"

    hq_env.command(["submit", "--directives", "always", "test"])
    table = hq_env.command(["job", "info", "5"], as_table=True)
    assert table.get_row_value("Name") == "abc"


def test_hq_directives_mode_auto(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")

    content = """#! /bin/bash
#HQ --foo=bar
./do-something
"""

    Path("test.sh").write_text(content)

    hq_env.command(
        ["submit", "--directives", "auto", "test.sh"],
        expect_fail="Found argument '--foo' which wasn't expected",
    )


def test_hq_directives_mode_always(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")

    content = """#! /bin/bash
#HQ --foo=bar
./do-something
"""

    Path("program").write_text(content)

    hq_env.command(
        ["submit", "--directives", "always", "program"],
        expect_fail="Found argument '--foo' which wasn't expected",
    )


def test_hq_directives_mode_off(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")

    content = """#! /bin/bash
#HQ --foo=bar
./do-something
"""

    Path("test.sh").write_text(content)

    hq_env.command(["submit", "--directives", "off", "test.sh"])

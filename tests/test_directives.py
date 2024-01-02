from pathlib import Path

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.io import write_file
from .utils.job import default_task_output


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
    wait_for_job_state(hq_env, 4, "FAILED")
    table = hq_env.command(["job", "info", "4"], as_table=True)
    assert table[0].get_row_value("Name") == "test"

    hq_env.command(["submit", "--directives", "file", "test"])
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
        expect_fail="unexpected argument '--foo' found",
    )


def test_hq_directives_mode_file(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    content = """#! /bin/bash
#HQ --foo=bar
./do-something
"""

    Path("program").write_text(content)

    hq_env.command(
        ["submit", "--directives", "file", "program"],
        expect_fail="unexpected argument '--foo' found",
    )


def test_hq_directives_mode_stdin(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(
        ["submit", "--stdin", "--directives=stdin", "bash"],
        stdin="#!/bin/bash\n#HQ --name=abc\necho Hello\n",
    )
    wait_for_job_state(hq_env, 1, "FINISHED")
    with open(default_task_output(1), "rb") as f:
        assert f.read() == b"Hello\n"
    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Name", "abc")

    hq_env.command(
        ["submit", "--name", "xyz", "--stdin", "--directives=stdin", "bash"],
        stdin="#!/bin/bash\n#HQ --name=abc\necho Hello\n",
    )
    table = hq_env.command(["job", "info", "2"], as_table=True)
    table.check_row_value("Name", "xyz")


def test_hq_directives_mode_stdin_no_stdin(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(
        ["submit", "--directives=stdin", "bash"],
        expect_fail="You have to use `--stdin` when you specify `--directives=stdin`",
    )


def test_hq_directives_stdin_ignore_shebang(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()
    hq_env.command(
        ["submit", "--stdin", "--directives=stdin", "bash"],
        stdin="#!/foo/bar\necho 'Hello'",
    )
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_hq_directives_mode_off(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus="1")

    content = """#! /bin/bash
#HQ --foo=bar
./do-something
"""

    Path("test.sh").write_text(content)

    hq_env.command(["submit", "--directives", "off", "test.sh"])


def test_hq_directives_overwrite_from_cli(hq_env: HqEnv):
    hq_env.start_server()

    write_file(
        "test.sh",
        """#! /bin/bash
#HQ --priority 100

./do-something
""",
    )

    hq_env.command(["submit", "--directives", "file", "test.sh"])
    table = hq_env.command(["job", "info", "1"], as_table=True)
    assert table.get_row_value("Priority") == "100"

    hq_env.command(["submit", "--priority", "8", "--directives", "file", "test.sh"])
    table = hq_env.command(["job", "info", "2"], as_table=True)
    assert table.get_row_value("Priority") == "8"


def test_hq_directives_shebang_with_args(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    write_file(
        "test.sh",
        """#!/bin/bash -l
echo "hello"
""",
    )

    hq_env.command(["submit", "--directives", "file", "test.sh"])
    wait_for_job_state(hq_env, 1, "FINISHED")

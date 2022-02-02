from pathlib import Path

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.io import check_file_contents
from ..utils.table import parse_multiline_cell
from . import bash, prepare_job_client


def test_submit_simple(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    job.program(args=["hostname"])
    job_id = client.submit(job)

    wait_for_job_state(hq_env, job_id, "FINISHED")


def test_submit_cwd(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)

    cwd = Path(hq_env.server_dir) / "workdir"
    cwd.mkdir()

    job.program(args=["hostname"], cwd=str(cwd))
    job_id = client.submit(job)

    wait_for_job_state(hq_env, job_id, "FINISHED")

    table = hq_env.command(["job", "tasks", str(job_id)], as_table=True)
    cell = table.get_column_value("Paths")[0]
    assert parse_multiline_cell(cell)["Workdir"] == str(cwd)


def test_submit_env(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    job.program(
        args=bash("echo $FOO > out.txt; echo $BAZ >> out.txt"),
        env={"FOO": "BAR", "BAZ": "123"},
    )
    job_id = client.submit(job)

    wait_for_job_state(hq_env, job_id, "FINISHED")
    check_file_contents("out.txt", "BAR\n123\n")


def test_submit_stdio(hq_env: HqEnv):
    (job, client) = prepare_job_client(hq_env)
    job.program(
        args=bash("echo Test1; cat -; >&2 echo Test2"),
        stdout="out",
        stderr="err",
        stdin=b"Hello\n",
    )
    job_id = client.submit(job)
    wait_for_job_state(hq_env, job_id, "FINISHED")
    check_file_contents("out", "Test1\nHello\n")
    check_file_contents("err", "Test2\n")

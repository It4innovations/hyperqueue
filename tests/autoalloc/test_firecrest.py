import os
from typing import List, Optional

from ..conftest import HqEnv
from ..utils.wait import wait_until
from .mock.firecrest import MockFirecrest
from .utils import (
    extract_script_args,
    prepare_tasks,
    remove_queue,
    wait_for_alloc,
)

CLIENT_SECRET = "mock-client-secret-12345"
REMOTE_HQ_PATH = "/remote/bin/hq"
REMOTE_SERVER_DIR = "/remote/server-dir"
REMOTE_WORKDIR = "/remote/workdir"


def start_server(hq_env: HqEnv, secret: Optional[str] = CLIENT_SECRET, **kwargs):
    """
    Starts the HQ server with fast autoalloc refresh and (by default) the FirecREST
    client secret in its environment.
    """
    env = {
        "HQ_AUTOALLOC_REFRESH_INTERVAL_MS": "100",
        "HQ_AUTOALLOC_MAX_SCHEDULE_DELAY_MS": "100",
        "HQ_AUTOALLOC_SCHEDULE_TICK_INTERVAL_MS": "100",
    }
    if secret is not None:
        env["HQ_FIRECREST_CLIENT_SECRET"] = secret
    return hq_env.start_server(env=env, **kwargs)


def firecrest_queue_args(
    mock: MockFirecrest,
    time_limit="3m",
    additional_args: Optional[str] = None,
) -> List[str]:
    args = [
        "--api-url",
        mock.api_url,
        "--system",
        "daint",
        "--token-url",
        mock.token_url,
        "--client-id",
        "test-client",
        "--remote-hq-path",
        REMOTE_HQ_PATH,
        "--remote-server-dir",
        REMOTE_SERVER_DIR,
        "--remote-workdir",
        REMOTE_WORKDIR,
        "--time-limit",
        time_limit,
    ]
    if additional_args is not None:
        args.append("--")
        args.extend(additional_args.split(" "))
    return args


def add_queue(
    hq_env: HqEnv,
    mock: MockFirecrest,
    dry_run=False,
    time_limit="3m",
    additional_args: Optional[str] = None,
    expect_fail: Optional[str] = None,
) -> str:
    args = ["alloc", "add", "firecrest"]
    if not dry_run:
        args.append("--no-dry-run")
    args += firecrest_queue_args(mock, time_limit=time_limit, additional_args=additional_args)
    return hq_env.command(args, expect_fail=expect_fail)


def test_firecrest_submit_script(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, mock, additional_args="--account=foo --partition=bar")
        wait_until(lambda: len(mock.submitted) > 0)

        submission = mock.submitted[0]
        assert submission["name"] == "hq-1-1"
        assert submission["working_directory"] == REMOTE_WORKDIR

        script = submission["script"]
        assert extract_script_args(script, "#SBATCH") == [
            "--nodes=1",
            "--job-name=hq-1-1",
            f"--output={REMOTE_WORKDIR}/hq-1-1.stdout",
            f"--error={REMOTE_WORKDIR}/hq-1-1.stderr",
            "--time=00:03:00",
            "--account=foo --partition=bar",
        ]
        worker_command = script.splitlines()[-1]
        assert f"{REMOTE_HQ_PATH} worker start" in worker_command
        assert '--manager "slurm"' in worker_command
        assert f'--server-dir "{REMOTE_SERVER_DIR}"' in worker_command


def test_firecrest_dry_run_success(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        hq_env.command(["alloc", "dry-run", "firecrest"] + firecrest_queue_args(mock))

        # The trial allocation should be submitted and immediately canceled
        assert mock.deleted_jobs == ["1"]
        assert mock.jobs["1"]["state"] == "CANCELLED"


def test_firecrest_dry_run_token_error(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        mock.fail_next_token_request(401)
        hq_env.command(
            ["alloc", "dry-run", "firecrest"] + firecrest_queue_args(mock),
            expect_fail="OAuth2 token request failed",
        )


def test_firecrest_dry_run_submit_error(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        mock.fail_next_api_request(500)
        hq_env.command(
            ["alloc", "dry-run", "firecrest"] + firecrest_queue_args(mock),
            expect_fail="FirecREST job submission failed",
        )


def test_firecrest_add_queue_missing_secret(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env, secret=None)
        add_queue(
            hq_env,
            mock,
            expect_fail="The environment variable `HQ_FIRECREST_CLIENT_SECRET` with the FirecREST client secret is not set",
        )


def test_firecrest_allocation_lifecycle(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, mock)
        wait_for_alloc(hq_env, "QUEUED", "1")

        mock.set_job_state("1", "RUNNING")
        wait_for_alloc(hq_env, "RUNNING", "1")

        mock.set_job_state("1", "COMPLETED")
        wait_for_alloc(hq_env, "FINISHED", "1")


def test_firecrest_allocation_failure(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, mock)
        wait_for_alloc(hq_env, "QUEUED", "1")

        mock.set_job_state("1", "NODE_FAIL")
        wait_for_alloc(hq_env, "FAILED", "1")


def test_firecrest_cancel_on_queue_remove(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, mock)
        wait_for_alloc(hq_env, "QUEUED", "1")

        remove_queue(hq_env, 1, force=True)
        wait_until(lambda: mock.jobs["1"]["state"] == "CANCELLED")


def test_firecrest_status_queries_are_batched(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, mock)
        wait_for_alloc(hq_env, "QUEUED", "1")

        # Let several refresh cycles pass; the status of allocations should be
        # queried through the job-list endpoint, not per job id
        wait_until(lambda: mock.counter("GET /compute/jobs") >= 3)
        assert mock.counter("GET /compute/jobs/{id}") == 0


def test_firecrest_retry_with_fresh_token_on_401(hq_env: HqEnv):
    with MockFirecrest() as mock:
        start_server(hq_env)
        prepare_tasks(hq_env)

        add_queue(hq_env, mock)
        wait_for_alloc(hq_env, "QUEUED", "1")

        tokens_before = mock.token_counter
        mock.fail_next_api_request(401)

        # The 401 should be recovered from transparently: a fresh token is fetched
        # and the allocation status keeps being refreshed without errors
        wait_until(lambda: mock.token_counter == tokens_before + 1)
        mock.set_job_state("1", "RUNNING")
        wait_for_alloc(hq_env, "RUNNING", "1")


def test_firecrest_journal_restore(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "journal")
    with MockFirecrest() as mock:
        start_server(hq_env, args=["--journal", journal_path])
        add_queue(hq_env, mock)
        hq_env.stop_server()

        start_server(hq_env, args=["--journal", journal_path])
        table = hq_env.command(["alloc", "list"], as_table=True)
        table.check_columns_value(["ID", "Manager"], 0, ["1", "FirecREST"])


def test_firecrest_journal_restore_missing_secret(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "journal")
    with MockFirecrest() as mock:
        start_server(hq_env, args=["--journal", journal_path])
        add_queue(hq_env, mock)
        hq_env.stop_server()

        # Without the secret the queue cannot be restored; the server should
        # nevertheless start and work
        start_server(hq_env, secret=None, args=["--journal", journal_path])
        table = hq_env.command(["alloc", "list"], as_table=True)
        assert len(table) == 0


def test_firecrest_secret_not_persisted(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "journal")
    with MockFirecrest() as mock:
        start_server(hq_env, args=["--journal", journal_path])
        add_queue(hq_env, mock)
        hq_env.stop_server()

        with open(journal_path, "rb") as f:
            journal = f.read()
        # The journal stores the name of the environment variable holding the secret,
        # never the secret itself
        assert CLIENT_SECRET.encode() not in journal
        assert b"HQ_FIRECREST_CLIENT_SECRET" in journal

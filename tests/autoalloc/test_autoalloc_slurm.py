import json
import os
from os.path import dirname, join

from ..conftest import HqEnv
from ..utils.wait import wait_until
from .utils import (
    add_queue,
    extract_script_args,
    prepare_tasks,
    program_code_store_args_json,
    wait_for_event,
)


def test_add_slurm_descriptor(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "500ms"])
    output = add_queue(
        hq_env,
        manager="slurm",
        name="foo",
        backlog=5,
        workers_per_alloc=2,
    )
    assert "Allocation queue 1 successfully created" in output

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("ID", 0, "1")


def test_slurm_queue_sbatch_fail(hq_env: HqEnv):
    sbatch_code = "exit(1)"

    with hq_env.mock.mock_program("sbatch", sbatch_code):
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm")
        wait_for_event(hq_env, "Allocation submission failed")
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        table.check_column_value(
            "Message",
            0,
            "sbatch execution failed\nCaused by:\nExit code: 1\nStderr:\nStdout:",
        )


def test_slurm_queue_sbatch_args(hq_env: HqEnv):
    path = join(hq_env.work_path, "sbatch.out")
    sbatch_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("sbatch", sbatch_code):
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(
            hq_env,
            manager="slurm",
            time_limit="3m",
            additional_args="--foo=bar a b --baz 42",
        )
        wait_until(lambda: os.path.exists(path))
        with open(path) as f:
            args = json.loads(f.read())
            sbatch_script_path = args[1]
        with open(sbatch_script_path) as f:
            data = f.read()
            pbs_args = extract_script_args(data, "#SBATCH")
            assert pbs_args == [
                "--nodes=1",
                "--job-name=hq-alloc-1",
                f"--output={join(dirname(sbatch_script_path), 'stdout')}",
                f"--error={join(dirname(sbatch_script_path), 'stderr')}",
                "--time=00:03:00",
                "--foo=bar a b --baz 42",
            ]


def test_slurm_queue_sbatch_success(hq_env: HqEnv):
    sbatch_code = """print("Submitted batch job 123.job")"""

    with hq_env.mock.mock_program("sbatch", sbatch_code):
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm")
        wait_for_event(hq_env, "Allocation queued")
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        table.check_column_value("Message", 0, "123.job")

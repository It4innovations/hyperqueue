import contextlib
import json
import os
import time
from os.path import join
from typing import List, Optional

from .conftest import HqEnv
from .utils.check import check_error_log
from .utils.wait import wait_until


def test_autoalloc_descriptor_list(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")

    with mock.activate():
        hq_env.start_server()
        add_queue(hq_env, name=None, queue="queue", backlog=5)

        table = hq_env.command(["alloc", "list"], as_table=True)
        table.check_columns_value(
            (
                "ID",
                "Backlog size",
                "Workers per alloc",
                "Queue",
                "Timelimit",
                "Manager",
                "Name",
            ),
            0,
            ("1", "5", "1", "queue", "N/A", "PBS", ""),
        )

        add_queue(
            hq_env,
            manager="pbs",
            name="bar",
            queue="qexp",
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
                "Queue",
                "Timelimit",
                "Name",
            ),
            1,
            ("2", "1", "2", "qexp", "1h", "bar"),
        )

        add_queue(hq_env, manager="slurm", queue="partition", backlog=1)
        table = hq_env.command(["alloc", "list"], as_table=True)
        table.check_columns_value(
            ("ID", "Queue", "Manager"), 2, ("3", "partition", "SLURM")
        )


def test_add_pbs_descriptor(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "500ms"])
        output = add_queue(
            hq_env,
            manager="pbs",
            name="foo",
            queue="queue",
            backlog=5,
            workers_per_alloc=2,
        )
        assert "Allocation queue 1 successfully created" in output

        info = hq_env.command(["alloc", "list"], as_table=True)
        info.check_column_value("ID", 0, "1")


def test_add_slurm_descriptor(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "500ms"])
    output = add_queue(
        hq_env,
        manager="slurm",
        name="foo",
        queue="queue",
        backlog=5,
        workers_per_alloc=2,
    )
    assert "Allocation queue 1 successfully created" in output

    info = hq_env.command(["alloc", "list"], as_table=True)
    info.check_column_value("ID", 0, "1")


def test_pbs_fail_without_qstat(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "500ms"])
    hq_env.command(
        ["alloc", "add", "pbs", "--name", "foo", "--queue", "queue", "--backlog", "1"],
        expect_fail="qstat",
    )


def test_pbs_queue_qsub_fail(hq_env: HqEnv):
    qsub_code = "exit(1)"

    with hq_env.mock.mock_program("qsub", qsub_code):
        with hq_env.mock.mock_program("qstat", ""):
            hq_env.start_server(args=["--autoalloc-interval", "100ms"])
            prepare_tasks(hq_env)

            add_queue(hq_env)
            time.sleep(0.2)
            table = hq_env.command(["alloc", "events", "1"], as_table=True)
            table.check_column_value("Event", 0, "Allocation submission failed")
            table.check_column_value("Message", 0, "qsub execution failed")


def test_slurm_queue_sbatch_fail(hq_env: HqEnv):
    sbatch_code = "exit(1)"

    with hq_env.mock.mock_program("sbatch", sbatch_code):
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm")
        time.sleep(0.2)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        table.check_column_value("Event", 0, "Allocation submission failed")
        table.check_column_value("Message", 0, "sbatch execution failed")


def program_code_store_args_json(path: str) -> str:
    """
    Creates program code that stores its cmd arguments as JSON into the specified `path`.
    """
    return f"""
import sys
import json

with open("{path}", "w") as f:
    f.write(json.dumps(sys.argv))
"""


def test_pbs_queue_qsub_args(hq_env: HqEnv):
    path = join(hq_env.work_path, "qsub.out")
    qsub_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("qsub", qsub_code):
        with hq_env.mock.mock_program("qstat", ""):
            hq_env.start_server(args=["--autoalloc-interval", "100ms"])
            prepare_tasks(hq_env)

            add_queue(hq_env, additional_args="--foo=bar a b --baz 42")
            wait_until(lambda: os.path.exists(path))
            with open(path) as f:
                args = json.loads(f.read())
                start = args.index("--foo=bar")
                end = args.index("--")
                args = args[start:end]
                assert args == ["--foo=bar", "a", "b", "--baz", "42"]


def test_slurm_queue_sbatch_args(hq_env: HqEnv):
    path = join(hq_env.work_path, "sbatch.out")
    sbatch_code = program_code_store_args_json(path)

    with hq_env.mock.mock_program("sbatch", sbatch_code):
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm", additional_args="--foo=bar a b --baz 42")
        wait_until(lambda: os.path.exists(path))
        with open(path) as f:
            args = json.loads(f.read())
            start = args.index("--foo=bar")
            end = args.index("--wrap")
            args = args[start:end]
            assert args == ["--foo=bar", "a", "b", "--baz", "42"]


def test_pbs_queue_qsub_success(hq_env: HqEnv):
    qsub_code = """print("123.job")"""

    with hq_env.mock.mock_program("qsub", qsub_code):
        with hq_env.mock.mock_program("qstat", ""):
            hq_env.start_server(args=["--autoalloc-interval", "100ms"])
            prepare_tasks(hq_env)

            add_queue(hq_env)
            time.sleep(0.2)
            table = hq_env.command(["alloc", "events", "1"], as_table=True)
            table.check_column_value("Event", 0, "Allocation queued")
            table.check_column_value("Message", 0, "123.job")


def test_slurm_queue_sbatch_success(hq_env: HqEnv):
    sbatch_code = """print("Submitted batch job 123.job")"""

    with hq_env.mock.mock_program("sbatch", sbatch_code):
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env, manager="slurm")
        time.sleep(0.2)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        table.check_column_value("Event", 0, "Allocation queued")
        table.check_column_value("Message", 0, "123.job")


def test_pbs_queue_qsub_check_args(hq_env: HqEnv):
    output_log = join(hq_env.work_path, "output.log")
    qsub_code = f"""
import sys
import traceback

args = sys.argv[1:]

def check_arg(key, val):
    for (index, arg) in enumerate(args):
        if arg == key:
            assert args[index + 1] == val
            return
    raise Exception(f"Key `{{key}}` not found")


def check():
    check_arg("-q", "queue")

    assert "-lselect=1" in args
    for arg in args:
        assert not arg.startswith("-lwalltime")

try:
    check()
except:
    with open("{output_log}", "w") as f:
        tb = traceback.format_exc()
        f.write(tb)
        f.write(" ".join(args))
"""

    with hq_env.mock.mock_program("qsub", qsub_code):
        with hq_env.mock.mock_program("qstat", ""):
            with check_error_log(output_log):
                hq_env.start_server(args=["--autoalloc-interval", "100ms"])
                add_queue(hq_env)
                time.sleep(0.2)


def test_pbs_events_job_lifecycle(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")

    with mock.activate():
        mock.set_job_data("Q")
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env)

        # Queued
        time.sleep(0.2)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        table.check_column_value("Event", -1, "Allocation queued")

        # Started
        mock.set_job_data("R", stime="Thu Aug 19 13:05:39 2021")
        time.sleep(0.2)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        assert "Allocation started" in table.get_column_value("Event")

        # Finished
        mock.set_job_data(
            "F",
            stime="Thu Aug 19 13:05:39 2021",
            mtime="Thu Aug 19 13:05:39 2021",
            exit_code=0,
        )
        time.sleep(0.2)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        assert "Allocation finished" in table.get_column_value("Event")


def test_pbs_events_job_failed(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")
    mock.set_job_data(
        "F",
        stime="Thu Aug 19 13:05:39 2021",
        mtime="Thu Aug 19 13:05:39 2021",
        exit_code=1,
    )

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env)

        time.sleep(0.5)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        column = table.get_column_value("Event")
        assert "Allocation failed" in column


def test_pbs_allocations_job_lifecycle(hq_env: HqEnv):
    mock = PbsMock(
        hq_env,
        qtime="Thu Aug 19 13:05:38 2021",
        stime="Thu Aug 19 13:05:39 2021",
        mtime="Thu Aug 19 13:05:39 2021",
    )
    mock.set_job_data("Q")

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo")
        time.sleep(0.2)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_columns_value(
            ("Id", "State", "Worker count"), 0, ("0", "Queued", "1")
        )

        mock.set_job_data("R")
        time.sleep(0.2)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_column_value("State", 0, "Running")

        mock.set_job_data("F", exit_code=0)
        time.sleep(0.2)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_column_value("State", 0, "Finished")


def test_allocations_ignore_job_changes_after_finish(hq_env: HqEnv):
    mock = PbsMock(
        hq_env,
        jobs=["1", "2"],
        qtime="Thu Aug 19 13:05:38 2021",
        stime="Thu Aug 19 13:05:39 2021",
        mtime="Thu Aug 19 13:05:39 2021",
    )
    mock.set_job_data("F", exit_code=0)

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env)
        time.sleep(0.3)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_column_value("State", 0, "Finished")

        mock.set_job_data("R")
        time.sleep(0.3)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_column_value("State", 0, "Finished")


def test_pbs_delete_active_jobs(hq_env: HqEnv):
    mock = PbsMock(
        hq_env,
        jobs=["1", "2"],
        qtime="Thu Aug 19 13:05:38 2021",
        stime="Thu Aug 19 13:05:39 2021",
        mtime="Thu Aug 19 13:05:39 2021",
    )
    mock.set_job_data("R")

    with mock.activate():
        process = hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env, name="foo", backlog=2, workers_per_alloc=1)

        def allocations_up():
            table = hq_env.command(["alloc", "info", "1"], as_table=True)
            return len(table) == 3

        wait_until(allocations_up)

        hq_env.command(["server", "stop"])
        process.wait()
        hq_env.check_process_exited(process)

        wait_until(lambda: len(mock.deleted_jobs()) == 2)
        assert sorted(mock.deleted_jobs()) == ["1", "2"]


def test_remove_descriptor(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")

    with mock.activate():
        hq_env.start_server()
        add_queue(hq_env)
        add_queue(hq_env)
        add_queue(hq_env)

        result = remove_queue(hq_env, queue_id=2)
        assert "Allocation queue 2 successfully removed" in result

        table = hq_env.command(["alloc", "list"], as_table=True)
        table.check_columns_value(["ID"], 0, ["1"])
        table.check_columns_value(["ID"], 1, ["3"])


def test_pbs_remove_descriptor_cancel_allocations(hq_env: HqEnv):
    mock = PbsMock(
        hq_env,
        jobs=["1", "2"],
        qtime="Thu Aug 19 13:05:38 2021",
        stime="Thu Aug 19 13:05:39 2021",
        mtime="Thu Aug 19 13:05:39 2021",
    )
    mock.set_job_data("R")

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        prepare_tasks(hq_env)

        add_queue(hq_env, backlog=2, workers_per_alloc=1)

        def allocations_up():
            table = hq_env.command(["alloc", "info", "1"], as_table=True)
            return len(table) == 3

        wait_until(allocations_up)

        remove_queue(hq_env, 1)

        wait_until(lambda: len(hq_env.command(["alloc", "list"], as_table=True)) == 1)

        assert sorted(mock.deleted_jobs()) == ["1", "2"]


class PbsMock:
    def __init__(self, hq_env: HqEnv, jobs: List[str] = None, **data):
        if jobs is None:
            jobs = list(str(i) for i in range(1000))
        self.hq_env = hq_env
        self.jobs = jobs
        self.qstat_path = join(self.hq_env.work_path, "pbs-qstat")
        self.qsub_path = join(self.hq_env.work_path, "pbs-qsub")
        self.qdel_dir = join(self.hq_env.work_path, "pbs-qdel")
        os.makedirs(self.qdel_dir)
        self.data = data

        with open(self.qsub_path, "w") as f:
            f.write(json.dumps(self.jobs))

        self.qsub_code = f"""
import json

with open("{self.qsub_path}") as f:
    jobs = json.loads(f.read())

if not jobs:
    raise Exception("No more jobs can be scheduled")

job = jobs.pop(0)
with open("{self.qsub_path}", "w") as f:
    f.write(json.dumps(jobs))

print(job)
"""
        self.qstat_code = f"""
import sys
import json

jobid = None
args = sys.argv[1:]
for (index, arg) in enumerate(args[:-1]):
    if arg == "-f":
        jobid = args[index + 1]
        break

assert jobid is not None

with open("{self.qstat_path}") as f:
    jobdata = json.loads(f.read())

data = {{
    "Jobs": {{
        jobid: jobdata
    }}
}}
print(json.dumps(data))
"""
        self.qdel_code = f"""
import sys
import json
import os

jobid = sys.argv[1]

with open(os.path.join("{self.qdel_dir}", jobid), "w") as f:
    f.write(jobid)
    f.flush()
"""

    @contextlib.contextmanager
    def activate(self):
        with self.hq_env.mock.mock_program("qsub", self.qsub_code):
            with self.hq_env.mock.mock_program("qstat", self.qstat_code):
                with self.hq_env.mock.mock_program("qdel", self.qdel_code):
                    yield

    def set_job_data(
        self,
        status: str,
        qtime: str = None,
        stime: str = None,
        mtime: str = None,
        exit_code: int = None,
    ):
        jobdata = dict(self.data)
        jobdata.update(
            {
                "job_state": status,
            }
        )
        if qtime is not None:
            jobdata["qtime"] = qtime
        if stime is not None:
            jobdata["stime"] = stime
        if mtime is not None:
            jobdata["mtime"] = mtime
        if exit_code is not None:
            jobdata["Exit_status"] = exit_code
        with open(self.qstat_path, "w") as f:
            f.write(json.dumps(jobdata))

    def deleted_jobs(self) -> List[str]:
        return list(os.listdir(self.qdel_dir))


def add_queue(
    hq_env: HqEnv,
    manager="pbs",
    name: Optional[str] = "foo",
    queue="queue",
    backlog=1,
    workers_per_alloc=1,
    additional_args=None,
    time_limit=None,
) -> str:
    args = ["alloc", "add", manager]
    if name is not None:
        args.extend(["--name", name])
    queue_key = "queue" if manager == "pbs" else "partition"
    args.extend(
        [
            f"--{queue_key}",
            queue,
            "--backlog",
            str(backlog),
            "--workers-per-alloc",
            str(workers_per_alloc),
        ]
    )
    if time_limit is not None:
        args.extend(["--time-limit", time_limit])
    if additional_args is not None:
        args.append("--")
        args.extend(additional_args.split(" "))

    return hq_env.command(args)


def prepare_tasks(hq_env: HqEnv, count=1000):
    hq_env.command(["submit", f"--array=0-{count}", "sleep", "1"])


def remove_queue(hq_env: HqEnv, queue_id: int):
    args = ["alloc", "remove", str(queue_id)]
    return hq_env.command(args)

import contextlib
import json
import os
import time
from os.path import join
from typing import List

from .conftest import HqEnv
from .utils.check import check_error_log
from .utils.wait import wait_until


def test_autoalloc_descriptor_list(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")

    with mock.activate():
        hq_env.start_server()
        add_queue(hq_env, name="foo", queue="queue", workers=5)

        table = hq_env.command(["alloc", "list"], as_table=True)
        table.check_value_columns(("ID", "Target worker count", "Max workers per allocation", "Queue", "Timelimit"),
                                  0,
                                  ("1", "5", "1", "queue", "N/A"))

        hq_env.command(
            ["alloc", "add", "pbs", "--name", "bar", "--queue", "qexp", "--workers", "1",
             "--max-workers-per-alloc",
             "2", "--time-limit", "1h"])
        table = hq_env.command(["alloc", "list"], as_table=True)
        table.check_value_columns(("ID", "Target worker count", "Max workers per allocation", "Queue", "Timelimit"),
                                  1,
                                  ("2", "1", "2", "qexp", "1h"))


def test_add_pbs_descriptor(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "500ms"])
        output = hq_env.command(
            ["alloc", "add", "pbs", "--name", "foo", "--queue", "queue", "--workers", "5",
             "--max-workers-per-alloc", "2"])
        assert "Allocation queue 1 successfully created" in output

        info = hq_env.command(["alloc", "list"], as_table=True)
        info.check_value_column("ID", 0, "1")


def test_pbs_fail_without_qstat(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "500ms"])
    hq_env.command(["alloc", "add", "pbs", "--name", "foo", "--queue", "queue", "--workers", "1"],
                   expect_fail="qstat")


def test_pbs_queue_qsub_fail(hq_env: HqEnv):
    qsub_code = "exit(1)"

    with hq_env.mock.mock_program("qsub", qsub_code):
        with hq_env.mock.mock_program("qstat", ""):
            hq_env.start_server(args=["--autoalloc-interval", "100ms"])
            add_queue(hq_env)
            time.sleep(0.2)
            table = hq_env.command(["alloc", "events", "1"], as_table=True)
            table.check_value_column("Event", 0, "Allocation submission failed")
            table.check_value_column("Message", 0, "qsub execution failed")


def test_pbs_queue_qsub_success(hq_env: HqEnv):
    qsub_code = """print("123.job")"""

    with hq_env.mock.mock_program("qsub", qsub_code):
        with hq_env.mock.mock_program("qstat", ""):
            hq_env.start_server(args=["--autoalloc-interval", "100ms"])
            add_queue(hq_env)
            time.sleep(0.2)
            table = hq_env.command(["alloc", "events", "1"], as_table=True)
            table.check_value_column("Event", 0, "Allocation queued")
            table.check_value_column("Message", 0, "123.job")


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
        add_queue(hq_env)

        # Queued
        time.sleep(0.5)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        table.check_value_column("Event", -1, "Allocation queued")

        # Started
        mock.set_job_data("R", stime="Thu Aug 19 13:05:39 2021")
        time.sleep(0.5)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        table.check_value_column("Event", -1, "Allocation started")

        # Finished
        mock.set_job_data("F", stime="Thu Aug 19 13:05:39 2021", mtime="Thu Aug 19 13:05:39 2021", exit_code=0)
        time.sleep(0.5)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        assert "Allocation finished" in table.get_column_value("Event")


def test_pbs_events_job_failed(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")
    mock.set_job_data("F", stime="Thu Aug 19 13:05:39 2021", mtime="Thu Aug 19 13:05:39 2021", exit_code=1)

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        add_queue(hq_env)

        time.sleep(0.5)
        table = hq_env.command(["alloc", "events", "1"], as_table=True)
        column = table.get_column_value("Event")
        assert "Allocation failed" in column


def test_pbs_allocations_job_lifecycle(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021", stime="Thu Aug 19 13:05:39 2021",
                   mtime="Thu Aug 19 13:05:39 2021")
    mock.set_job_data("Q")

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        add_queue(hq_env, name="foo")
        time.sleep(0.2)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_value_columns(("Id", "State", "Worker count"), 0,
                                  ("1", "Queued", "1"))

        mock.set_job_data("R")
        time.sleep(0.2)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_value_column("State", 0, "Running")

        mock.set_job_data("F", exit_code=0)
        time.sleep(0.2)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_value_column("State", 0, "Finished")


def test_pbs_allocations_ignore_job_changes_after_finish(hq_env: HqEnv):
    mock = PbsMock(hq_env, jobs=["1", "2"], qtime="Thu Aug 19 13:05:38 2021", stime="Thu Aug 19 13:05:39 2021",
                   mtime="Thu Aug 19 13:05:39 2021")
    mock.set_job_data("F", exit_code=0)

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        add_queue(hq_env)
        time.sleep(0.3)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_value_column("State", 0, "Finished")

        mock.set_job_data("R")
        time.sleep(0.3)

        table = hq_env.command(["alloc", "info", "1"], as_table=True)
        table.check_value_column("State", 0, "Finished")


def test_pbs_delete_active_jobs(hq_env: HqEnv):
    mock = PbsMock(hq_env,
                   jobs=["1", "2"],
                   qtime="Thu Aug 19 13:05:38 2021",
                   stime="Thu Aug 19 13:05:39 2021",
                   mtime="Thu Aug 19 13:05:39 2021")
    mock.set_job_data("R")

    with mock.activate():
        process = hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        add_queue(hq_env, name="foo", workers=2, max_workers_per_alloc=1)

        def allocations_up():
            table = hq_env.command(["alloc", "info", "1"], as_table=True)
            return len(table) == 3

        wait_until(allocations_up)

        hq_env.command(["server", "stop"])
        process.wait()
        hq_env.check_process_exited(process)

        wait_until(lambda: len(mock.deleted_jobs()) == 2)
        assert sorted(mock.deleted_jobs()) == ["1", "2"]


class PbsMock:
    def __init__(self, hq_env: HqEnv, jobs: List[str] = None, **data):
        if jobs is None:
            jobs = ["1"]
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

    def set_job_data(self, status: str, qtime: str = None, stime: str = None, mtime: str = None, exit_code: int = None):
        jobdata = dict(self.data)
        jobdata.update({
            "job_state": status,
        })
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


def add_queue(hq_env, type="pbs", name="foo", queue="queue", workers=1, max_workers_per_alloc=1):
    return hq_env.command(["alloc", "add", type, "--name", name, "--queue", queue, "--workers", str(workers),
                           "--max-workers-per-alloc", str(max_workers_per_alloc)])

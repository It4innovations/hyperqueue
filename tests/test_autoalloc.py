import contextlib
import json
from os.path import join
from typing import List

import time

from .conftest import HqEnv
from .utils.check import check_error_log


def test_autoalloc_info(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "1m 5s"])
    table = hq_env.command(["auto-alloc", "info"], as_table=True)
    table.check_value_row("Refresh interval", "1m 5s")


def test_autoalloc_descriptor_info(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "5"])

    table = hq_env.command(["auto-alloc", "info"], as_table=True)[1:]
    table.check_value_columns(("Name", "Target worker count", "Max workers per allocation", "Queue", "Timelimit"), 0,
                              ("foo", "5", "1", "queue", "N/A"))

    hq_env.command(
        ["auto-alloc", "add", "bar", "pbs", "qexp", "1", "--max-workers-per-alloc", "2", "--timelimit", "1h"])
    table = hq_env.command(["auto-alloc", "info"], as_table=True)[1:]
    table.check_value_columns(("Name", "Target worker count", "Max workers per allocation", "Queue", "Timelimit"), 0,
                              ("bar", "1", "2", "qexp", "1h"))


def test_autoalloc_descriptor_name_collision(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "5"])
    hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "5"], expect_fail="Descriptor foo already exists")


def test_add_pbs_descriptor(hq_env: HqEnv):
    hq_env.start_server(args=["--autoalloc-interval", "500ms"])
    output = hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "5", "--max-workers-per-alloc", "2"])
    assert "Allocation queue foo was successfully created" in output

    info = hq_env.command(["auto-alloc", "info"], as_table=True)[1:]
    info.check_value_column("Name", 0, "foo")


def test_pbs_queue_qsub_fail(hq_env: HqEnv):
    qsub_code = "exit(1)"

    with hq_env.mock.mock_program("qsub", qsub_code):
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "1"])
        time.sleep(0.2)
        table = hq_env.command(["auto-alloc", "events", "foo"], as_table=True)
        table.check_value_column("Event", 0, "Allocation submission failed")
        table.check_value_column("Message", 0, "qsub execution failed")


def test_pbs_queue_qsub_success(hq_env: HqEnv):
    qsub_code = """print("123.job")"""

    with hq_env.mock.mock_program("qsub", qsub_code):
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "1"])
        time.sleep(0.2)
        table = hq_env.command(["auto-alloc", "events", "foo"], as_table=True)
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
        with check_error_log(output_log):
            hq_env.start_server(args=["--autoalloc-interval", "100ms"])
            hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "1"])
            time.sleep(0.2)


def test_pbs_events_job_lifecycle(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")

    with mock.activate():
        mock.set_job_data("Q")
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "1"])

        # Queued
        time.sleep(0.5)
        table = hq_env.command(["auto-alloc", "events", "foo"], as_table=True)
        table.check_value_column("Event", -1, "Allocation queued")

        # Started
        mock.set_job_data("R", stime="Thu Aug 19 13:05:39 2021")
        time.sleep(0.5)
        table = hq_env.command(["auto-alloc", "events", "foo"], as_table=True)
        table.check_value_column("Event", -1, "Allocation started")

        # Finished
        mock.set_job_data("F", exit_code=0)
        time.sleep(0.5)
        table = hq_env.command(["auto-alloc", "events", "foo"], as_table=True)
        assert "Allocation finished" in table.get_column_value("Event")


def test_pbs_events_job_failed(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")
    mock.set_job_data("F", exit_code=1)

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "1"])

        time.sleep(0.5)
        table = hq_env.command(["auto-alloc", "events", "foo"], as_table=True)
        column = table.get_column_value("Event")
        assert "Allocation failed" in column


def test_pbs_allocations_job_lifecycle(hq_env: HqEnv):
    mock = PbsMock(hq_env, qtime="Thu Aug 19 13:05:38 2021")
    mock.set_job_data("Q")

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "1"])
        time.sleep(0.2)

        table = hq_env.command(["auto-alloc", "allocations", "foo"], as_table=True)
        table.check_value_columns(("Id", "State", "Worker count"), 0,
                                  ("123.jobid", "Queued", "1"))

        mock.set_job_data("R")
        time.sleep(0.2)

        table = hq_env.command(["auto-alloc", "allocations", "foo"], as_table=True)
        table.check_value_column("State", 0, "Running")

        mock.set_job_data("F", exit_code=0)
        time.sleep(0.2)

        table = hq_env.command(["auto-alloc", "allocations", "foo"], as_table=True)
        table.check_value_column("State", 0, "Finished")


def test_pbs_allocations_ignore_job_changes_after_finish(hq_env: HqEnv):
    mock = PbsMock(hq_env, jobs=["1", "2"], qtime="Thu Aug 19 13:05:38 2021")
    mock.set_job_data("F", exit_code=0)

    with mock.activate():
        hq_env.start_server(args=["--autoalloc-interval", "100ms"])
        hq_env.command(["auto-alloc", "add", "foo", "pbs", "queue", "1"])
        time.sleep(0.2)

        table = hq_env.command(["auto-alloc", "allocations", "foo"], as_table=True)
        table.check_value_column("State", 0, "Finished")

        mock.set_job_data("R")
        time.sleep(0.2)

        table = hq_env.command(["auto-alloc", "allocations", "foo"], as_table=True)
        table.check_value_column("State", 0, "Finished")


class PbsMock:
    def __init__(self, hq_env: HqEnv, jobs: List[str] = None, **data):
        if jobs is None:
            jobs = ["1"]
        self.hq_env = hq_env
        self.jobs = jobs
        self.qstat_path = join(self.hq_env.work_path, "pbs-qstat")
        self.qsub_path = join(self.hq_env.work_path, "pbs-qsub")
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

    @contextlib.contextmanager
    def activate(self):
        with self.hq_env.mock.mock_program("qsub", self.qsub_code):
            with self.hq_env.mock.mock_program("qstat", self.qstat_code):
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

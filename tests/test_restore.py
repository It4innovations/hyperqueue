import time

from .utils.cmd import python
from .autoalloc.mock.mock import MockJobManager
from .autoalloc.mock.slurm import SlurmManager, adapt_slurm
from .autoalloc.utils import ManagerQueue, ExtractSubmitScriptPath, add_queue, remove_queue
from .conftest import HqEnv
from .utils import wait_for_job_state
import os


def test_restore_fully_finished(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.start_worker()
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    hq_env.command(["submit", "--", "sleep", "0"])
    wait_for_job_state(hq_env, [1, 2], "FINISHED")
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])
    hq_env.start_worker()
    hq_env.command(["submit", "--", "sleep", "0"])
    wait_for_job_state(hq_env, 3, "FINISHED")

    table = hq_env.command(["worker", "list"], as_table=True)
    table.check_columns_value(["ID", "State"], 0, ["3", "RUNNING"])


def test_restore_waiting_task(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    hq_env.command(["submit", "--", "sleep", "0"])
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])
    table = hq_env.command(["job", "list"], as_table=True)

    table.check_columns_value(["ID", "State", "Name"], 0, ["1", "WAITING", "hostname"])
    table.check_columns_value(["ID", "State", "Name"], 1, ["2", "WAITING", "sleep"])

    hq_env.start_worker()
    wait_for_job_state(hq_env, [1, 2], "FINISHED")


def test_restore_partially_finished_task(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(
        [
            "submit",
            "--array=0-4",
            "--",
            *python(
                "import os, time;"
                f"open('{tmp_path}/task-marker' + os.environ.get('HQ_TASK_ID'), 'w').write(os.environ.get('HQ_INSTANCE_ID'));"
                "time.sleep(1.5 if os.environ.get('HQ_TASK_ID') in ('1','3','4') else 0);",
            ),
        ]
    )
    hq_env.start_worker(cpus=5)
    wait_for_job_state(hq_env, 1, "RUNNING")
    time.sleep(0.2)

    markers = sorted([name for name in os.listdir(tmp_path) if name.startswith("task-marker")])
    assert markers == ["task-marker0", "task-marker1", "task-marker2", "task-marker3", "task-marker4"]
    for marker in markers:
        path = os.path.join(tmp_path, marker)
        with open(path) as f:
            assert f.read() == "0"
        os.unlink(path)

    out = hq_env.command(["--output-mode=json", "job", "info", "1"], as_json=True)
    stats = out[0]["info"]["task_stats"]
    assert stats["running"] == 3
    assert stats["finished"] == 2
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])

    out = hq_env.command(["--output-mode=json", "job", "info", "1"], as_json=True)
    stats = out[0]["info"]["task_stats"]
    assert stats["waiting"] == 3
    assert stats["finished"] == 2

    hq_env.start_worker(cpus=4)
    wait_for_job_state(hq_env, [1], "FINISHED")
    markers = sorted([name for name in os.listdir(tmp_path) if name.startswith("task-marker")])
    assert markers == ["task-marker1", "task-marker3", "task-marker4"]
    for marker in markers:
        path = os.path.join(tmp_path, marker)
        with open(path) as f:
            assert f.read() == "1"


def test_restore_partially_failed_task(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(
        [
            "submit",
            "--array=0-4",
            "--",
            *python(
                f"import os, time; open('{tmp_path}/task-marker' + os.environ.get('HQ_TASK_ID'), 'w');"
                "time.sleep(1.5 if os.environ.get('HQ_TASK_ID') in ('1','3','4') else 0); sys.exit(1)"
            ),
        ]
    )
    hq_env.start_worker(cpus=5)
    wait_for_job_state(hq_env, 1, "RUNNING")
    time.sleep(0.2)

    markers = sorted([name for name in os.listdir(tmp_path) if name.startswith("task-marker")])
    assert markers == ["task-marker0", "task-marker1", "task-marker2", "task-marker3", "task-marker4"]
    for marker in markers:
        os.unlink(os.path.join(tmp_path, marker))

    out = hq_env.command(["--output-mode=json", "job", "info", "1"], as_json=True)
    stats = out[0]["info"]["task_stats"]
    assert stats["running"] == 3
    assert stats["failed"] == 2
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])

    out = hq_env.command(["--output-mode=json", "job", "info", "1"], as_json=True)
    stats = out[0]["info"]["task_stats"]
    assert stats["waiting"] == 3
    assert stats["failed"] == 2

    out = hq_env.command(["--output-mode=json", "task", "info", "1", "0"], as_json=True)
    assert out[0]["error"] == "Error: Program terminated with exit code 1"

    hq_env.start_worker(cpus=4)
    wait_for_job_state(hq_env, [1], "FAILED")
    markers = sorted([name for name in os.listdir(tmp_path) if name.startswith("task-marker")])
    assert markers == ["task-marker1", "task-marker3", "task-marker4"]


def test_restore_canceled(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    hq_env.command(["submit", "--", "sleep", "0"])
    hq_env.command(["job", "cancel", "all"])
    wait_for_job_state(hq_env, [1, 2], "CANCELED")
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])
    out = hq_env.command(["--output-mode=json", "job", "list"], as_json=True)
    assert len(out) == 0


def test_repeated_restore(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    time.sleep(1.0)
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])
    out = hq_env.command(["--output-mode=json", "job", "list"], as_json=True)
    assert len(out) == 2


def test_restore_not_fully_written_log(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    hq_env.stop_server()

    file_size = os.path.getsize(journal_path)
    with open(journal_path, "a") as f:
        f.truncate(file_size - 1)

    hq_env.start_server(args=["--journal", journal_path])
    hq_env.command(["submit", "--array=0-3", "--", "hostname"])
    time.sleep(1.0)
    hq_env.stop_server()

    hq_env.start_server(args=["--journal", journal_path])
    out = hq_env.command(["--output-mode=json", "job", "list"], as_json=True)
    assert len(out) == 2


def test_restore_queues(hq_env: HqEnv, tmp_path):
    queue = ManagerQueue()
    handler = ExtractSubmitScriptPath(queue, SlurmManager())

    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])

    with MockJobManager(hq_env, adapt_slurm(handler)):
        add_queue(
            hq_env,
            manager="slurm",
            time_limit="3m",
            additional_args="--foo=bar a b --baz 42",
        )
        add_queue(
            hq_env,
            manager="slurm",
            time_limit="2m",
        )
        add_queue(
            hq_env,
            manager="slurm",
            time_limit="1m",
        )
        remove_queue(
            hq_env,
            2,
        )

        alloc_list1 = hq_env.command(["--output-mode=json", "alloc", "list"], as_json=True)
        assert len(alloc_list1) == 2

        hq_env.stop_server()
        hq_env.start_server(args=["--journal", journal_path])

        alloc_list2 = hq_env.command(["--output-mode=json", "alloc", "list"], as_json=True)
        assert alloc_list1 == alloc_list2

        add_queue(
            hq_env,
            manager="slurm",
            time_limit="1m",
        )
        alloc_list3 = hq_env.command(["--output-mode=json", "alloc", "list"], as_json=True)
        assert len(alloc_list3) == 3
        assert set(q["id"] for q in alloc_list3) == {1, 3, 4}

        hq_env.stop_server()
        hq_env.start_server(args=["--journal", journal_path])

        alloc_list4 = hq_env.command(["--output-mode=json", "alloc", "list"], as_json=True)
        assert alloc_list3 == alloc_list4


def test_restore_open_job(hq_env: HqEnv, tmp_path):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path])
    for _ in range(3):
        hq_env.command(["job", "open"])
    hq_env.command(["job", "close", "2"])
    hq_env.stop_server()
    hq_env.start_server(args=["--journal", journal_path])

    table = hq_env.command(["job", "info", "1"], as_table=True)
    print(table)

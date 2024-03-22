import time

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
    hq_env.command(["submit", "--array=0-4", "--", "python", "-c",
                    "import os, time;"
                    f"open('{tmp_path}/task-marker' + os.environ.get('HQ_TASK_ID'), 'w').write(os.environ.get('HQ_INSTANCE_ID'));"
                    "time.sleep(1.5 if os.environ.get('HQ_TASK_ID') in ('1','3','4') else 0);"
                    ])
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
    hq_env.command(["submit", "--array=0-4", "--", "python", "-c",
                    f"import os, time; open('{tmp_path}/task-marker' + os.environ.get('HQ_TASK_ID'), 'w');"
                    "time.sleep(1.5 if os.environ.get('HQ_TASK_ID') in ('1','3','4') else 0); sys.exit(1)"
                    ])
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

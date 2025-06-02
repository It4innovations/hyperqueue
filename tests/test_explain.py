from .utils import wait_for_task_state
from .conftest import HqEnv
import re

from inline_snapshot import snapshot


def test_explain_single_node(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=5, args=["--time-limit=5min"])
    hq_env.command(["submit", "sleep", "10"])
    hq_env.command(["submit", "--cpus=10", "sleep", "1"])
    hq_env.command(["submit", "--cpus=10", "--time-request=1h", "sleep", "1"])

    r = hq_env.command(["task", "explain", "1", "0", "all"])
    assert r == snapshot("""Task 1@0 is ready to run because it has no dependencies.
+-----------+----------+---------+------+----------+---------+
| Worker Id | Runnable | Variant | Type | Provides | Request |
+-----------+----------+---------+------+----------+---------+
| 1         | Yes      |         |      |          |         |
+-----------+----------+---------+------+----------+---------+
""")
    r = hq_env.command(["task", "--output-mode=json", "explain", "1", "0", "1"], as_json=True)
    assert r == {
        "explanation": {"n_task_deps": 0, "n_waiting_deps": 0, "workers": [{"variants": [[]], "worker_id": 1}]},
        "task_id": {"job_id": 1, "job_task_id": 0},
    }

    r = hq_env.command(["task", "explain", "2", "0", "1"])
    assert r == snapshot("""Task 2@0 is ready to run because it has no dependencies.
+-----------+----------+---------+------+----------+---------+
| Worker Id | Runnable | Variant | Type | Provides | Request |
+-----------+----------+---------+------+----------+---------+
| 1         | No       | 0       | cpus | 5        | 10      |
+-----------+----------+---------+------+----------+---------+
""")
    r = hq_env.command(["task", "--output-mode=json", "explain", "2", "0", "1"], as_json=True)

    assert r == {
        "explanation": {
            "n_task_deps": 0,
            "n_waiting_deps": 0,
            "workers": [
                {
                    "variants": [
                        [{"Resources": {"request_amount": 100000, "resource": "cpus", "worker_amount": 50000}}]
                    ],
                    "worker_id": 1,
                }
            ],
        },
        "task_id": {"job_id": 2, "job_task_id": 0},
    }

    r = hq_env.command(["task", "explain", "3", "0", "1"])
    r = re.sub(r"5\ds", "5Xs", r, count=1)
    assert r == snapshot("""Task 3@0 is ready to run because it has no dependencies.
+-----------+----------+---------+------+----------+---------+
| Worker Id | Runnable | Variant | Type | Provides | Request |
+-----------+----------+---------+------+----------+---------+
| 1         | No       | 0       | time | 4m 5Xs   | 1h      |
|           |          |         | cpus | 5        | 10      |
+-----------+----------+---------+------+----------+---------+
""")
    r = hq_env.command(["task", "--output-mode=json", "explain", "3", "0", "1"], as_json=True)
    del r["explanation"]["workers"][0]["variants"][0][0]["Time"]["remaining_time"]
    assert r == {
        "explanation": {
            "n_task_deps": 0,
            "n_waiting_deps": 0,
            "workers": [
                {
                    "variants": [
                        [
                            {"Time": {"min_time": {"nanos": 0, "secs": 3600}}},
                            {"Resources": {"request_amount": 100000, "resource": "cpus", "worker_amount": 50000}},
                        ]
                    ],
                    "worker_id": 1,
                }
            ],
        },
        "task_id": {"job_id": 3, "job_task_id": 0},
    }


def test_explain_multi_node(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=5, args=["--group=A"])
    hq_env.start_worker(cpus=5, args=["--group=B"])
    hq_env.start_worker(cpus=5, args=["--group=A"])

    hq_env.command(["submit", "--nodes=3", "sleep", "10"])

    r = hq_env.command(["task", "explain", "1", "0", "1"])
    assert r == snapshot("""Task 1@0 is ready to run because it has no dependencies.
+-----------+----------+---------+-------+--------------+---------+
| Worker Id | Runnable | Variant | Type  | Provides     | Request |
+-----------+----------+---------+-------+--------------+---------+
| 1         | No       | 0       | nodes | group size 2 | 3       |
+-----------+----------+---------+-------+--------------+---------+
""")


def test_explain_variants(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_workers(2, cpus=4)
    hq_env.start_worker(cpus=2, args=["--resource", "gpus=[0,1]"])
    hq_env.start_workers(2, cpus=4)

    tmp_path.joinpath("job.toml").write_text(
        """
[[task]]
id = 0
command = ["sleep", "1"]

[[task.request]]
resources = { "cpus" = "8" }

[[task.request]]
resources = { "cpus" = "1", "gpus" = "1" }

[[task]]
id = 1
command = ["sleep", "1"]

[[task.request]]
resources = { "cpus" = "2" }

[[task.request]]
resources = { "cpus" = "1", "gpus" = "1" }
"""
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    r = hq_env.command(["task", "explain", "1", "1", "1"])
    assert r == snapshot("""Task 1@1 is ready to run because it has no dependencies.
+-----------+-----------+---------+------+----------+---------+
| Worker Id | Runnable  | Variant | Type | Provides | Request |
+-----------+-----------+---------+------+----------+---------+
| 1         | Yes (1/2) | 1       | gpus | 0        | 1       |
+-----------+-----------+---------+------+----------+---------+
""")


def test_explain_deps(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_workers(2, cpus=4)

    tmp_path.joinpath("job.toml").write_text(
        """
[[task]]
id = 0
command = ["sleep", "0"]

[[task]]
id = 1
command = ["sleep", "100"]

[[task]]
id = 2
command = ["sleep", "1"]
deps = [0, 1]
"""
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_task_state(hq_env, 1, [0, 1, 2], ["finished", "running", "waiting"])
    hq_env.command(["job", "submit-file", "job.toml"])
    r = hq_env.command(["task", "explain", "1", "2", "1"])
    assert r == snapshot("""Task 1@2 is not ready to run, because 1/2 dependencies are not finished.
+-----------+----------+---------+------+----------+---------+
| Worker Id | Runnable | Variant | Type | Provides | Request |
+-----------+----------+---------+------+----------+---------+
| 1         | Yes      |         |      |          |         |
+-----------+----------+---------+------+----------+---------+
""")

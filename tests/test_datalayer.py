from .conftest import HqEnv
from .utils import wait_for_job_state, wait_for_task_state
from .utils.wait import wait_until
from .utils.job import default_task_output

from contextlib import contextmanager
import os
import time
import json
import psutil
import hashlib


@contextmanager
def check_data_env(hq_env: HqEnv, tmp_path, server_args=None):
    journal_path = os.path.join(tmp_path, "my.journal")
    hq_env.start_server(args=["--journal", journal_path] + (server_args if server_args else []))

    def start_worker(**kwargs):
        if "args" in kwargs:
            args = kwargs["args"]
        else:
            args = []
            kwargs["args"] = args
        args.append("--overview-interval=200ms")
        return hq_env.start_worker(**kwargs)

    yield start_worker
    check_for_memory_leaks(hq_env)


def test_task_data_invalid_call(hq_env: HqEnv):
    hq_env.command(
        ["data", "get", "0", "out.txt"], use_server_dir=False, expect_fail="HQ_DATA_ACCESS variable not found"
    )


def get_datanode_stats(hq_env: HqEnv):
    output = hq_env.command(["journal", "replay"], ignore_stderr=True)
    workers = {}
    for line in output.splitlines(keepends=False):
        e = json.loads(line)["event"]
        if "hw-state" in e:
            worker_id = e["id"]
            workers[worker_id] = e["data-node"]
        elif e["type"] == "worker-lost":
            del workers[e["id"]]
    return workers


def check_for_memory_leaks(hq_env: HqEnv):
    objects = None
    worker_id = None

    def check():
        nonlocal objects, worker_id
        workers = get_datanode_stats(hq_env)
        for worker_id, data_node in workers.items():
            objects = data_node["objects"]
            if objects:
                return False
        return True

    def on_timeout():
        return f"Worker {worker_id} still holds some objects: {objects}"

    wait_until(check, on_timeout=on_timeout)


def test_data_create_no_consumer(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true

[[task]]
id = 0
command = ["bash", "-c", "set -e; echo 'abc' > test.txt; $HQ data put 1 test.txt"]
"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        start_worker()
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_job_state(hq_env, 1, "FINISHED")


def test_data_transfer_invalid_input_id(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 12
command = ["bash", "-c", "set -e; $HQ data get 3 test.txt"]
"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        start_worker()
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_job_state(hq_env, 1, "FAILED")
    with open(os.path.join(tmp_path, default_task_output(job_id=1, task_id=12, type="stderr"))) as f:
        assert "Input 3 not found" in f.read()


def test_data_transfer_invalid_upload(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 12
command = ["bash", "-c", "set -e; touch test.txt; $HQ data put 3 test.txt; $HQ data put 3 test.txt"]
"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        start_worker()
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_job_state(hq_env, 1, "FAILED")
    with open(os.path.join(tmp_path, default_task_output(job_id=1, task_id=12, type="stderr"))) as f:
        assert "/3 already exists" in f.read()


def test_data_transfer_same_worker(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 12
command = ["bash", "-c", "set -e; echo 'abc' > test.txt; sleep 1; $HQ data put 3 test.txt"]

[[task]]
id = 13
command = ["bash", "-c", "set -e; $HQ data get 0 out.txt; cat out.txt"]

[[task.data_deps]]
task_id = 12
output_id = 3 
"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        start_worker(cpus=4)
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_job_state(hq_env, 1, "FINISHED")
    with open(os.path.join(tmp_path, default_task_output(job_id=1, task_id=13, type="stdout"))) as f:
        assert f.read() == "abc\n"
    time.sleep(1)
    stats = get_datanode_stats(hq_env)
    assert stats[1]["stats"] == {
        "locally_downloaded_bytes": 4,
        "locally_downloaded_objects": 1,
        "locally_uploaded_bytes": 4,
        "locally_uploaded_objects": 1,
        "remotely_downloaded_bytes": 0,
        "remotely_downloaded_objects": 0,
        "remotely_uploaded_bytes": 0,
        "remotely_uploaded_objects": 0,
    }


def test_data_cleanup_when_task_failed(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 12
command = ["bash", "-c", "set -e; echo 'abc' > test.txt; sleep 1; $HQ data put 3 test.txt; exit 1"]

[[task]]
id = 13
command = ["bash", "-c", "set -e; $HQ data get 0 out.txt; cat out.txt"]

[[task.data_deps]]
task_id = 12
output_id = 3 
"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        start_worker(cpus=4)
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_job_state(hq_env, 1, "FAILED")
        print(hq_env.command(["job", "info", "1"]))


def test_data_transfer_different_worker(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 1
command = ["bash", "-c", "set -e; echo 'abc' > test.txt; sleep 1; $HQ data put 22 test.txt"]
[[task.request]]
resources = { "a" = 1 }


[[task]]
id = 2
command = ["bash", "-c", "set -e; $HQ data get 0 out.txt; cat out.txt"]
[[task.request]]
resources = { "b" = 1 }

[[task.data_deps]]
task_id = 1
output_id = 22
"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        start_worker(args=["--resource", "a=sum(1)"], hostname="localhost")
        start_worker(args=["--resource", "b=sum(1)"])
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_job_state(hq_env, 1, "FINISHED")
    with open(os.path.join(tmp_path, default_task_output(job_id=1, task_id=2, type="stdout"))) as f:
        assert f.read() == "abc\n"
    time.sleep(1)
    stats = get_datanode_stats(hq_env)
    assert stats[1]["stats"] == {
        "locally_downloaded_bytes": 0,
        "locally_downloaded_objects": 0,
        "locally_uploaded_bytes": 4,
        "locally_uploaded_objects": 1,
        "remotely_downloaded_bytes": 0,
        "remotely_downloaded_objects": 0,
        "remotely_uploaded_bytes": 4,
        "remotely_uploaded_objects": 1,
    }
    assert stats[2]["stats"] == {
        "locally_downloaded_bytes": 4,
        "locally_downloaded_objects": 1,
        "locally_uploaded_bytes": 0,
        "locally_uploaded_objects": 0,
        "remotely_downloaded_bytes": 4,
        "remotely_downloaded_objects": 1,
        "remotely_uploaded_bytes": 0,
        "remotely_uploaded_objects": 0,
    }


def test_data_transfer_failed_worker(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 1
command = ["bash", "-c", "set -e; echo 'abc' > test.txt; sleep 1; $HQ data put 22 test.txt"]
[[task.request]]
resources = { "a" = 1 }


[[task]]
id = 2
command = ["bash", "-c", "set -e; $HQ data get 0 out.txt; cat out.txt"]
[[task.request]]
resources = { "b" = 1 }

[[task.data_deps]]
task_id = 1
output_id = 22
"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        w = start_worker(args=["--resource", "a=sum(1)", "--heartbeat=10min"], hostname="localhost")
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_task_state(hq_env, 1, [1, 2], ["finished", "waiting"])
        p = psutil.Process(w.pid)
        p.suspend()
        start_worker(args=["--resource", "b=sum(1)", "--max-download-tries=1"])
        wait_for_job_state(hq_env, 1, "FAILED", timeout_s=30)
        table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
        err = table.get_column_value("Error")[1]
        assert "Fails to download data object" in err
        assert "it has input index 0" in err
        p.resume()


def test_worker_with_data_lost(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 1
command = ["bash", "-c", "set -e; echo 'abc' > test.txt; sleep 1; $HQ data put 22 test.txt"]
[[task.request]]
resources = { "a" = 1 }


[[task]]
id = 2
command = ["bash", "-c", "set -e; $HQ data get 0 out.txt; cat out.txt"]
[[task.request]]
resources = { "b" = 1 }

[[task.data_deps]]
task_id = 1
output_id = 22

"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        start_worker(args=["--resource", "a=sum(1)"], hostname="localhost")
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_task_state(hq_env, 1, [1, 2], ["finished", "waiting"])
        hq_env.kill_worker(1)
        time.sleep(1)
        start_worker(args=["--resource", "b=sum(1)", "--max-download-tries=1"])
        wait_for_job_state(hq_env, 1, "FAILED")
        table = hq_env.command(["task", "list", "1", "-v"], as_table=True)
        err = table.get_column_value("Error")[1]
        assert "Fails to download data object" in err
        assert "it has input index 0" in err


def test_data_transfer_failed_data_deps(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 1
command = ["bash", "-c", "set -e; touch in.txt $HQ data put 1 in.txt"]
[[task.request]]
resources = { "a" = 1 }


[[task]]
id = 2
command = ["bash", "-c", "sleep", "0"]
[[task.request]]
resources = { "b" = 1 }

[[task.data_deps]]
task_id = 1
output_id = 0

"""
    )
    with check_data_env(hq_env, tmp_path) as start_worker:
        start_worker(args=["--resource", "a=sum(1)"], hostname="localhost")
        start_worker(args=["--resource", "b=sum(1)"])
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_job_state(hq_env, 1, "FAILED")
        msg = hq_env.command(["task", "--output-mode", "json", "info", "1", "2"], as_json=True)[0]["error"]
        assert "did not produced expected output(s): 0" in msg


def test_data_transfer_big_file(hq_env: HqEnv, tmp_path):
    tmp_path.joinpath("generator.py").write_text(
        """
import random
r = random.Random(7890001)
f = open("in.txt", "wb")
for i in range(97):
    f.write(r.randbytes(3 * 1024 * 1024))
"""
    )
    tmp_path.joinpath("job.toml").write_text(
        """
data_layer = true
        
[[task]]
id = 1
command = ["bash", "-c", "set -e; python generator.py; touch in2.txt; $HQ data put 0 in.txt; $HQ data put 1 in2.txt"]
[[task.request]]
resources = { "a" = 1 }


[[task]]
id = 2
command = ["bash", "-c", "set -e; $HQ data get 0 out.txt; $HQ data get 1 out2.txt"]
[[task.request]]
resources = { "b" = 1 }

[[task.data_deps]]
task_id = 1
output_id = 0

[[task.data_deps]]
task_id = 1
output_id = 1

"""
    )
    with check_data_env(
        # Running unencrypted because debug mode is slow
        hq_env,
        tmp_path,
        server_args=["--disable-worker-authentication-and-encryption"],
    ) as start_worker:
        start_worker(args=["--resource", "a=sum(1)"], hostname="localhost")
        start_worker(args=["--resource", "b=sum(1)"])
        hq_env.command(["job", "submit-file", "job.toml"])
        wait_for_job_state(hq_env, 1, "FINISHED")
        with open("out.txt", "rb") as f:
            assert hashlib.md5(f.read()).hexdigest() == "be44c343cf99772daab97b6d7588de33"
    stats = get_datanode_stats(hq_env)
    assert stats[1]["stats"] == {
        "locally_downloaded_bytes": 0,
        "locally_downloaded_objects": 0,
        "locally_uploaded_bytes": 305135616,
        "locally_uploaded_objects": 2,
        "remotely_downloaded_bytes": 0,
        "remotely_downloaded_objects": 0,
        "remotely_uploaded_bytes": 305135616,
        "remotely_uploaded_objects": 2,
    }
    assert stats[2]["stats"] == {
        "locally_downloaded_bytes": 305135616,
        "locally_downloaded_objects": 2,
        "locally_uploaded_bytes": 0,
        "locally_uploaded_objects": 0,
        "remotely_downloaded_bytes": 305135616,
        "remotely_downloaded_objects": 2,
        "remotely_uploaded_bytes": 0,
        "remotely_uploaded_objects": 0,
    }

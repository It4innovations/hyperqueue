import collections
import multiprocessing
import time

import pytest

from .utils.wait import wait_until
from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.cmd import python
from .utils.io import read_file
from .utils.job import default_task_output, list_jobs


def test_worker_resources_display(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(
        cpus="4x2",
        args=[
            "--resource",
            "potato=range(1-12)",
            "--resource",
            "fairy=sum(1000_1000)",
            "--resource",
            "shark=[1,3,5,2]",
            "--resource",
            "small=sum(0.5)",
        ],
    )
    table = hq_env.command(["worker", "list"], as_table=True)
    assert table.get_column_value("Resources") == ["cpus 4x2; fairy 10001000; potato 12; shark 4; small 0.5"]

    table = hq_env.command(["worker", "info", "1"], as_table=True)
    print(table.get_row_value("Resources"))
    assert table.get_row_value("Resources") == "cpus: 4x2\nfairy: 10001000\npotato: 12\nshark: 4\nsmall: 0.5"


def test_task_resources_ignore_worker_without_resource(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        [
            "submit",
            "--resource",
            "fairy=1",
            "--resource",
            "potato=1000_000",
            "uname",
        ]
    )

    def check_unscheduled():
        time.sleep(0.5)
        table = hq_env.command(["job", "info", "1"], as_table=True)
        assert table.get_row_value("State") == "WAITING"

    hq_env.start_worker(cpus=4)
    check_unscheduled()

    hq_env.start_worker(cpus=4, args=["--resource", "fairy=sum(1000)"])
    check_unscheduled()

    hq_env.start_worker(cpus=4, args=["--resource", "fairy=sum(2)", "--resource", "potato=sum(500)"])
    check_unscheduled()


def test_task_resources_allocate(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        [
            "submit",
            "--resource",
            "fairy=1000",
            "--resource",
            "potato=1",
            "--",
            "bash",
            "-c",
            (  # no comma
                "echo $HQ_RESOURCE_REQUEST_fairy:$HQ_RESOURCE_REQUEST_potato"
                ":$HQ_RESOURCE_VALUES_fairy:$HQ_RESOURCE_VALUES_potato"
            ),
        ]
    )

    hq_env.start_worker(
        cpus=4,
        args=[
            "--resource",
            "fairy=sum(2000_000)",
            "--resource",
            "potato=range(30-35)",
        ],
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    with open(default_task_output()) as f:
        f_count, p_count, f_idx, p_idx = f.read().rstrip().split(":")
        assert f_count == "1000 compact"
        assert p_count == "1 compact"
        assert f_idx == ""
        assert 30 <= int(p_idx) <= 35


def test_task_resources_range_multiple_allocated_values(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4)

    hq_env.command(
        [
            "submit",
            "--array=1-3",
            "--resource",
            "fairy=2",
            "--",
            "bash",
            "-c",
            "sleep 1; echo $HQ_RESOURCE_REQUEST_fairy:$HQ_RESOURCE_VALUES_fairy",
        ]
    )
    hq_env.start_worker(cpus=4, args=["--resource", "fairy=range(31-36)"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    all_values = []
    for i in range(1, 4):
        with open(default_task_output(task_id=i)) as f:
            rq, indices = f.read().rstrip().split(":")
            assert rq == "2 compact"
            values = [int(x) for x in indices.split(",")]
            assert len(values) == 2
            all_values += values
    assert all(31 <= x <= 36 for x in all_values)
    assert len(set(all_values)) == 6


def test_task_resource_fractions_sum(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4, args=["--resource", "foo=sum(2)"])

    hq_env.command(
        [
            "submit",
            "--array=1-4",
            "--resource",
            "foo=0.5",
            "--",
            "bash",
            "-c",
            "date +%s%N\nsleep 1",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")
    times = []
    for task_id in range(1, 5):
        with open(default_task_output(task_id=task_id)) as f:
            times.append(int(f.readline().rstrip()))
    times.sort()
    assert ((times[-1] - times[0]) / 1000_000_000) < 0.15


def test_task_resource_fractions_scatter(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4, args=["--resource", "foo=[[a,b,c],[i, j, k],[x, y],[v, w]]"])

    hq_env.command(
        [
            "submit",
            "--resource",
            "foo=3.5",
            "--",
            "bash",
            "-c",
            "echo $HQ_RESOURCE_VALUES_foo",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")
    with open(default_task_output()) as f:
        r = f.readline().rstrip().split(",")
        assert len(r) == 4
        assert len(set(r)) == 4


def test_task_resource_fractions_sum2(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4, args=["--resource", "foo=sum(2.2)"])

    hq_env.command(
        [
            "submit",
            "--array=1-4",
            "--resource",
            "foo=0.6",
            "--",
            "bash",
            "-c",
            "date +%s%N\nsleep 1",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")
    times = []
    for task_id in range(1, 5):
        with open(default_task_output(task_id=task_id)) as f:
            times.append(int(f.readline().rstrip()))
    times.sort()
    assert 0.99 < ((times[-1] - times[0]) / 1000_000_000) < 1.25


def test_task_resource_fractions_sharing_small(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4, args=["--resource", "foo=[a,b]"])

    hq_env.command(
        [
            "submit",
            "--array=1-4",
            "--resource",
            "foo=0.4",
            "--",
            "bash",
            "-c",
            "date +%s%N\necho $HQ_RESOURCE_VALUES_foo\nsleep 1",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")
    times = []
    counts = collections.Counter()
    for task_id in range(1, 5):
        with open(default_task_output(task_id=task_id)) as f:
            times.append(int(f.readline().rstrip()))
            name = f.readline().strip()
            assert name in ["a", "b"]
            counts[name] += 1
    times.sort()
    assert ((times[-1] - times[0]) / 1000_000_000) < 0.15
    assert counts == collections.Counter({"a": 2, "b": 2})


def test_task_resource_fractions_sharing_larger(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4, args=["--resource", "foo=[a0, a1, a2, a3, a4, a5, a6, a7, a8, a9]"])

    hq_env.command(
        [
            "submit",
            "--array=1-4",
            "--resource",
            "foo=2.5",
            "--",
            "bash",
            "-c",
            "date +%s%N\necho $HQ_RESOURCE_VALUES_foo\nsleep 1",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")
    times = []
    counts_frac = collections.Counter()
    counts_whole = collections.Counter()
    for task_id in range(1, 5):
        with open(default_task_output(task_id=task_id)) as f:
            times.append(int(f.readline().rstrip()))
            names = f.readline().strip().split(",")
            for name in names[:-1]:
                counts_whole[name] += 1
            counts_frac[names[-1]] += 1
    times.sort()
    assert ((times[-1] - times[0]) / 1000_000_000) < 0.15
    assert tuple(counts_frac.values()) == (2, 2)
    assert tuple(counts_whole.values()) == (1,) * 8


def test_task_resources_allocate_string(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(args=["--resource", "foo=[a,b,c]"])

    hq_env.command(
        [
            "submit",
            "--array=1-4",
            "--resource",
            "foo=2",
            "--",
            "bash",
            "-c",
            "echo $HQ_RESOURCE_VALUES_foo",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    for task_id in range(1, 5):
        with open(default_task_output(task_id=task_id)) as f:
            indices = set(f.read().rstrip().split(","))
            assert len(indices) == 2
            assert indices.issubset(("a", "b", "c"))


def test_worker_resource_hwdetect_mem(hq_env: HqEnv):
    hq_env.start_server()
    resources = hq_env.command(["worker", "hwdetect"])

    assert "mem:" in resources
    for resource in resources.splitlines():
        if "mem:" in resource:
            value = resource.split("mem: ")[1]
            assert value != ""


def test_worker_set_gpu_env_for_task(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(args=["--resource", "gpus/nvidia=[0,1]"])
    hq_env.command(
        [
            "submit",
            "--resource",
            "gpus/nvidia=2",
            "--",
            "bash",
            "-c",
            """
echo $CUDA_VISIBLE_DEVICES
""",
        ]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")
    assert list(set(int(v) for v in line.split(",")) for line in read_file(default_task_output()).splitlines()) == [
        {0, 1}
    ]


@pytest.mark.parametrize(
    "env_and_res",
    (
        ("CUDA_VISIBLE_DEVICES", "gpus/nvidia"),
        ("ROCR_VISIBLE_DEVICES", "gpus/amd"),
    ),
)
def test_worker_detect_gpus_from_env(hq_env: HqEnv, env_and_res: str):
    env, resource = env_and_res
    hq_env.start_server()
    resources = hq_env.command(["worker", "hwdetect"], env={env: "1,3"})
    assert f"{resource}: [1,3]" in resources


def test_worker_detect_uuid_gpus_from_env(hq_env: HqEnv):
    hq_env.start_server()
    resources = hq_env.command(["worker", "hwdetect"], env={"CUDA_VISIBLE_DEVICES": "foo,bar"})
    assert "gpus/nvidia: [foo,bar]" in resources


def test_worker_detect_multiple_gpus_from_env(hq_env: HqEnv):
    hq_env.start_server()
    resources = hq_env.command(
        ["worker", "hwdetect"],
        env={
            "CUDA_VISIBLE_DEVICES": "0,1",
            "ROCR_VISIBLE_DEVICES": "1",
        },
    )
    assert "gpus/nvidia: [0,1]" in resources
    assert "gpus/amd: [1]" in resources


@pytest.mark.skipif(
    multiprocessing.cpu_count() < 2,
    reason="This test needs at least two cores to be available",
)
def test_worker_detect_respect_cpu_mask(hq_env: HqEnv):
    hq_env.start_server()
    resources = hq_env.command(["worker", "hwdetect"], cmd_prefix=["taskset", "-c", "1"])
    assert "cpus: [1]" in resources


def test_task_info_resources(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker()

    hq_env.command(
        [
            "submit",
            "--array=1-3",
            "--resource",
            "fairy=2",
            "sleep 0",
        ]
    )

    table = hq_env.command(["task", "info", "1", "1"], as_table=True)
    table.check_row_value("Resources", "cpus: 1 compact\nfairy: 2 compact")


def test_string_resource_list(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        [
            "submit",
            "--array=1-3",
            "--resource",
            "fairy=2",
            "sleep 0",
        ]
    )

    table = hq_env.command(["task", "info", "1", "1"], as_table=True)
    table.check_row_value("Resources", "cpus: 1 compact\nfairy: 2 compact")


def test_resource_name_slash(hq_env: HqEnv):
    hq_env.start_server()

    res_name = "gpus/amd"
    hq_env.command(["submit", "--resource", f"{res_name}=1", "ls"])
    hq_env.start_worker(args=["--resource", f"{res_name}=[0]"])
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_resource_name_ensure_normalization(hq_env: HqEnv):
    hq_env.start_server()

    res_name = "gpus/amd"
    hq_env.command(
        [
            "submit",
            "--resource",
            f"{res_name}=1",
            "--",
            *python(
                """
import os
import sys
print(os.environ["HQ_RESOURCE_REQUEST_gpus_amd"], flush=True)
print(os.environ["HQ_RESOURCE_VALUES_gpus_amd"], flush=True)
"""
            ),
        ]
    )
    hq_env.start_worker(args=["--resource", f"{res_name}=[0]"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    with open(default_task_output()) as f:
        assert f.read() == "1 compact\n0\n"


def test_cpu_resource_sum(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        args=["worker", "start", "--cpus=sum(5)"],
        expect_fail="Resource kind `sum` cannot be used with CPUs",
    )


def test_manual_cpu_resource_sum(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        args=["worker", "start", "--resource", "cpus=sum(5)"],
        expect_fail="Resource kind `sum` cannot be used with CPUs",
    )


def test_resources_invalid_definitions(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.command(
        args=["submit", "--resource", "foo=10", "--resource", "foo=1", "--", "sleep", "1"],
        expect_fail="Resource 'foo' defined more than once",
    )

    hq_env.command(
        args=["submit", "--resource", "foo=0", "--", "sleep", "1"],
        expect_fail="Zero resources cannot be requested",
    )


def test_resources_and_priorities_one_by_one_submit(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(args=["--resource", "foo=[1, 2, 3, 4]"], cpus=8)

    for i in range(1, 5):
        hq_env.command(["submit", "--resource", "foo=2", "--", "sleep", "100"])
        if i < 3:
            wait_for_job_state(hq_env, i, "RUNNING")
    time.sleep(0.6)
    wait_for_job_state(hq_env, [1, 2], "RUNNING")
    wait_for_job_state(hq_env, [3, 4], "WAITING")
    time.sleep(1.0)
    hq_env.command(["submit", "--cpus=2", "--priority=-1", "--", "sleep", "100"])

    def check():
        table = list_jobs(hq_env)
        return table.get_column_value("State") == ["RUNNING", "RUNNING", "WAITING", "WAITING", "RUNNING"]

    wait_until(check, timeout_s=5)


def test_resources_and_priorities_submit_before_worker(hq_env: HqEnv):
    hq_env.start_server()
    for i in range(1, 5):
        hq_env.command(["submit", "--resource", "foo=2", "--", "sleep", "100"])
    hq_env.command(["submit", "--cpus=2", "--priority=-1", "--", "sleep", "100"])
    hq_env.start_worker(args=["--resource", "foo=[1, 2, 3, 4]"], cpus=8)

    def check():
        table = list_jobs(hq_env)
        s = table.get_column_value("State")
        return s[-1] == "RUNNING" and s[:-1].count("RUNNING") == 2 and s[:-1].count("WAITING") == 2

    wait_until(check, timeout_s=5)


def test_resources_and_many_priorities(hq_env: HqEnv):
    hq_env.start_server()
    for i in range(1, 10):
        hq_env.command(["submit", "--resource", "foo=2", f"--priority={i * 10}", "--", "sleep", "100"])
    for i in range(12):
        hq_env.command(["submit", "--cpus=1", f"--priority={i // 3 - 5}", "--", "sleep", "100"])
    hq_env.start_worker(args=["--resource", "foo=[1, 2, 3, 4, 5, 6]"], cpus=12)

    def check():
        table = list_jobs(hq_env)
        s = table.get_column_value("State")
        return s == 6 * ["WAITING"] + 3 * ["RUNNING"] + 3 * ["WAITING"] + 9 * ["RUNNING"]

    wait_until(check, timeout_s=5)

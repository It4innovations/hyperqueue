import time

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.job import default_task_output


def test_worker_resources_display(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(
        cpus=4,
        args=[
            "--resource",
            "potato=range(1-12)",
            "--resource",
            "fairy=sum(1000_1000)",
            "--resource",
            "shark=list(1,3,5,2)",
        ],
    )
    table = hq_env.command(["worker", "list"], as_table=True)
    assert table.get_column_value("Resources") == [
        "1x4 cpus; fairy Sum(10001000); potato Range(1-12); shark List(1,2,3,5)"
    ]

    table = hq_env.command(["worker", "info", "1"], as_table=True)
    assert (
        table.get_row_value("Resources")
        == "1x4 cpus\nfairy: Sum(10001000)\npotato: Range(1-12)\nshark: List(1,2,3,5)"
    )


def test_task_resources_ignore_worker_without_resource(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        [
            "submit",
            "--resource",
            "fairy=1",
            "--resource",
            "potato=1000_000",
            "hostname",
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

    hq_env.start_worker(
        cpus=4, args=["--resource", "fairy=sum(2)", "--resource", "potato=sum(500)"]
    )
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
            "echo $HQ_RESOURCE_REQUEST_fairy:$HQ_RESOURCE_REQUEST_potato"  # no comma
            ":$HQ_RESOURCE_VALUES_fairy:$HQ_RESOURCE_VALUES_potato",
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
        assert f_count == "1000"
        assert p_count == "1"
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
    hq_env.start_worker(cpus=4, args=["--resource", "--resource", "fairy=range(31-36)"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    all_values = []
    for i in range(1, 4):
        with open(default_task_output(task_id=i)) as f:
            rq, indices = f.read().rstrip().split(":")
            assert rq == "2"
            values = [int(x) for x in indices.split(",")]
            assert len(values) == 2
            all_values += values
    assert all(31 <= x <= 36 for x in all_values)
    assert len(set(all_values)) == 6


def test_worker_resource_hwdetect_mem(hq_env: HqEnv):
    hq_env.start_server()
    resources = hq_env.command(["worker", "hwdetect"])

    assert "mem:" in resources
    for resource in resources.splitlines():
        if "mem:" in resource:
            value = resource.split("mem: ")[1]
            assert value != ""


def test_worker_detect_gpus_from_env(hq_env: HqEnv):
    hq_env.start_server()
    resources = hq_env.command(
        ["worker", "hwdetect"], env={"CUDA_VISIBLE_DEVICES": "1,3"}
    )
    assert "gpus: List(1,3)" in resources

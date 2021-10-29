import time

from .conftest import HqEnv
from .utils import wait_for_job_state


def test_worker_resources(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(
        cpus=4,
        args=[
            "--resource",
            "potato=indices(1-12)",
            "--resource",
            "fairy=sum(1000_1000)",
        ],
    )
    table = hq_env.command(["worker", "list"], as_table=True)
    assert table.get_column_value("Resources") == [
        "1x4 cpus; fairy 10001000; potato 12"
    ]

    table = hq_env.command(["worker", "info", "1"], as_table=True)
    assert (
        table.get_row_value("Resources")
        == "1x4 cpus\nfairy: Sum(10001000)\npotato: Indices(1-12)"
    )


def test_task_resources1(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=4)

    hq_env.command(
        [
            "submit",
            "--resource",
            "fairy=1",
            "--resource",
            "potato=1000_000",
            "--",
            "bash",
            "-c",
            "echo $HQ_RESOURCE_REQUEST_fairy:$HQ_RESOURCE_REQUEST_potato"  # no comma here
            ":$HQ_RESOURCE_INDICES_fairy:$HQ_RESOURCE_INDICES_potato",
        ]
    )
    time.sleep(0.4)
    table = hq_env.command(["job", "1"], as_table=True)
    assert table.get_row_value("State") == "WAITING"
    assert (
        table.get_row_value("Resources") == "cpus: 1 compact\nfairy: 1\npotato: 1000000"
    )

    hq_env.start_worker(cpus=4, args=["--resource", "potato=sum(2000_000)"])
    time.sleep(0.4)
    assert table.get_row_value("State") == "WAITING"

    hq_env.start_worker(
        cpus=4,
        args=[
            "--resource",
            "potato=sum(2000_000)",
            "--resource",
            "fairy=indices(30-35)",
        ],
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    with open("job-1/0.stdout") as f:
        f_count, p_count, f_idx, p_idx = f.read().rstrip().split(":")
        assert f_count == "1"
        assert p_count == "1000000"
        assert 30 <= int(f_idx) <= 35
        assert p_idx == ""


def test_task_resources2(hq_env: HqEnv):
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
            "sleep 1; echo $HQ_RESOURCE_REQUEST_fairy:$HQ_RESOURCE_INDICES_fairy",
        ]
    )
    hq_env.start_worker(
        cpus=4, args=["--resource", "--resource", "fairy=indices(31-36)"]
    )
    wait_for_job_state(hq_env, 1, "FINISHED")

    all_values = []
    for i in range(1, 4):
        with open(f"job-1/{i}.stdout") as f:
            rq, indices = f.read().rstrip().split(":")
            assert rq == "2"
            values = [int(x) for x in indices.split(",")]
            assert len(values) == 2
            all_values += values
    assert all(31 <= x <= 36 for x in all_values)
    assert len(set(all_values)) == 6

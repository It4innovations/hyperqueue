import collections
import time

from .conftest import HqEnv
from .utils import wait_for_job_state
from .utils.io import check_file_contents
from .utils.job import default_task_output


def test_job_file_submit_minimal(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker()
    tmp_path.joinpath("job.toml").write_text(
        """
[[task]]
command = ["sleep", "0"]
    """
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_job_file_submit_maximal(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_workers(3, cpus=4, args=["--resource", "gpus=[0,1]"])
    tmp_path.joinpath("job.toml").write_text(
        """
name = "test-job"
stream_log = "output.log"
max_fails = 11

[[task]]
id = 12
stdout = "testout-%{TASK_ID}"
stderr = "testerr-%{TASK_ID}"
task_dir = true
time_limit = "1m 10s"
priority = -1
crash_limit = 12
command = ["bash", "-c", "echo $ABC $XYZ; >&2 echo error; (echo \\"$(</dev/stdin)\\")"]
env = {"ABC" = "123", "XYZ" = "aaaa"}
stdin = "Hello world!"
[[task.request]]
n_nodes = 2

[[task]]
id = 200
pin = "taskset"
cwd = "test-200"
command = ["bash", "-c", "echo test1"]
[[task.request]]
resources = { "cpus" = "4 compact!", "gpus" = 2 }
time_request = "10s"


[[task]]
id = 13
pin = "omp"
command = ["sleep", "0"]
"""
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    table = hq_env.command(["job", "info", "1"], as_table=True)
    table.check_row_value("Name", "test-job")

    table = hq_env.command(["task", "info", "1", "12"], as_table=True)
    table.check_row_value("Environment", "ABC=123\nXYZ=aaaa")
    table.check_row_value("Pin", "none")
    table.check_row_value("Time limit", "1m 10s")
    table.check_row_value("Priority", "-1")
    table.check_row_value("Crash limit", "12")
    table.check_row_value("Task dir", "yes")
    table.check_row_value("Resources", "nodes: 2")
    paths = table.get_row_value("Paths").split("\n")
    assert paths[1].endswith("testout-12")
    assert paths[2].endswith("testerr-12")

    r = hq_env.command(["job", "cat", "1", "stdout", "--tasks", "12"]).strip()
    assert r == "123 aaaa\nHello world!"

    table = hq_env.command(["task", "info", "1", "200"], as_table=True)
    table.check_row_value("Pin", "taskset")
    table.check_row_value("Time limit", "None")
    table.check_row_value("Priority", "0")
    table.check_row_value("Crash limit", "0")
    table.check_row_value("Task dir", "no")

    paths = table.get_row_value("Paths").split("\n")
    assert paths[0].endswith("test-200")
    assert paths[1].endswith("/test-200/job-1/200.stdout")
    assert paths[2].endswith("/test-200/job-1/200.stderr")

    table = hq_env.command(["task", "info", "1", "13"], as_table=True)
    table.check_row_value("Pin", "omp")


def test_job_file_resource_variants1(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_workers(2, cpus=4)
    hq_env.start_worker(cpus=2, args=["--resource", "gpus=[0,1]"])
    hq_env.start_workers(2, cpus=4)

    tmp_path.joinpath("job.toml").write_text(
        """
[[task]]
id = 0
command = ["sleep", "0"]

[[task.request]]
resources = { "cpus" = "8" }

[[task.request]]
resources = { "cpus" = "1", "gpus" = "1" }
"""
    )
    hq_env.command(["job", "submit-file", "job.toml"])

    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
    table.check_row_value(
        "Resources",
        "# Variant 1\ncpus: 8 compact\n# Variant 2\ncpus: 1 compact\ngpus: 1 compact",
    )
    table.check_row_value("Worker", "worker3")
    wait_for_job_state(hq_env, 1, "FINISHED")


def test_job_file_resource_variants2(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_workers(2, cpus=2)
    hq_env.start_workers(1, cpus=4, args=["--resource", "x=[0,1]"])
    hq_env.start_workers(2, cpus=2, args=["--resource", "x=[0]", "--resource", "y=[0]"])

    tmp_path.joinpath("job.toml").write_text(
        """
    [[task]]
    id = 0
    command = ["/bin/bash",
               "-c",
               "echo $HQ_RESOURCE_REQUEST_cpus,$HQ_RESOURCE_REQUEST_x,$HQ_RESOURCE_REQUEST_y,$HQ_RESOURCE_VARIANT"]
    [[task.request]]
    resources = { "cpus" = "8" }

    [[task.request]]
    resources = { "cpus" = "1", "gpus" = "1" }

    [[task.request]]
    resources = { "cpus" = "4", "x" = "1" }
    """
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")
    table = hq_env.command(["task", "info", "1", "0"], as_table=True)
    table.check_row_value("Worker", "worker3")
    r = hq_env.command(["job", "cat", "1", "stdout"]).strip()
    assert r == "4 compact,1 compact,,2"


def test_job_file_resource_variants3(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker(cpus=16, args=["--resource", "x=[0,1]"])

    tmp_path.joinpath("job.toml").write_text(
        "\n".join(
            [
                f"""
    [[task]]
    id = {x}
    command = ["/bin/bash",
               "-c",
               "sleep 1; echo ${{HQ_RESOURCE_REQUEST_cpus}},${{HQ_RESOURCE_REQUEST_x}},${{HQ_RESOURCE_VARIANT}}"]
    [[task.request]]
    resources = {{ "cpus" = "1", "x"=1 }}
    [[task.request]]
    resources = {{ "cpus" = "4" }}
    """
                for x in range(5)
            ]
        )
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    r = hq_env.command(["job", "cat", "1", "stdout"])
    c = collections.Counter(r.strip().split("\n"))
    assert c["1 compact,1 compact,0"] == 2
    assert c["4 compact,,1"] == 3


def test_job_file_auto_id(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker()
    tmp_path.joinpath("job.toml").write_text(
        """
[[task]]
command = ["sleep", "0"]

[[task]]
id = 12
command = ["sleep", "0"]

[[task]]
id = 3
command = ["sleep", "0"]

[[task]]
command = ["sleep", "0"]

[[task]]
command = ["sleep", "0"]
    """
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")
    r = hq_env.command(["--output-mode=json", "job", "info", "1"], as_json=True)
    ids = set(x["id"] for x in r[0]["tasks"])
    assert ids == {3, 12, 13, 14, 15}


def test_job_file_array(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    tmp_path.joinpath("job.toml").write_text(
        """
[array]
ids = "2, 10-14, 120"
command = ["sleep", "0"]
    """
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    r = hq_env.command(["job", "info", "1"], as_table=True)
    r.check_row_value("Tasks", "7; Ids: 2, 10-14, 120")


def test_job_file_fail_mixing_array_and_tasks(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    tmp_path.joinpath("job.toml").write_text(
        """
[array]
ids = "2"
command = ["sleep", "0"]

[[task]]
id = 1
command = ["sleep", "0"]
    """
    )
    hq_env.command(
        ["job", "submit-file", "job.toml"],
        expect_fail="Definition of array job and individual task cannot be mixed",
    )


def test_job_file_array_entries_without_ids(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker()
    tmp_path.joinpath("job.toml").write_text(
        """
[array]
entries = ["a", "bb", "ccc"]
command = ["/bin/bash", "-c", "echo $HQ_ENTRY"]
    """
    )

    expected = {0: "a", 1: "bb", 2: "ccc"}

    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i in range(3):
        stdout = default_task_output(job_id=1, task_id=i, type="stdout")
        check_file_contents(stdout, expected[i] + "\n")


def test_job_file_array_entries_with_ids(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker()
    tmp_path.joinpath("job.toml").write_text(
        """
[array]
ids = "2,10-12"
entries = ["a", "bb", "ccc", "x"]
command = ["/bin/bash", "-c", "echo $HQ_ENTRY"]
    """
    )

    expected = {2: "a", 10: "bb", 11: "ccc", 12: "x"}

    hq_env.command(["job", "submit-file", "job.toml"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    for i in expected.keys():
        stdout = default_task_output(job_id=1, task_id=i, type="stdout")
        check_file_contents(stdout, expected[i] + "\n")


def test_job_file_dependencies(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.start_worker()
    tmp_path.joinpath("job.toml").write_text(
        """
[[task]]
id = 1
command = ["sleep", "0"]

[[task]]
id = 3
command = ["sleep", "0"]

[[task]]
id = 5
command = ["sleep", "0"]
deps = [1, 3]
    """
    )
    hq_env.command(["job", "submit-file", "job.toml"])
    table = hq_env.command(["task", "info", "1", "5"], as_table=True)
    table.check_row_value("Dependencies", "1,3")
    table = hq_env.command(["task", "info", "1", "1"], as_table=True)
    table.check_row_value("Dependencies", "")
    table = hq_env.command(["task", "info", "1", "3"], as_table=True)
    table.check_row_value("Dependencies", "")

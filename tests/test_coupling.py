from .utils import wait_for_job_state
from .utils.cmd import bash
from .conftest import HqEnv


def test_coupling_invalid_inputs(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(args=["--coupling", "cpus"], expect_fail="Resource coupling needs at least two resources")
    hq_env.start_worker(args=["--coupling", "cpus,xxx"], expect_fail="Coupling of unknown resource: 'xxx'")
    hq_env.start_worker(
        args=["--resource", "xxx=[1,2,3]", "--resource", "yyy=[[1,2,3], [10,11]]", "--coupling", "xxx,yyy"],
        expect_fail="Coupled resources needs to have the same number of groups",
    )


def test_coupling_output(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(
        cpus="[[1, 2, 3], [4, 5, 6]]",
        args=["--coupling=cpus,foo", "--resource=ddd=sum(123)", "--resource=foo=[[10,20,30],[40,50,60]]"],
    )
    table = hq_env.command(["worker", "info", "1"], as_table=True)
    table.check_row_value("Resources", "cpus: 2x3 [coupled]\nddd: 123\nfoo: 2x3 [coupled]")

    result = hq_env.command(["--output-mode=json", "worker", "info", "1"], as_json=True)
    assert result["configuration"]["resources"]["coupling"] == {"names": ["cpus", "foo"]}


def test_coupling_alloc1(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        [
            "submit",
            "--array=1-50",
            "--cpus=1",
            "--resource=foo=3 compact!",
            "--",
            *bash('echo "$HQ_RESOURCE_VALUES_cpus;$HQ_RESOURCE_VALUES_foo"'),
        ]
    )

    hq_env.command(
        [
            "submit",
            "--array=1-50",
            "--cpus=1",
            "--resource=foo=1 compact!",
            "--",
            *bash('echo "$HQ_RESOURCE_VALUES_cpus;$HQ_RESOURCE_VALUES_foo"'),
        ]
    )

    hq_env.start_worker(
        cpus="[[1, 2, 3], [4, 5, 6]]",
        args=["--coupling=cpus,foo", "--resource=ddd=sum(123)", "--resource=foo=[[10,20,30,40],[50,60,70,80]]"],
    )

    wait_for_job_state(hq_env, 1, "FINISHED")

    def get_groups(data):
        cpus, foos = (x.split(",") for x in data.rstrip().split(";"))
        g = set()
        for c in cpus:
            g.add(int(c) // 4)
        for f in foos:
            g.add(int(f) // 50)
        return g

    for i in range(1, 51):
        with open(f"job-1/{i}.stdout", "r") as f:
            assert len(get_groups(f.read())) == 1
    for i in range(1, 51):
        with open(f"job-2/{i}.stdout", "r") as f:
            assert len(get_groups(f.read())) == 1


def test_coupling_alloc2(hq_env: HqEnv):
    hq_env.start_server()

    hq_env.command(
        [
            "submit",
            "--cpus=4",
            "--resource=foo=2",
            "--",
            *bash('echo "$HQ_RESOURCE_VALUES_cpus;$HQ_RESOURCE_VALUES_foo"'),
        ]
    )

    hq_env.command(
        [
            "submit",
            "--cpus=2",
            "--resource=foo=4",
            "--",
            *bash('echo "$HQ_RESOURCE_VALUES_cpus;$HQ_RESOURCE_VALUES_foo"'),
        ]
    )

    hq_env.start_worker(
        cpus="[[1, 2, 3], [10, 11, 12], [21, 22, 23]]",
        args=["--coupling=cpus,foo", "--resource=ddd=sum(123)", "--resource=foo=[[1, 2, 3],[10, 11, 12],[20, 21, 23]]"],
    )

    wait_for_job_state(hq_env, 1, "FINISHED")

    def get_groups(data):
        cpus, foos = (x.split(",") for x in data.rstrip().split(";"))
        g = set()
        for c in cpus:
            g.add(int(c) // 10)
        for f in foos:
            g.add(int(f) // 10)
        return g

    with open(f"job-1/0.stdout", "r") as f:
        assert len(get_groups(f.read())) == 2
    with open(f"job-2/0.stdout", "r") as f:
        assert len(get_groups(f.read())) == 2

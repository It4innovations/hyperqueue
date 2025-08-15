import collections

from .utils.job import default_task_output
from .utils import wait_for_job_state
from .utils.cmd import bash
from .conftest import HqEnv


def test_coupling_invalid_inputs(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(
        args=["--coupling", "foo[0]:goo[1]"],
        expect_fail="Resource not found: foo",
    )
    hq_env.start_worker(
        args=["--resource", "xxx=[1,2,3]", "--resource", "yyy=[[1,2,3], [10,11]]", "--coupling", "xxx[0]:yyy[0]"],
        expect_fail="Resource 'xxx' has only a single group",
    )
    hq_env.start_worker(
        args=[
            "--resource",
            "xxx=[[1,2,3], [10, 20, 30]]",
            "--resource",
            "yyy=[[1,2,3], [10,11]]",
            "--coupling",
            "xxx[0]:yyy[3]",
        ],
        expect_fail="Invalid group id 3 for resource 'yyy'",
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

    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    def get_groups(data):
        cpus, foos = (x.split(",") for x in data.rstrip().split(";"))
        g = set()
        for c in cpus:
            g.add(int(c) // 4)
        for f in foos:
            g.add(int(f) // 50)
        return g

    for i in range(1, 51):
        with open(default_task_output(job_id=1, task_id=i), "r") as f:
            assert len(get_groups(f.read())) == 1
    for i in range(1, 51):
        with open(default_task_output(job_id=2, task_id=i), "r") as f:
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

    with open(default_task_output(job_id=1), "r") as f:
        assert len(get_groups(f.read())) == 2
    with open(default_task_output(job_id=2), "r") as f:
        assert len(get_groups(f.read())) == 2


def test_coupling_combined(hq_env: HqEnv):
    def groups(job_id):
        with open(default_task_output(job_id)) as f:
            a, b = f.read().rstrip().split(";")
            data1 = [int(x) // 10 for x in a.split(",")]
            data2 = [int(x) // 10 for x in b.split(",")]
        c1 = collections.Counter(data1)
        c2 = collections.Counter(data2)
        return sorted(c1.values()), sorted(c2.values())

    hq_env.start_server()

    hq_env.start_worker(
        cpus="[[1, 2, 3, 4], [11, 12, 13, 14], [21, 22, 23, 24]]",
        args=["--resource=foo=[[1, 2],[10,11],[22,21]]", "--coupling=cpus,foo"],
    )

    for i, (cpus, foos, expect) in enumerate(
        [
            ("6 tight", "2 tight", ([2, 4], [2])),
            ("6 scatter", "2 scatter", ([2, 2, 2], [1, 1])),
            ("6 compact", "2 compact", ([3, 3], [1, 1])),
            ("6 tight", "2 compact", ([2, 4], [1, 1])),
            ("6 compact", "2 tight", ([3, 3], [2])),
            ("6 scatter", "2 tight", ([2, 2, 2], [2])),
            ("6 scatter", "2 compact", ([2, 2, 2], [2])),
        ],
        1,
    ):
        hq_env.command(
            [
                "submit",
                f"--cpus={cpus}",
                "--resource",
                f"foo={foos}",
                "--",
                *bash('echo "$HQ_RESOURCE_VALUES_cpus;$HQ_RESOURCE_VALUES_foo"'),
            ]
        )
        wait_for_job_state(hq_env, i, "FINISHED")
        assert groups(i) == expect

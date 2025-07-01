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

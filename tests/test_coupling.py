from .conftest import HqEnv


def test_coupling_invalid_inputs(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(args=["--coupling", "cpus"], expect_fail="Resource coupling needs at least two resources")
    hq_env.start_worker(args=["--coupling", "cpus,xxx"], expect_fail="Coupling of unknown resource: 'xxx'")
    hq_env.start_worker(
        args=[
            "--resource",
            "xxx=[1,2,3]",
            "--resource",
            "yyy=[1,2,3]",
            "--resource",
            "zzz=[1,2,3]",
            "--coupling",
            "xxx,yyy",
            "--coupling",
            "yyy,zzz",
        ],
        expect_fail="Resource 'yyy' is in more than one coupling",
    )
    hq_env.start_worker(
        args=["--resource", "xxx=[1,2,3]", "--resource", "yyy=[[1,2,3], [10,11]]", "--coupling", "xxx,yyy"],
        expect_fail="Coupled resources needs to have the same number of groups",
    )

import time

from .conftest import HqEnv


def test_manager_autodetect(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(n_cpus=1)
    time.sleep(0.1)
    hq_env.start_worker(n_cpus=1, env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"})
    time.sleep(0.1)
    hq_env.start_worker(n_cpus=1, env={"SLURM_JOB_ID": "y5678"})
    time.sleep(0.2)

    table = hq_env.command(["worker", "list"], as_table=True)

    assert table[0][4] == "Manager"
    assert table[0][5] == "Manager Job Id"

    assert table[1][4] == "None"
    assert table[1][5] == "N/A"

    assert table[2][4] == "PBS"
    assert table[2][5] == "x1234"

    assert table[3][4] == "SLURM"
    assert table[3][5] == "y5678"


def test_manager_set_none(hq_env: HqEnv):
    hq_env.start_server()
    args = ["--manager", "none"]
    hq_env.start_worker(n_cpus=1, args=args)
    time.sleep(0.1)
    hq_env.start_worker(n_cpus=1, args=args, env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"})
    time.sleep(0.1)
    hq_env.start_worker(n_cpus=1, args=args, env={"SLURM_JOB_ID": "y5678"})
    time.sleep(0.2)

    table = hq_env.command(["worker", "list"], as_table=True)

    assert table[0][4] == "Manager"
    assert table[0][5] == "Manager Job Id"

    for i in [1, 2, 3]:
        assert table[i][4] == "None"
        assert table[i][5] == "N/A"


def test_manager_set_pbs(hq_env: HqEnv):
    hq_env.start_server()
    args = ["--manager", "pbs"]
    p = hq_env.start_worker(n_cpus=1, args=args)
    time.sleep(0.1)
    hq_env.check_process_exited(p, 1)
    time.sleep(0.2)
    hq_env.start_worker(n_cpus=1, args=args, env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"})
    time.sleep(0.2)

    table = hq_env.command(["worker", "list"], as_table=True)
    assert table[1][4] == "PBS"
    assert table[1][5] == "x1234"



def test_manager_set_slurm(hq_env: HqEnv):
    hq_env.start_server()
    args = ["--manager", "slurm"]
    p = hq_env.start_worker(n_cpus=1, args=args)
    time.sleep(0.1)
    hq_env.check_process_exited(p, 1)
    time.sleep(0.2)
    hq_env.start_worker(n_cpus=1, args=args, env={"SLURM_JOB_ID": "abcd"})
    time.sleep(0.2)

    table = hq_env.command(["worker", "list"], as_table=True)
    print(table)
    assert table[1][4] == "SLURM"
    assert table[1][5] == "abcd"


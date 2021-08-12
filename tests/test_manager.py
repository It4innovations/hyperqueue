import time

from .conftest import HqEnv


def test_manager_autodetect(hq_env: HqEnv):
    hq_env.start_server()
    hq_env.start_worker(cpus=1)
    hq_env.start_worker(
        cpus=1, env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"}
    )
    hq_env.start_worker(cpus=1, env={"SLURM_JOB_ID": "y5678"})

    table = hq_env.command(["worker", "list"], as_table=True)
    table.check_value_columns(["Manager", "Manager Job Id"], 0, ["None", "N/A"])
    table.check_value_columns(["Manager", "Manager Job Id"], 1, ["PBS", "x1234"])
    table.check_value_columns(["Manager", "Manager Job Id"], 2, ["SLURM", "y5678"])


def test_manager_set_none(hq_env: HqEnv):
    hq_env.start_server()
    args = ["--manager", "none"]
    hq_env.start_worker(cpus=1, args=args)
    hq_env.start_worker(
        cpus=1, args=args, env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"}
    )
    hq_env.start_worker(cpus=1, args=args, env={"SLURM_JOB_ID": "y5678"})

    table = hq_env.command(["worker", "list"], as_table=True)

    for i in [0, 1, 2]:
        table.check_value_columns(["Manager", "Manager Job Id"], i, ["None", "N/A"])


def test_manager_set_pbs(hq_env: HqEnv):
    hq_env.start_server()
    args = ["--manager", "pbs"]
    p = hq_env.start_worker(cpus=1, args=args)
    hq_env.check_process_exited(p, 1)

    qstat = """
import sys
import json

assert "x1234" in sys.argv

data = {
    "Jobs": {
        "x1234": {
            "Resource_list": {
                "walltime": "01:12:34"
            },
            "resources_used": {
                "walltime": "00:13:45"
            }
        }
    }
}
print(json.dumps(data))
"""

    with hq_env.mock.mock_program("qstat", qstat):
        hq_env.start_worker(
            cpus=1, args=args, env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"}
        )

        table = hq_env.command(["worker", "list"], as_table=True)
        table.check_value_columns(["Manager", "Manager Job Id"], 0, ["PBS", "x1234"])


def test_manager_set_slurm(hq_env: HqEnv):
    hq_env.start_server()
    args = ["--manager", "slurm"]
    p = hq_env.start_worker(cpus=1, args=args)
    hq_env.check_process_exited(p, 1)
    time.sleep(0.2)
    hq_env.start_worker(cpus=1, args=args, env={"SLURM_JOB_ID": "abcd"})
    table = hq_env.command(["worker", "list"], as_table=True)
    table.check_value_columns(["Manager", "Manager Job Id"], 0, ["SLURM", "abcd"])

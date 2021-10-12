import time

from .conftest import HqEnv


def scontrol_return(job_id: str) -> str:
    return f"""print(\"\"\"JobId={job_id} JobName=bash
   UserId=sboehm00(33646) GroupId=interactive(25200) MCS_label=N/A
   Priority=124370 Nice=0 Account=lig8_dev QOS=normal
   JobState=RUNNING Reason=None Dependency=(null)
   Requeue=0 Restarts=0 BatchFlag=0 Reboot=0 ExitCode=0:0
   RunTime=00:01:34 TimeLimit=00:15:00 TimeMin=N/A
   SubmitTime=2021-10-07T11:14:47 EligibleTime=2021-10-07T11:14:47
   AccrueTime=2021-10-07T11:14:47
   StartTime=2021-10-07T11:15:26 EndTime=2021-10-07T11:30:26 Deadline=N/A
   PreemptEligibleTime=2021-10-07T11:15:26 PreemptTime=None
   SuspendTime=None SecsPreSuspend=0 LastSchedEval=2021-10-07T11:15:26 Scheduler=Main
   Partition=m100_all_serial AllocNode:Sid=login01:58040
   ReqNodeList=(null) ExcNodeList=(null)
   NodeList=login06
   BatchHost=login06
   NumNodes=1 NumCPUs=4 NumTasks=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:*
   TRES=cpu=4,mem=7600M,node=1,billing=4
   Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
   MinCPUsNode=1 MinMemoryCPU=1900M MinTmpDiskNode=0
   Features=(null) DelayBoot=00:00:00
   OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
   Command=/usr/bin/bash
   WorkDir=/m100/home/userexternal/sboehm00
   Power=\"\"\")"""


def qstat_return_walltime(job_id: str) -> str:
    return f"""
import sys
import json

assert "{job_id}" in sys.argv

data = {{
    "Jobs": {{
        "{job_id}": {{
            "Resource_List": {{
                "walltime": "01:12:34"
            }},
            "resources_used": {{
                "walltime": "00:13:45"
            }}
        }}
    }}
}}
print(json.dumps(data))
"""


def test_manager_autodetect(hq_env: HqEnv):
    hq_env.start_server()

    with hq_env.mock.mock_program("qstat", qstat_return_walltime("x1234")):
        with hq_env.mock.mock_program("scontrol", scontrol_return("y5678")):
            hq_env.start_worker(cpus=1)
            hq_env.start_worker(
                cpus=1, env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"}
            )
            hq_env.start_worker(cpus=1, env={"SLURM_JOB_ID": "y5678"})

            table = hq_env.command(["worker", "list"], as_table=True)
            table.check_value_columns(["Manager", "Manager Job Id"], 0, ["None", "N/A"])
            table.check_value_columns(
                ["Manager", "Manager Job Id"], 1, ["PBS", "x1234"]
            )
            table.check_value_columns(
                ["Manager", "Manager Job Id"], 2, ["SLURM", "y5678"]
            )

            table = hq_env.command(["worker", "info", "2"], as_table=True)
            table.check_value_row("Manager", "PBS")
            table.check_value_row("Time Limit", "58m 49s")

            table = hq_env.command(["worker", "info", "3"], as_table=True)
            table.check_value_row("Manager", "SLURM")
            table.check_value_row("Time Limit", "15m")


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


def test_manager_pbs_no_env(hq_env: HqEnv):
    hq_env.start_server()
    p = hq_env.start_worker(cpus=1, args=["--manager", "pbs"])
    time.sleep(1)
    hq_env.check_process_exited(p, 1)


def test_manager_pbs(hq_env: HqEnv):
    hq_env.start_server()

    with hq_env.mock.mock_program("qstat", qstat_return_walltime("x1234")):
        hq_env.start_worker(
            cpus=1,
            args=["--manager", "pbs"],
            env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"},
        )

        table = hq_env.command(["worker", "list"], as_table=True)
        table.check_value_columns(["Manager", "Manager Job Id"], 0, ["PBS", "x1234"])


def test_manager_pbs_qstat_path_from_env(hq_env: HqEnv):
    hq_env.start_server()

    with hq_env.mock.mock_program("foo", qstat_return_walltime("x1234")):
        hq_env.start_worker(
            cpus=1,
            args=["--manager", "pbs"],
            env={
                "PBS_ENVIRONMENT": "PBS_BATCH",
                "PBS_JOBID": "x1234",
                "HQ_QSTAT_PATH": "foo",
            },
        )

        table = hq_env.command(["worker", "list"], as_table=True)
        table.check_value_columns(["Manager", "Manager Job Id"], 0, ["PBS", "x1234"])


def test_manager_pbs_no_qstat(hq_env: HqEnv):
    hq_env.start_server()

    process = hq_env.start_worker(
        cpus=1,
        args=["--manager", "pbs"],
        env={"PBS_ENVIRONMENT": "PBS_BATCH", "PBS_JOBID": "x1234"},
    )
    process.wait()
    hq_env.check_process_exited(process, expected_code=1)


def test_manager_slurm_no_env(hq_env: HqEnv):
    hq_env.start_server()
    p = hq_env.start_worker(cpus=1, args=["--manager", "slurm"])
    time.sleep(1)
    hq_env.check_process_exited(p, 1)


def test_manager_slurm(hq_env: HqEnv):
    hq_env.start_server()

    with hq_env.mock.mock_program("scontrol", scontrol_return("abcd")):
        hq_env.start_worker(
            cpus=1, args=["--manager", "slurm"], env={"SLURM_JOB_ID": "abcd"}
        )

    table = hq_env.command(["worker", "list"], as_table=True)
    table.check_value_columns(["Manager", "Manager Job Id"], 0, ["SLURM", "abcd"])

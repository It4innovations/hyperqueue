use std::process::Command;
use std::time::Duration;

use crate::common::manager::common::format_duration;
use crate::Map;

/// Format a duration as a SLURM time string, e.g. 01:05:02
pub fn format_slurm_duration(duration: &Duration) -> String {
    format_duration(duration)
}

pub fn parse_slurm_datetime(datetime: &str) -> anyhow::Result<chrono::NaiveDateTime> {
    Ok(chrono::NaiveDateTime::parse_from_str(
        datetime,
        "%Y-%m-%dT%H:%M:%S",
    )?)
}

pub fn parse_remaining_timelimit(output: &str) -> anyhow::Result<Duration> {
    let items = get_scontrol_items(output);
    let start_time = parse_slurm_datetime(
        items
            .get("StartTime")
            .ok_or_else(|| anyhow::anyhow!("StartTime entry not found"))?,
    )?;

    let end_time = parse_slurm_datetime(
        items
            .get("EndTime")
            .ok_or_else(|| anyhow::anyhow!("EndTime entry not found"))?,
    )?;

    let duration = end_time - start_time;
    if duration.num_seconds() < 0 {
        anyhow::bail!("Slurm: EndTime is smaller than StartTime");
    }

    Ok(duration.to_std().unwrap())
}

/// Calculates how much time is left for the given job using `scontrol`.
/// This function is blocking
pub fn get_remaining_timelimit(job_id: &str) -> anyhow::Result<Duration> {
    let result = Command::new("scontrol")
        .arg("show")
        .arg("job")
        .arg(job_id)
        .output()?;
    if !result.status.success() {
        anyhow::bail!(
            "scontrol command exited with {}: {}\n{}",
            result.status,
            String::from_utf8_lossy(&result.stderr),
            String::from_utf8_lossy(&result.stdout)
        );
    }

    let output = String::from_utf8_lossy(&result.stdout).into_owned();
    log::debug!("scontrol output: {}", output.trim());
    parse_remaining_timelimit(&output)
}

/// Parse <key>=<value> pairs from the output of `scontrol show job <job-id>`.
pub fn get_scontrol_items(output: &str) -> Map<&str, &str> {
    let mut map = Map::new();
    for line in output.lines() {
        for item in line.trim().split(' ') {
            let iter: Vec<_> = item.split('=').take(2).collect();
            if iter.len() < 2 {
                continue;
            }
            let (key, value) = (iter[0], iter[1]);
            map.insert(key, value);
        }
    }
    map
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::common::manager::slurm::{parse_remaining_timelimit, parse_slurm_datetime};

    #[test]
    fn test_parse_slurm_datetime() {
        let date = parse_slurm_datetime("2021-09-29T09:36:56").unwrap();
        assert_eq!(
            date.format("%d.%m.%Y %H:%M:%S").to_string(),
            "29.09.2021 09:36:56"
        );
    }

    #[test]
    fn test_parse_slurm_remaining_time() {
        let output = "JobId=4641914 JobName=bash
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
   Power=";
        assert_eq!(
            parse_remaining_timelimit(output).unwrap(),
            Duration::from_secs(900)
        )
    }
}

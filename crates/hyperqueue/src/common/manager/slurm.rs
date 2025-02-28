use crate::Map;
use crate::common::manager::common::format_duration;
use crate::common::utils::time::parse_hms_time;
use std::process::Command;
use std::time::Duration;

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

fn parse_slurm_duration(value: &str) -> anyhow::Result<Duration> {
    if let Some(p) = value.find('-') {
        let days = value[..p].parse()?;
        let datetime = parse_hms_time(&value[p + 1..])?;
        Ok(datetime + chrono::Duration::days(days).to_std()?)
    } else {
        parse_hms_time(value)
    }
}

pub fn parse_remaining_timelimit(output: &str) -> anyhow::Result<Duration> {
    let items = get_scontrol_items(output);
    let run_time = parse_slurm_duration(
        items
            .get("RunTime")
            .ok_or_else(|| anyhow::anyhow!("RunTime entry not found"))?,
    )?;

    let time_limit = parse_slurm_duration(
        items
            .get("TimeLimit")
            .ok_or_else(|| anyhow::anyhow!("TimeLimit entry not found"))?,
    )?;

    if time_limit < run_time {
        anyhow::bail!("Slurm: TimeLimit is smaller than RunTime");
    }

    Ok(time_limit - run_time)
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
    use crate::common::manager::slurm::{
        parse_remaining_timelimit, parse_slurm_datetime, parse_slurm_duration,
    };
    use std::time::Duration;

    #[test]
    fn test_parse_slurm_datetime() {
        let date = parse_slurm_datetime("2021-09-29T09:36:56").unwrap();
        assert_eq!(
            date.format("%d.%m.%Y %H:%M:%S").to_string(),
            "29.09.2021 09:36:56"
        );
    }

    #[test]
    fn test_parse_slurm_duration() {
        let date = parse_slurm_duration("10:20:30").unwrap();
        assert_eq!(
            chrono::Duration::from_std(date).unwrap(),
            chrono::Duration::hours(10)
                + chrono::Duration::minutes(20)
                + chrono::Duration::seconds(30)
        );
        let date = parse_slurm_duration("17-01:00:11").unwrap();
        assert_eq!(
            chrono::Duration::from_std(date).unwrap(),
            chrono::Duration::days(17) + chrono::Duration::hours(1) + chrono::Duration::seconds(11)
        )
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
            Duration::from_secs(15 * 60 - 60 - 34)
        )
    }
}

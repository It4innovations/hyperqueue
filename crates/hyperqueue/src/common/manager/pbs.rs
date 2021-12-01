use std::process::Command;
use std::str;
use std::time::Duration;

use serde_json::Value;

use crate::common::manager::common::format_duration;
use crate::common::timeutils::parse_hms_time;

fn parse_pbs_job_remaining_time(job_id: &str, data: &str) -> anyhow::Result<Duration> {
    let data_json: Value = serde_json::from_str(data)?;

    let walltime = parse_hms_time(
        data_json["Jobs"][job_id]["Resource_List"]["walltime"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Could not find walltime key for job {}", job_id))?,
    )?;
    let used = parse_hms_time(
        data_json["Jobs"][job_id]["resources_used"]["walltime"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Could not find used time key for job {}", job_id))?,
    )?;

    if walltime < used {
        anyhow::bail!("Pbs: Used time is bigger then walltime");
    }

    Ok(walltime - used)
}

/// Calculates how much time is left for the given job using `qstat`.
pub fn get_remaining_timelimit(job_id: &str) -> anyhow::Result<Duration> {
    let result = Command::new("qstat")
        .args(&["-f", "-F", "json", job_id])
        .output()?;
    if !result.status.success() {
        anyhow::bail!(
            "qstat command exited with {}: {}\n{}",
            result.status,
            String::from_utf8_lossy(&result.stderr),
            String::from_utf8_lossy(&result.stdout)
        );
    }

    let output = String::from_utf8_lossy(&result.stdout).into_owned();
    log::debug!("qstat output: {}", output.trim());

    parse_pbs_job_remaining_time(job_id, output.as_str())
}

/// Format a duration as a PBS time string, e.g. 01:05:02
pub fn format_pbs_duration(duration: &Duration) -> String {
    format_duration(duration)
}

pub fn parse_pbs_datetime(datetime: &str) -> anyhow::Result<chrono::NaiveDateTime> {
    Ok(chrono::NaiveDateTime::parse_from_str(
        datetime,
        "%a %b %d %H:%M:%S %Y",
    )?)
}

#[cfg(test)]
mod test {
    use crate::common::manager::pbs::parse_pbs_datetime;

    #[test]
    fn test_parse_pbs_datetime() {
        let date = parse_pbs_datetime("Thu Aug 19 13:05:17 2021").unwrap();
        assert_eq!(
            date.format("%d.%m.%Y %H:%M:%S").to_string(),
            "19.08.2021 13:05:17"
        );
    }
}

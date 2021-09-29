use std::path::PathBuf;
use std::process::Command;
use std::str;
use std::time::Duration;

use anyhow::Context;
use chrono::Duration as ChronoDuration;
use serde_json::Value;

use crate::common::env::HQ_QSTAT_PATH;
use crate::common::manager::common::format_duration;

pub struct PbsContext {
    pub qstat_path: PathBuf,
}

impl PbsContext {
    pub fn create() -> anyhow::Result<Self> {
        let qstat_path = match Command::new("qstat").spawn() {
            Ok(mut process) => {
                process.wait().ok();
                PathBuf::from("qstat")
            }
            Err(e) => {
                log::warn!(
                    "Couldn't get qstat path directly ({}), trying to get it from environment variable {}",
                    e,
                    HQ_QSTAT_PATH
                );
                let path = std::env::var(HQ_QSTAT_PATH)
                    .context("Cannot get qstat path from environment variable")?;
                path.into()
            }
        };

        Ok(Self { qstat_path })
    }
}

fn parse_duration(raw_time: &str) -> anyhow::Result<Duration> {
    let numbers: Vec<&str> = raw_time.split(':').collect();
    let duration = ChronoDuration::hours(numbers[0].parse()?)
        + ChronoDuration::minutes(numbers[1].parse()?)
        + ChronoDuration::seconds(numbers[2].parse()?);
    Ok(duration.to_std()?)
}

fn get_time(job_id: &str, data: &str) -> anyhow::Result<Duration> {
    let data_json: Value = serde_json::from_str(data)?;

    let walltime = parse_duration(
        data_json["Jobs"][job_id]["Resource_List"]["walltime"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Could not find walltime key for job {}", job_id))?,
    )?;
    let used = parse_duration(
        data_json["Jobs"][job_id]["resources_used"]["walltime"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Could not find used time key for job {}", job_id))?,
    )?;

    Ok(walltime - used)
}

/// Calculates how much time is left for the given job using `qstat`.
/// TODO: make this async
pub fn get_remaining_timelimit(ctx: &PbsContext, job_id: &str) -> anyhow::Result<Duration> {
    let result = Command::new(&ctx.qstat_path)
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

    get_time(job_id, output.as_str())
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
